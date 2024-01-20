import asyncio
import base64
import enum
import json
import logging
import os
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterable, Optional, Callable, AsyncContextManager, Awaitable, Any, TypeVar

import nacl.public
import nacl.utils

from ._async_util import EventEmitter

_logger = logging.getLogger(__name__)


_KPXC_SOCKET_NAME = "org.keepassxc.KeePassXC.BrowserServer"


def _kpxc_socket_dirs() -> Iterable[Path]:
    """As of keepassxreboot/keepassxc#8030, the directory containing the socket is either:

    - XDG_RUNTIME_DIR/app/org.keepassxc.KeePassXC/ (default)
    - XDG_RUNTIME_DIR/ (legacy)
    - /tmp/ or the appropriate system temp directory (fallback)

    Return an iterable of the potential directories, which should be searched in-order."""

    xdg_runtime_dir_v = os.getenv("XDG_RUNTIME_DIR")
    if xdg_runtime_dir_v is not None:
        xdg_runtime_dir = Path(xdg_runtime_dir_v)
        yield xdg_runtime_dir / "app" / "org.keepassxc.KeePassXC"
        yield xdg_runtime_dir

    yield Path(tempfile.gettempdir())


def _b64encode_s(data: bytes):
    return base64.b64encode(data).decode()


def _kpxc_stringify_bool(value: bool):
    """ "true" or "false" """
    return "true" if value else "false"


def _magic(value: Any) -> Any:
    return value


class KeePassXCError(Exception):
    pass


# === socket connection ===


class Connection(AsyncContextManager):
    _private_init = object()

    def __init__(self, /, _private_init):
        if _private_init is not Connection._private_init:
            raise TypeError("__init__ is private")

        self._reader: asyncio.StreamReader = _magic(None)
        self._writer: asyncio.StreamWriter = _magic(None)

    @classmethod
    async def create(cls, socket_dirs: Optional[Iterable[Path]] = None, socket_name: str = _KPXC_SOCKET_NAME):
        self = cls(_private_init=Connection._private_init)

        exceptions = []
        non_empty = False

        for socket_dir in (socket_dirs if socket_dirs is not None else _kpxc_socket_dirs()):
            non_empty = True
            try:
                self._reader, self._writer = await asyncio.open_unix_connection(socket_dir / socket_name)
                break
            except OSError as e:
                exceptions.append(e)
                continue

        if not non_empty:
            raise ValueError("socket_dirs must be a non-empty sequence")
        elif exceptions:
            raise ExceptionGroup("could not connect", exceptions)

        return self

    def send(self, message: bytes):
        _logger.debug("sent: %s", message)
        self._writer.write(message)

    async def drain(self):
        await self._writer.drain()

    async def read(self):
        message = await self._reader.read(1024 * 1024)  # TODO: size?
        _logger.debug("read: %s", message)
        return message

    def close(self):
        self._writer.close()

    async def wait_closed(self):
        with suppress(asyncio.CancelledError):
            await self._writer.wait_closed()

    async def __aexit__(self, exc_type, exc_value, tb) -> None:
        self.close()
        await self.wait_closed()


# === messaging requests and signals (unencrypted) ===

class MessageAction(Enum):
    ASSOCIATE = "associate"
    CHANGE_PUBLIC_KEYS = "change-public-keys"
    CREATE_NEW_GROUP = "create-new-group"
    DATABASE_LOCKED = "database-locked"
    DATABASE_UNLOCKED = "database-unlocked"
    GENERATE_PASSWORD = "generate-password"
    GET_DATABASE_GROUPS = "get-database-groups"
    GET_DATABASE_HASH = "get-databasehash"
    GET_LOGINS = "get-logins"
    GET_TOTP = "get-totp"
    LOCK_DATABASE = "lock-database"
    PASSKEYS_GET = "passkeys-get"
    PASSKEYS_REGISTER = "passkeys-register"
    REQUEST_AUTOTYPE = "request-autotype"
    SET_LOGIN = "set-login"
    TEST_ASSOCIATE = "test-associate"

    def is_signal(self):
        return self in {MessageAction.DATABASE_LOCKED, MessageAction.DATABASE_UNLOCKED}

    def make_message(self, /, **data):
        return {"action": self.value} | data

    @staticmethod
    def of_message(message):
        message_ = dict(message)
        action = MessageAction(message_.pop("action"))
        return action, message_


class KeePassXCExited(KeePassXCError):
    pass


class BaseClient(AsyncContextManager):
    _private_init = object()

    def __init__(self, /, _private_init):
        if _private_init is not BaseClient._private_init:
            raise TypeError("__init__ is private")

        self._replies: dict[MessageAction, asyncio.Queue[asyncio.Future[Any]]] = {
            action: asyncio.Queue(1)
            for action in MessageAction if not action.is_signal()
        }
        self._pending: set[asyncio.Future] = set()

        self._event_emitter: EventEmitter[MessageAction, Any] = EventEmitter()
        self._connection: Connection = _magic(None)
        self._worker_task: asyncio.Task = _magic(None)

        self._eof = False

    @classmethod
    async def create(cls, connection: Callable[[], Awaitable[Connection]] = Connection.create):
        self = cls(_private_init=BaseClient._private_init)

        self._connection = await connection()
        self._worker_task = asyncio.create_task(self._worker())

        return self

    async def _worker(self):
        while True:
            # TODO: error handling

            message = await self._connection.read()
            if not message:
                self._do_eof()
                return

            action, data = MessageAction.of_message(json.loads(message))
            if action.is_signal():
                self._event_emitter.emit(action, data)
            else:
                q = self._replies[action]
                if q.qsize() == 0:
                    _logger.warning("unsolicited message ignored: %s %s", action, data)
                    continue
                q.get_nowait().set_result(data)

    def _do_eof(self):
        self._eof = True
        for p in self._pending:
            p.set_exception(KeePassXCExited("eof"))
        self._event_emitter.close()

    async def _do_request(self, action: MessageAction, msg: bytes):
        if self._eof:
            raise KeePassXCExited()

        fut = asyncio.get_running_loop().create_future()
        self._pending.add(fut)

        try:
            await self._replies[action].put(fut)  # maxsize=1 on queue ensures blocking until pending request completes
            if not self._eof:
                self._connection.send(msg)
                await self._connection.drain()  # TODO: is drain necessary?
            return await fut
        finally:
            self._pending.remove(fut)

    async def request(self, action: MessageAction, /, **data):
        if action.is_signal():
            raise ValueError("action is a signal")
        msg = json.dumps(action.make_message(**data)).encode()
        return await self._do_request(action, msg)

    def listen(self, *actions: MessageAction):
        for action in actions:
            if not action.is_signal():
                raise ValueError(f"action {action.value} is not a signal")
        return self._event_emitter.listen(*actions)

    def close(self):
        self._worker_task.cancel()
        self._event_emitter.close()
        self._connection.close()

    async def wait_closed(self):
        with suppress(asyncio.CancelledError):
            await self._worker_task
        await self._connection.wait_closed()

    async def __aexit__(self, exc_type, exc_value, tb) -> None:
        self.close()
        await self.wait_closed()


# === exceptions: protocol-level ===

class ErrorCode(enum.IntEnum):
    KEEPASS_DATABASE_NOT_OPENED = 1
    KEEPASS_DATABASE_HASH_NOT_RECEIVED = 2
    KEEPASS_CLIENT_PUBLIC_KEY_NOT_RECEIVED = 3
    KEEPASS_CANNOT_DECRYPT_MESSAGE = 4
    KEEPASS_TIMEOUT_OR_NOT_CONNECTED = 5
    KEEPASS_ACTION_CANCELLED_OR_DENIED = 6
    KEEPASS_CANNOT_ENCRYPT_MESSAGE = 7
    KEEPASS_ASSOCIATION_FAILED = 8
    KEEPASS_KEY_CHANGE_FAILED = 9
    KEEPASS_ENCRYPTION_KEY_UNRECOGNIZED = 10
    KEEPASS_NO_SAVED_DATABASES_FOUND = 11
    KEEPASS_INCORRECT_ACTION = 12
    KEEPASS_EMPTY_MESSAGE_RECEIVED = 13
    KEEPASS_NO_URL_PROVIDED = 14
    KEEPASS_NO_LOGINS_FOUND = 15
    KEEPASS_NO_GROUPS_FOUND = 16
    KEEPASS_CANNOT_CREATE_NEW_GROUP = 17
    KEEPASS_NO_VALID_UUID_PROVIDED = 18
    KEEPASS_ACCESS_TO_ALL_ENTRIES_DENIED = 19
    PASSKEYS_ATTESTATION_NOT_SUPPORTED = 20
    PASSKEYS_CREDENTIAL_IS_EXCLUDED = 21
    PASSKEYS_REQUEST_CANCELED = 22
    PASSKEYS_INVALID_USER_VERIFICATION = 23
    PASSKEYS_EMPTY_PUBLIC_KEY = 24
    PASSKEYS_INVALID_URL_PROVIDED = 25
    PASSKEYS_RESIDENT_KEYS_NOT_SUPPORTED = 26


@dataclass
class ProtocolError(ConnectionError, KeePassXCError):
    message: str
    code: ErrorCode

    def __str__(self):
        return f"[KeePassXC: Error {self.code}] {self.message}"


class BadNonceError(ConnectionError, KeePassXCError):
    pass


class NotSuccessfulError(KeePassXCError):
    pass


# === messaging: encrypted messages ===

@dataclass(frozen=True)
class Nonce:
    value: bytes

    SIZE = 24  # in bytes
    _MASK = 2 ** (SIZE * 8) - 1

    @staticmethod
    def generate():
        return Nonce(nacl.utils.random(Nonce.SIZE))

    def __int__(self):
        return int.from_bytes(self.value, byteorder="little")

    def check_increment(self, other: "Nonce"):
        return int(other) == ((int(self) + 1) & Nonce._MASK)


@dataclass(frozen=True)
class SessionClientID:
    value: bytes

    SIZE = 24  # in bytes

    @staticmethod
    def generate():
        return SessionClientID(nacl.utils.random(SessionClientID.SIZE))


@dataclass(frozen=True)
class DatabaseIdentity:
    identifier: str
    key: nacl.public.PublicKey

    def to_json(self):
        return {
            "id": self.identifier,
            "key": _b64encode_s(self.key.encode())
        }

    @staticmethod
    def of_json(data):
        return DatabaseIdentity(data["id"], nacl.public.PublicKey(base64.b64decode(data["key"])))


def generate_identification_key():
    return nacl.public.PrivateKey.generate().public_key


@dataclass(frozen=True)
class DatabaseMeta:
    hash: Optional[str]
    version: str  # TODO: should this be optional?

    @staticmethod
    def of_data(data):
        return DatabaseMeta(data.get("hash", None), data["version"])


# === response data-classes ===

@dataclass(frozen=True)
class AssociateResponse:
    id: str

    @staticmethod
    def of_data(data):
        return AssociateResponse(data["id"])


@dataclass(frozen=True)
class CreateNewGroupResponse:
    name: str
    uuid: str

    @staticmethod
    def of_data(data):
        return CreateNewGroupResponse(data["name"], data["uuid"])


@dataclass(frozen=True)
class Group:
    name: str
    uuid: str
    children: tuple["Group", ...]

    @staticmethod
    def of_data(data):
        return Group(data["name"], data["uuid"], tuple(Group.of_data(d) for d in data["children"]))

    def _dump_tree(self, depth: int):
        yield f"|{'+' * (2 * depth)} {self.name} ({self.uuid})"
        for child in self.children:
            yield from child._dump_tree(depth + 1)

    def dump_tree(self):
        return "\n".join(self._dump_tree(0))


@dataclass(frozen=True)
class GetDatabaseGroupsResponse:
    groups: tuple[Group, ...]

    @staticmethod
    def of_data(data):
        return GetDatabaseGroupsResponse(tuple(Group.of_data(d) for d in data["groups"]["groups"]))


@dataclass(frozen=True)
class GetDatabaseHashResponse:
    hash: str

    @staticmethod
    def of_data(data):
        return GetDatabaseHashResponse(data["hash"])


@dataclass(frozen=True)
class Entry:
    group: str
    login: str
    name: str
    password: str
    stringFields: tuple[str, ...]
    uuid: str

    @staticmethod
    def of_data(data):
        return Entry(
            data["group"], data["login"], data["name"],
            data["password"], tuple(data["stringFields"]), data["uuid"],
        )


@dataclass(frozen=True)
class GetLoginsResponse:
    count: int
    entries: tuple[Entry, ...]

    @staticmethod
    def of_data(data):
        return GetLoginsResponse(data["count"], tuple(Entry.of_data(d) for d in data["entries"]))


@dataclass(frozen=True)
class GetTOTPResponse:
    totp: str

    @staticmethod
    def of_data(data):
        return GetTOTPResponse(data["totp"])


@dataclass(frozen=True)
class LockDatabaseResponse:
    pass


@dataclass(frozen=True)
class RequestAutotypeResponse:
    pass


T = TypeVar("T")


class Client(AsyncContextManager):
    _private_init = object()

    def __init__(self, /, _private_init):
        if _private_init is not Client._private_init:
            raise TypeError("__init__ is private")

        self._session_client_key = nacl.public.PrivateKey.generate()
        self._session_client_id = SessionClientID.generate()

        self._base_client: BaseClient = _magic(None)
        self._session_server_public_key: nacl.public.PublicKey = _magic(None)
        self._box: nacl.public.Box = _magic(None)

        self._unlocked = asyncio.Event()
        self._locked = asyncio.Event()
        self._lock_listener_task: asyncio.Task = _magic(None)

    @classmethod
    async def create(cls, base_client: Callable[[], Awaitable[BaseClient]] = BaseClient.create):
        self = cls(_private_init=Client._private_init)

        self._base_client = await base_client()
        self._session_server_public_key = await self._exchange_public_keys()

        self._box = nacl.public.Box(self._session_client_key, self._session_server_public_key)

        # note that locked and unlocked signals don't necessarily alternate,
        # e.g. when switching between two open databases in KPXC, only 'unlocked' events are fired;
        # thus, we have a separate asyncio.Event for each
        if await self.is_unlocked():
            self._unlocked.set()
        else:
            self._locked.set()
        self._lock_listener_task = asyncio.create_task(self._lock_listener())

        return self

    async def _lock_listener(self):
        async with self.listen(MessageAction.DATABASE_LOCKED, MessageAction.DATABASE_UNLOCKED) as listener:
            async for event, _ in listener:
                match event:
                    case MessageAction.DATABASE_LOCKED:
                        self._unlocked.clear()
                        self._locked.set()
                    case MessageAction.DATABASE_UNLOCKED:
                        self._locked.clear()
                        self._unlocked.set()

    def listen(self, *actions):
        return self._base_client.listen(*actions)

    async def is_unlocked(self):
        try:
            await self.get_database_hash()
            return True
        except ProtocolError as e:
            if e.code == ErrorCode.KEEPASS_DATABASE_NOT_OPENED:
                return False
            raise  # if something else went wrong, raise

    async def wait_unlocked(self):
        await self._unlocked.wait()

    async def wait_locked(self):
        await self._locked.wait()

    async def retry_when_locked(self, action: Callable[[], Awaitable[T]]) -> T:
        """Wait until the database is unlocked, then try to perform the given action. If the action raises a
        ProtocolError with the error code KEEPASS_DATABASE_NOT_OPENED, the action is tried again.

        This is necessary, because if we only wait for the database to be
        unlocked, it could get locked again at any time."""

        while True:
            await self.wait_unlocked()
            try:
                return await action()
            except ProtocolError as e:
                if e.code == ErrorCode.KEEPASS_DATABASE_NOT_OPENED:
                    continue
                raise

    @staticmethod
    def _make_message_data(session_client_id: SessionClientID, /, *, nonce: Optional[Nonce] = None, **data):
        """Adds nonce and clientID to the message data"""
        nonce_: Nonce = nonce or Nonce.generate()
        msg = dict(
            nonce=_b64encode_s(nonce_.value),
            clientID=_b64encode_s(session_client_id.value),
            **data
        )
        return msg, nonce_

    @staticmethod
    def _check_error(resp):
        err = resp.get("error", None)
        if err is not None:
            raise ProtocolError(err, ErrorCode(int(resp["errorCode"])))

    @staticmethod
    def _check_success(resp):
        if not resp.get("success", None) == "true":
            raise NotSuccessfulError(f"success != 'true'")

    @staticmethod
    def _check_nonce(nonce1: Nonce, resp):
        if not nonce1.check_increment(Nonce(base64.b64decode(resp["nonce"]))):
            raise BadNonceError("bad nonce")

    async def _exchange_public_keys(self):
        data, nonce = Client._make_message_data(
            self._session_client_id,
            publicKey=_b64encode_s(self._session_client_key.public_key.encode())
        )

        resp = await self._base_client.request(MessageAction.CHANGE_PUBLIC_KEYS, **data)
        Client._check_error(resp)
        Client._check_success(resp)
        Client._check_nonce(nonce, resp)

        return nacl.public.PublicKey(base64.b64decode(resp["publicKey"]))

    def _wrap_encrypt(self, action: MessageAction, /, **data):
        message = action.make_message(**data)
        _logger.debug("encrypted: %s", message)
        encrypted_message = self._box.encrypt(json.dumps(message).encode())
        return Client._make_message_data(
            self._session_client_id,
            nonce=Nonce(encrypted_message.nonce),
            message=_b64encode_s(encrypted_message.ciphertext)
        )

    def _unwrap_decrypt(self, obj):
        inner_msg = json.loads(self._box.decrypt(base64.b64decode(obj["message"]), base64.b64decode(obj["nonce"])))
        _logger.debug("decrypted: %s", inner_msg)
        return inner_msg

    async def _request_encrypted(self, action: MessageAction, /, **data):
        """Sends a message by encrypting the inner message and wrapping it with nonce and clientID."""

        encrypted_message, nonce = self._wrap_encrypt(action, **data)

        response = await self._base_client.request(action, **encrypted_message)

        Client._check_error(response)
        Client._check_nonce(nonce, response)

        inner_response = self._unwrap_decrypt(response)

        Client._check_error(inner_response)
        Client._check_nonce(nonce, inner_response)
        Client._check_success(inner_response)

        return inner_response

    async def test_associate(self, identity: DatabaseIdentity):
        data = identity.to_json()  # id, key
        response_data = await self._request_encrypted(MessageAction.TEST_ASSOCIATE, **data)
        return AssociateResponse.of_data(response_data), DatabaseMeta.of_data(response_data)

    async def associate(self, identification_key: nacl.public.PublicKey):
        response_data = await self._request_encrypted(
            MessageAction.ASSOCIATE,
            key=_b64encode_s(self._session_client_key.public_key.encode()),
            idKey=_b64encode_s(identification_key.encode())
        )
        return AssociateResponse.of_data(response_data), DatabaseMeta.of_data(response_data)

    async def create_new_group(self, group_name: str):
        response_data = await self._request_encrypted(
            MessageAction.CREATE_NEW_GROUP,
            groupName=group_name
        )
        return CreateNewGroupResponse.of_data(response_data), DatabaseMeta.of_data(response_data)

    async def get_database_groups(self):
        response_data = await self._request_encrypted(MessageAction.GET_DATABASE_GROUPS)
        return GetDatabaseGroupsResponse.of_data(response_data), DatabaseMeta.of_data(response_data)

    async def get_database_hash(self):
        response_data = await self._request_encrypted(MessageAction.GET_DATABASE_HASH)
        return GetDatabaseHashResponse.of_data(response_data), DatabaseMeta.of_data(response_data)

    async def get_logins(
            self,
            url: str,  # TODO: should this be restricted to valid URLs (?)
            identities: Iterable[DatabaseIdentity],
            submit_url: Optional[str] = None,
            http_auth: Optional[bool] = None,
    ):
        optional_data = {}
        if submit_url is not None:
            optional_data["submitUrl"] = submit_url
        if http_auth is not None:
            optional_data["httpAuth"] = _kpxc_stringify_bool(http_auth)

        response_data = await self._request_encrypted(
            MessageAction.GET_LOGINS,
            url=url,
            keys=tuple(ident.to_json() for ident in identities),
            **optional_data,
        )

        return GetLoginsResponse.of_data(response_data), DatabaseMeta.of_data(response_data)

    async def get_totp(self, totp_uuid: str):
        response_data = await self._request_encrypted(MessageAction.GET_TOTP, uuid=totp_uuid)
        return GetTOTPResponse.of_data(response_data), DatabaseMeta.of_data(response_data)

    async def lock_database(self):
        return (
            None,
            DatabaseMeta.of_data(await self._request_encrypted(MessageAction.LOCK_DATABASE)),
        )

    async def request_autotype(self, search: str):
        return (
            None,
            DatabaseMeta.of_data(await self._request_encrypted(MessageAction.REQUEST_AUTOTYPE, search=search))
        )

    async def set_logins(self):  # TODO, along with a couple other actions
        raise NotImplementedError("set_logins")

    def close(self):
        """Close the connection to the database."""
        self._lock_listener_task.cancel()
        self._base_client.close()

    async def wait_closed(self):
        """Wait for the connection to the database to fully close."""
        with suppress(asyncio.CancelledError):
            await self._lock_listener_task
        await self._base_client.wait_closed()

    async def __aexit__(self, exc_type, exc_value, tb) -> None:
        self.close()
        await self.wait_closed()
