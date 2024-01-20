import asyncio
import json
import logging


from keepassxc.database import Client, DatabaseIdentity, generate_identification_key, Group
from keepassxc.database import EstablishConnectionError, ProtocolError, ErrorCode


try:
    # for nice trees: pip install PrettyPrintTree
    from PrettyPrint import PrettyPrintTree
    groups_pp = PrettyPrintTree(
        lambda g: g.children,
        lambda g: f"{g.name} ({g.uuid[:6]})",
        orientation=PrettyPrintTree.Horizontal,
    )
except ImportError:
    def groups_pp(g):
        print(Group.dump_tree(g))


async def connect_to_keepassxc(min_delay=1, max_delay=60.):
    """Tries to connect to KeePassXC until a connection is established"""

    delay = min_delay
    while True:
        try:
            return await Client.create()
        except EstablishConnectionError:
            print(f"could not connect to KeePassXC, trying again in {delay} seconds...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)


async def example():
    # logging.basicConfig(level=logging.DEBUG)  # enable to see raw messages passed to/from KeePassXC

    async with await connect_to_keepassxc() as client:
        client: Client
        # TODO: why is this not typed automatically???? -> solution: add aenter to Client with explicit return type :(

        print("connected to KeePassXC")

        identity_input = input("enter an identity (JSON) or press enter for a new association: ")
        if not identity_input:
            # option 1: new association

            # generate a key to identify this client to the database
            identification_key = generate_identification_key()
            # ask KeePassXC to associate our key with the database
            assoc, _ = await client.retry_when_locked(lambda: client.associate(identification_key))
            identity = DatabaseIdentity(assoc.id, identification_key)
            print("associated new identity with database:", json.dumps(identity.to_json()))

        else:
            # option 2: existing association
            identity = DatabaseIdentity.of_json(json.loads(identity_input))
            assoc, _ = await client.retry_when_locked(lambda: client.test_associate(identity))
            print("associated with existing identity")

        # create a new group
        new_group, _ = await client.retry_when_locked(lambda: client.create_new_group("foo/bar/baz"))
        print(f"created (or found existing) group '{new_group.name}' with UUID {new_group.uuid}")

        # get the tree of groups
        groups, _ = await client.retry_when_locked(lambda: client.get_database_groups())
        print("groups in database:")
        groups_pp(groups.groups[0])

        # find logins for a URL
        url = "https://example.com"
        try:
            logins, _ = await client.retry_when_locked(lambda: client.get_logins(url, (identity,)))
        except ProtocolError as e:
            if e.code == ErrorCode.KEEPASS_NO_LOGINS_FOUND:
                print(f"no logins found for {url}")
        else:
            print(
                f"found {logins.count} login(s) for {url}:",
                *(f"- {e.name} ({e.login}) {'*' * len(e.password)}" for e in logins.entries),
                sep="\n"
            )

        # for more operations, see the Client class


async def main():
    try:
        await example()
    except ProtocolError as e:
        print(e)


if __name__ == "__main__":
    asyncio.run(main())
