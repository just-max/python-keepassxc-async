import asyncio
import collections
import functools
from collections import defaultdict
from typing import TypeVar, AsyncIterator, AsyncContextManager, Generic, Callable, DefaultDict, Optional

E = TypeVar("E")
T = TypeVar("T")


class EventListener(AsyncIterator[T], AsyncContextManager[AsyncIterator[tuple[E, T]]], Generic[E, T]):
    def __init__(self, remove: Callable[["EventListener"], None]):
        self._q: asyncio.Queue[Optional[tuple[E, T]]] = asyncio.Queue()

        self._remove = remove
        self._closed = False

    async def __anext__(self) -> tuple[T, E]:
        result = await self._q.get()
        if result is None:
            self._q.put_nowait(None)  # cancel the next waiting 'get', and so on...
            raise StopAsyncIteration
        return result

    async def __aexit__(self, exc_type, exc_value, tb) -> None:
        self._remove(self)
        self.close()

    def receive(self, event_type: E, data: T):
        if self._closed:
            raise RuntimeError("closed")

        self._q.put_nowait((event_type, data))

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._q.put_nowait(None)


class EventEmitter(Generic[E, T]):
    def __init__(self):
        self._listeners: DefaultDict[E, set[EventListener[E, T]]] = defaultdict(set)
        self._closed = False

    def _remove(self, events: tuple[E, ...], listener: EventListener[E, T]):
        for event in events:
            self._listeners[event].remove(listener)

    def listen(self, *events: E) -> AsyncContextManager[AsyncIterator[tuple[E, T]]]:
        if self._closed:
            raise RuntimeError("closed")

        listener: EventListener[E, T] = EventListener(functools.partial(self._remove, events))
        for event in events:
            self._listeners[event].add(listener)
        return listener

    def emit(self, event: E, data: T):
        if self._closed:
            raise RuntimeError("closed")

        for listener in self._listeners[event]:
            listener.receive(event, data)

    def close(self):
        if self._closed:
            return
        self._closed = True
        for listeners in self._listeners.values():
            for listener in listeners:
                # listeners may be in multiple sets, but stop is safe to call multiple times
                listener.close()
