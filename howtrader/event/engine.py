"""
Event-driven framework of Howtrader framework.
"""

from collections import defaultdict
from queue import Empty, Queue
from threading import Thread,current_thread
from time import sleep
from typing import Any, Callable, List

EVENT_TIMER = "eTimer"


class Event:
    """
    Event object consists of a type string which is used
    by event engine for distributing event, and a data
    object which contains the real data.
    """

    def __init__(self, type: str, data: Any = None) -> None:
        """"""
        self.type: str = type
        self.data: Any = data


# Defines handler function to be used in event engine.
HandlerType: callable = Callable[[Event], None]


class EventEngine:
    """
    Event engine distributes event object based on its type
    to those handlers registered.

    It also generates timer event by every interval seconds,
    which can be used for timing purpose.
    """

    def __init__(self, interval: int = 1, n: int = 5) -> None:
        """
        Timer event is generated every 1 second by default, if
        interval not specified.
        """
        self._interval: int = interval
        self._n: int = n

        self._queues: List[Queue] = []
        self._threads: List[Thread] = []
        for i in range(self._n):
            self._queues.append(Queue())
            self._threads.append(Thread(target=self._run,args=(i,),name=f"event-worker-{i}"))
        self._active: bool = False
        
        self._timer: Thread = Thread(target=self._run_timer)
        self._handlers: defaultdict = defaultdict(list)
        self._general_handlers: List = []

    def _run(self,index: int) -> None:
        """
        Get event from queue and then process it.
        """
        index = index % self._n
        queue = self._queues[index]
        while self._active:
            try:
                event: Event = queue.get(block=True, timeout=1)
                self._process(event)
                print(f"thread={current_thread().name},index={index},event={event.type}")
            except Empty:
                pass

    def _process(self, event: Event) -> None:
        """
        First distribute event to those handlers registered listening
        to this type.

        Then distribute event to those general handlers which listens
        to all types.
        """
        if event.type in self._handlers:
            [handler(event) for handler in self._handlers[event.type]]

        if self._general_handlers:
            [handler(event) for handler in self._general_handlers]

    def _run_timer(self) -> None:
        """
        Sleep by interval second(s) and then generate a timer event.
        """
        while self._active:
            sleep(self._interval)
            event: Event = Event(EVENT_TIMER)
            self.put(event)

    def start(self) -> None:
        """
        Start event engine to process events and generate timer events.
        """
        self._active = True
        for i in range(self._n):
            self._threads[i].start()
        self._timer.start()

    def stop(self) -> None:
        """
        Stop event engine.
        """
        self._active = False
        self._timer.join()
        for i in range(self._n):
            self._threads[i].join()

    def put(self, event: Event) -> None:
        """
        Put an event object into event queue.
        """
        index = hash(event.type) % self._n
        queue = self._queues[index]
        queue.put(event)

    def register(self, type: str, handler: HandlerType) -> None:
        """
        Register a new handler function for a specific event type. Every
        function can only be registered once for each event type.
        """
        handler_list: list = self._handlers[type]
        if handler not in handler_list:
            handler_list.append(handler)

    def unregister(self, type: str, handler: HandlerType) -> None:
        """
        Unregister an existing handler function from event engine.
        """
        handler_list: list = self._handlers[type]

        if handler in handler_list:
            handler_list.remove(handler)

        if not handler_list:
            self._handlers.pop(type)

    def register_general(self, handler: HandlerType) -> None:
        """
        Register a new handler function for all event types. Every
        function can only be registered once for each event type.
        """
        if handler not in self._general_handlers:
            self._general_handlers.append(handler)

    def unregister_general(self, handler: HandlerType) -> None:
        """
        Unregister an existing general handler function.
        """
        if handler in self._general_handlers:
            self._general_handlers.remove(handler)
