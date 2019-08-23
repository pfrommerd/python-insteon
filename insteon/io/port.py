import weakref
import asyncio

import time

from . import message
from .. import util as util

import logbook
logger = logbook.Logger(__name__)

"""
To use a request you must first enter an
await with request:
    block
and then you can use the various wait() calls
"""
class Request:
    def __init__(self, msg, retries=5, timeout=0.1, quiet=0.1):
        self.message = msg
        self.tries = 0
        self.remaining = retries
        self.timeout = timeout
        self.quiet_time = quiet

        # on self.written being set the requester knows
        # that the message has been written
        self.written = asyncio.Event()

        # on self.continue being set the writer loop
        # will move the next request
        self.successful = asyncio.Event() 

        # trigger this on a nack response
        # or to manually request another resend 
        # also (manually bump up remaining if necessary)
        self.failure = asyncio.Event()

        # lifetime of the request
        self.done = asyncio.Event() 

        # Notify when a message comes in
        self.received = asyncio.Condition()

        # A list of all the responses this message has received
        self.responses = []


    # ------------- Lifetime Management Functions --------------

    def __del__(self):
        # set this for sure when the object is deleted
        # so we don't have a leak
        self.done.set()

    def __enter__(self):
        pass

    def __exit__(self):
        self.done.set()

    def success(self):
        self.successful.set()

    # on failure, add some extra quiet time
    # note: currently this is only applied after all
    # tries fall through, we should make this a per-resend quiet_time
    def fail(self, extra_quiet=0):
        self.failure.set()

    # ---------------- Response Management Functions ------------

    # when consume() is called a message
    # gets eaten so it won't trigger wait anymore
    def consume(msg):
        self.responses.remove(msg)

    # will eat upto a particular message
    def consume_until(msg):
        for i in range(len(responses)):
            if responses[i] == msg:
                self.responses = self.responses[i + 1:]


    # The underlying wait functions
    # are wrapped above to have timeouts
    async def _wait():
        if len(self.responses) > 0:
            return self.responses[0]

        await self.received.lock()
        try:
            await self.received.wait()
            return self.responses[0]
        finally:
            self.received.release()

    async def _wait_util(predicate):
        for r in self.responses:
            if predicate(r):
                return r

        await self.received.lock()
        try:
            while True:
                if await self.received.wait_for(lambda: predicate(self.responses[-1])):
                    return self.responses[-1]
        finally:
            self.received.release()

    # called by the port when a message is matched to
    # this request
    async def process(msg):
        await self.received.lock()
        try:
            self.responses.append(msg)
            self.received.notify_all()
        finally:
            self.received.release()

class Port:
    def __init__(self, definitions={}):
        self.defs = definitions

        self._queue = asyncio.PriorityQueue()

        # Requests that aren't done yet
        # there can be multiple running concurrently at any given time
        self._open_requests = []

        self._write_handlers = []
        self._read_handlers = []

        # if using the start, stop api this will be set
        self._task = None

    def start(self, conn, loop=None):
        if not loop:
            loop = asyncio.get_event_loop()
        self._open_requests.clear()
        self._queue = asyncio.PriorityQueue() # clear the queue

        self._task = self._run(conn)
        return self._task


    def stop(self):
        if self._task:
            self._task.cancel()
        self._task = None

    """ Write returns a request object through which the 
        caller can get access to a queue containing all future messages that have been sent """
    def write(self, msg, priority=1, retries=5, timeout=0.1, quiet=0.1):
        req = Request(msg, retries, timeout, quiet)
        self._queue.put_nowait((priority, req))
        return req

    def _run(self, conn):
        return asyncio.gather(self._run_writer(conn), self._run_reader(conn))

    async def _run_writer(self, conn):
        try:
            while True:
                req = await self._queue.get()
                
                # Put a weak reference to the request in the open requests list
                self._open_requests.append(weakref.ref(req))

                # Do the writing
                for try_num in range(req.remaining):
                    # bump tries and clear failure flag
                    req.tries = try_num + 1
                    req.failure.clear()

                    # do the writing... (synchronously?)
                    await self._conn.write(req.message)

                    # set that the request has been written
                    req.written.set()

                    # notify the handlers that it has been written
                    handlers = list(self._write_handlers)
                    for h in handlers:
                        h(req.message)

                    try:
                        # wait for either the continue event to trigger
                        # or the resend condition
                        await asyncio.wait_for(
                                asyncio.wait(req.successful.wait(), 
                                        req.failure.wait(), asyncio.FIRST_COMPLETED), req.timeout)
                        if self.successful.is_set():
                            break
                    except asyncio.TimeoutError:
                        # We timed out so set the failure flag ourselves
                        req.failure.set()

                # Wait for the mandatory quiet time after the request
                await asyncio.sleep(req.quiet_time)
        finally:
            logger.info('shutting down writer')

    async def _run_reader(self, conn):
        decoder = message.MsgDecoder(self.defs)
        buf = bytes()
        try:
            while True:
                try:
                    await self._conn.read(1)
                    msg = decoder.decode(buf)
                    if not msg:
                        continue
                except TypeError as te:
                    continue
                except Exception as e:
                    logger.error(str(e))
                    continue

                # notify all the open requests
                for ref in self._open_requests:
                    req = ref()
                    if not req:
                        self._open_requests.remove(ref)
                    else:
                        req.process(msg)

                # notify all the handlers
                handlers = list(self._read_handlers)
                for h in handlers:
                    h(msg)
        finally:
            logger.info('shutting down reader')
