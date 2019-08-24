import weakref
import asyncio
import traceback

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
        self.response = None # set on wait() for convenience so this is always the last wait


    # ------------- Lifetime Management Functions --------------

    def __del__(self):
        # set this for sure when the object is deleted
        # so we don't have a leak
        self.done.set()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
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
    def consume(self, msg):
        if msg:
            self.responses.remove(msg)

    # will eat upto a particular message
    def consume_until(self, msg):
        for i in range(len(responses)):
            if responses[i] == msg:
                self.responses = self.responses[i + 1:]

    def wait(self, timeout=0):
        try:
            if timeout > 0:
                return asyncio.wait_for(self._wait(), timeout)
            else:
                return self._wait()
        except asyncio.TimeoutError:
            self.response = None
            return None

    def wait_until(self, predicate, timeout=0):
        try:
            if timeout > 0:
                return asyncio.wait_for(self._wait_until(predicate), timeout)
            else:
                return self._wait_until(predicate)
        except asyncio.TimeoutError:
            print('timed out')
            self.response = None
            return None

    async def wait_success_fail(self, success_type=None, timeout=0):
        # default success type
        if not success_type:
            success_type = self.message.type + 'Reply'

        def handle(msg):
            if msg.type == 'PureNACK':
                self.failure.set()
            if msg.type == success_type:
                self.successful.set()
                return True
            return False 
        await self.written.wait()
        return await self.wait_until(handle, timeout)

    # The underlying wait functions
    # are wrapped above to have timeouts
    async def _wait(self):
        if len(self.responses) > 0:
            return self.responses[0]

        await self.received.acquire()
        try:
            await self.received.wait()
            self.response = self.responses[0]
            return self.response
        finally:
            self.received.release()

    async def _wait_until(self, predicate):
        for r in self.responses:
            if predicate(r):
                return r

        await self.received.acquire()
        try:
            while True:
                await self.received.wait()
                resp = self.responses[-1]
                if predicate(resp):
                    self.response = resp
                    return resp
        finally:
            self.received.release()

    # called by the port when a message is matched to
    # this request
    async def process(self, msg):
        await self.received.acquire()
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

        self._watch_write = lambda m: logger.info(f'wrote: {m}')
        self._watch_read = lambda m: logger.info(f'read: {m}')

        # if using the start, stop api this will be set
        self._task = None

    def start(self, conn, loop=None):
        if not loop:
            loop = asyncio.get_event_loop()
        self._open_requests.clear()
        self._queue = asyncio.PriorityQueue() # clear the queue

        self._task = loop.create_task(self._run(conn))
        return self._task

    def notify_write(self, h):
        self._write_handlers.append(h)

    def notify_read(self, h):
        self._read_handlers.append(h)

    def stop_notify_write(self, h):
        self._write_handlers.remove(h)

    def stop_notify_read(self, h):
        self._read_handlers.remove(h)

    def start_watching(self):
        self.notify_write(self._watch_write)
        self.notify_read(self._watch_read)

    def stop_watching(self):
        self.stop_notify_write(self._watch_write)
        self.stop_notify_read(self._watch_read)

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    """ Write returns a request object through which the 
        caller can get access to a queue containing all future messages that have been sent """
    def write(self, msg, priority=1, retries=5, timeout=0.1, quiet=0.1):
        req = Request(msg, retries, timeout, quiet)
        self._queue.put_nowait((priority, req))
        return req

    async def _run(self, conn):
        try:
            await asyncio.gather(self._run_writer(conn), self._run_reader(conn))
        except asyncio.CancelledError:
            raise
        finally:
            pass

    async def _run_writer(self, conn):
        try:
            while True:
                pri, req = await self._queue.get()
                
                # Put a weak reference to the request in the open requests list
                self._open_requests.append(weakref.ref(req))

                # Do the writing
                for try_num in range(req.remaining):
                    # bump tries and clear failure flag
                    req.tries = try_num + 1
                    req.failure.clear()

                    # do the writing... (synchronously?)
                    await conn.write(req.message.bytes)
                    await conn.flush()

                    # set that the request has been written
                    req.written.set()

                    # notify the handlers that it has been written
                    handlers = list(self._write_handlers)
                    for h in handlers:
                        h(req.message)

                    try:
                        # wait for either the continue event to trigger
                        # or the resend condition
                        waitables = {req.successful.wait(), req.failure.wait()}
                        _, pending = await asyncio.wait_for(asyncio.wait(waitables, 
                                                        return_when=asyncio.FIRST_COMPLETED), 
                                                        req.timeout)
                        # cancel any pending waits
                        for w in pending:
                            w.cancel()

                        if req.successful.is_set():
                            break
                    except asyncio.CancelledError:
                        raise
                    except asyncio.TimeoutError:
                        # We timed out so set the failure flag ourselves
                        req.failure.set()

                # Wait for the mandatory quiet time after the request
                await asyncio.sleep(req.quiet_time)
        except EOFError:
            pass
        finally:
            pass

    async def _run_reader(self, conn):
        decoder = message.MsgDecoder(self.defs)
        buf = bytes()
        try:
            while True:
                try:
                    buf = await conn.read(1)
                    if buf is None:
                        raise EOFError()
                    msg = decoder.decode(buf)
                    if not msg:
                        continue
                except asyncio.CancelledError:
                    raise
                except TypeError:
                    raise
                except EOFError:
                    raise
                except Exception as e:
                    logger.error(str(e))
                    continue

                # notify all the open requests
                for ref in self._open_requests:
                    req = ref()
                    if not req:
                        self._open_requests.remove(ref)
                    else:
                        await req.process(msg)

                # notify all the handlers
                handlers = list(self._read_handlers)
                for h in handlers:
                    h(msg)
        except EOFError:
            pass
        except asyncio.CancelledError:
            raise
        except:
            traceback.print_exc()
        finally:
            pass
