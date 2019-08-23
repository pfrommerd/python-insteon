from .device import Device
from .network import Network

from ..io.address import Address

import threading
from contextlib import contextmanager

import logbook
logger = logbook.Logger(__name__)

_bound_modem = threading.local()

class Modem(Device):
    # Query for the address...
    def __init__(self, name, addr, port, net=None):
        self._port = port


        super().__init__(name, addr, net, self)

        # Add the features
        from .dbmanager import ModemDBManager
        self.add_feature('db', ModemDBManager(self))

        from .linker import ModemLinker
        self.add_feature('linker', ModemLinker(self))

    @staticmethod
    async def auto_create(name, port, net=None):
        addr = Address()
        # Query for the modem address
        addr_query = port.defs['GetIMInfo'].create()

        with port.write(addr_query) as req:
            if await req.wait_success_fail('GetIMInfoReply', timeout=5):
                addr = req.response['IMAddress']

        return Modem(name, addr, port, net)

    def bind(self):
        stack = getattr(_bound_modem, 'stack', None)
        if not stack:
            stack = []
            _bound_modem.stack = stack
        stack.append(self)

    def unbind(self):
        stack = getattr(_bound_modem, 'stack', None)
        if stack:
            stack.remove(self)

    @contextmanager
    def use(self):
        self.bind()
        yield
        self.unbind()

    @staticmethod
    def bound():
        stack = getattr(_bound_modem, 'stack', None)
        if stack:
            return stack[-1]
        else:
            return None
