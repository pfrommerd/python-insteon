from .device import Device
from .network import Network

from ..io.address import Address
from ..util import Channel

import threading
from contextlib import contextmanager

import logbook
logger = logbook.Logger(__name__)

_bound_modem = threading.local()

class Modem(Device):
    # Query for the address...
    def __init__(self, name, port, net=None):
        self._port = port

        addr = Address()
        # Query for the modem address
        addr_query = port.defs['GetIMInfo'].create()

        reply_channel = Channel()
        port.write(addr_query, ack_reply_channel=reply_channel)
        if reply_channel.wait(5): # Wait for a reply
            msg = reply_channel.recv()
            addr = msg['IMAddress']

        super().__init__(name, addr, net, self)

        # Add the features
        from .dbmanager import ModemDBManager
        self.add_feature('db', ModemDBManager(self))

        from .linker import ModemLinker
        self.add_feature('linker', ModemLinker(self))

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
