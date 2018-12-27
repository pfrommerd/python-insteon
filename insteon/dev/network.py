import threading
from contextlib import contextmanager

_bound_network = threading.local()

class Network:
    def __init__(self):
        self._dev_by_name = {}
        self._dev_by_addr = {}

    def register(self, device):
        if device.name:
            self._dev_by_name[device.name] = device
        self._dev_by_addr[device.address] = device

    def get_by_address(self, addr):
        if addr in self._dev_by_addr:
            return self._dev_by_addr[addr]
        else:
            return None

    def print(self):
        for name, dev in self._dev_by_name.items():
            print('{} {}'.format(dev.address.human, name))

    def bind(self):
        stack = getattr(_bound_network, 'stack', None)
        if not stack:
            stack = []
            _bound_network.stack = stack
        stack.append(self)

    def unbind(self):
        stack = getattr(_bound_network, 'stack', None)
        if stack:
            stack.remove(self)

    @contextmanager
    def use(self):
        self.bind()
        yield
        self.unbind()

    @staticmethod
    def bound():
        stack = getattr(_bound_network, 'stack', None)
        if stack:
            return stack[-1]
        else:
            return None

