import threading
from contextlib import contextmanager

_bound_network = threading.local()

class Network:
    def __init__(self):
        self._dev_by_name = {}
        self._dev_by_addr = {}

    def register(self, device):
        if device.name:
            self.dev_by_name[device.name] = device
        self.dev_by_addr[device.address] = device

    @contextmanager
    def bind(self):
        old = Network.bound()
        _bound_network.network = self
        yield
        _bound_network.network = old

    @staticmethod
    def bound():
        return getattr(_bound_network, 'network', None)
