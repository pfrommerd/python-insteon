from .device import Device
from .querier import Querier
from .linker import GenericLinker
from .dbmanager import GenericDBManager

class LightState:
    def __init__(self, dev):
        self._dev = dev
    
    async def set_on(self, onLevel=1, port=None):
        self._dev.querier.query_std(0x11, max(0, min(255, int(255*onLevel))), port=port)

    async def set_off(self, port=None):
        self._dev.querier.query_std(0x13, 0x00, port=port)

class Light(Device):
    def __init__(self, name, address, net=None, modem=None):
        super().__init__(name, address, net, modem)
        self.add_feature('querier', Querier(self))
        self.add_feature('db', GenericDBManager(self))
        self.add_feature('linker', GenericLinker(self))
        self.add_feature('state', LightState(self))
