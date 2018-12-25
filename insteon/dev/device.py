class Device:
    def __init__(self, name, addr, net=None, modem_=None):
        from . import network
        from . import modem

        self._name = name
        self._addr = addr
        self._features = {}
        self._network = net if net else network.Network.bound()
        self._modem = modem_ if modem_ else modem.Modem.bound()

        if self._network:
            self._network.register(self)

    @property
    def name(self):
        return self._name

    @property
    def address(self):
        return self._addr

    @property
    def features(self):
        return self._features

    @property
    def network(self):
        return self._network

    @property
    def modem(self):
        return self._modem

    @property
    def port(self):
        if self._modem:
            return self._modem._port
        else:
            return None

    def add_feature(self, name, feature):
        setattr(self, name, feature)
        self.features[name] = feature
