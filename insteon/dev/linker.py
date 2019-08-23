from ..util import InsteonError

import logbook
logger = logbook.Logger(__name__)

class Linker:
    def __init__(self, dev):
        self._dev = dev

    def start_linking_responder(self, group=0x01, port=None):
        pass

    def start_linking_controller(self, group=0x01, port=None):
        pass

    def stop_linking(self, port=None):
        pass

class ModemLinker(Linker):
    def __init__(self, modem):
        super().__init__(modem)

    def start_linking_responder(self, group=0x01, port=None):
        port = port if port else self._dev.port
        if not port:
            raise InsteonError('No port specified')

        msg = port.defs['StartALLLinking'].create()
        msg['LinkCode'] = 0x00
        msg['ALLLinkGroup'] = group

        ack_reply = Channel()
        port.write(msg, ack_reply_channel=ack_reply)

        if not ack_reply.wait(1):
            raise InsteonError('Received no reply')

    def start_linking_controller(self, group=0x01, port=None):
        port = port if port else self._dev.port
        if not port:
            raise InsteonError('No port specified')

        msg = port.defs['StartALLLinking'].create()
        msg['LinkCode'] = 0x01
        msg['ALLLinkGroup'] = group

        ack_reply = Channel()
        port.write(msg, ack_reply_channel=ack_reply)

        if not ack_reply.wait(1):
            raise InsteonError('Received no reply')

    def stop_linking(self, port=None):
        port = port if port else self._dev.port
        if not port:
            raise InsteonError('No port specified')

        msg = port.defs['CancelALLLinking'].create()
        ack_reply = Channel()
        port.write(msg, ack_reply_channel=ack_reply)

        if not ack_reply.wait(1):
            raise InsteonError('Received no reply')

class GenericLinker(Linker):
    def __init__(self, dev):
        super().__init__(dev)

    def start_linking_controller(self, group=0x01, port=None):
        port = port if port else self._dev.port
        if not port:
            raise InsteonError('No port specified')
        self._dev.querier.query_ext(0x09, 0x01, [])

    def start_linking_responder(self, group=0x01, port=None):
        self.start_linking_controller(group, port)

    def stop_linking(self, port=None):
        raise InsteonError('Not implemented')
