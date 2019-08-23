from ..util import InsteonError, calc_simple_crc, calc_long_crc
from ..io.message import MsgType

class Querier:
    def __init__(self, dev):
        self._dev = dev

    def send_std(self, cmd1, cmd2, flag=MsgType.DIRECT,
                        wait_response=False, extra_channels=[], port=None):
        port = port if port else self._dev.port

        msg = port.defs['SendStandardMessage'].create()
        msg['toAddress'] = self._dev.address
        msg['messageFlags'] = flag.value | 0xf
        msg['command1'] = cmd1
        msg['command2'] = cmd2

        direct_ack_channel = Channel(lambda x: x.type == 'StandardMessageReceived' and
                                            (MsgType.from_value(x['messageFlags']) == MsgType.ACK_OF_DIRECT or
                                             MsgType.from_value(x['messageFlags']) == MsgType.NACK_OF_DIRECT) and
                                            x['fromAddress'] == self._dev.address)
        ack_reply = Channel()

        extras = [direct_ack_channel]
        extras.extend(extra_channels)

        port.write(msg, ack_reply_channel=ack_reply, custom_channels=extras)

        if not ack_reply.wait(3):
            raise InsteonError('No IM reply to send command!')

        if wait_response:
            if not direct_ack_channel.wait(4):
                raise InsteonError('No response to standard query received')

        return direct_ack_channel

    def query_std(self, cmd1, cmd2, flag=MsgType.DIRECT,
                    wait_response=True, extra_channels=[], port=None):
        return self.send_std(cmd1, cmd2, flag, wait_response, extra_channels, port)

    def send_ext(self, cmd1, cmd2, data, flag=MsgType.DIRECT, large_checksum=False,
                    wait_response=False, extra_channels=[], port=None):
        port = port if port else self._dev.port

        msg = port.defs['SendExtendedMessage'].create()
        msg['toAddress'] = self._dev.address
        msg['messageFlags'] = flag.value | (1 << 4) | 0xf
        msg['command1'] = cmd1
        msg['command2'] = cmd2

        if large_checksum and len(data) > 12 or len(data) > 13:
            raise InsteonError('Cannot send more than 12 or 13 bytes in an ext message')

        for i,x in enumerate(data):
            msg['userData{}'.format(i + 1)] = x

        checksum_data = [cmd1, cmd2]
        checksum_data.extend(data)

        if large_checksum:
            crc = calc_long_crc(checksum_data)
            msg['userData13'] = crc & 0xFF
            msg['userData14'] = (crc >> 8) & 0xFF
        else:
            msg['userData14'] = calc_simple_crc(checksum_data)

        direct_ack_channel = Channel(lambda x: (x.type == 'ExtendedMessageReceived' or 
                                                x.type == 'StandardMessageReceived') and
                                            (MsgType.from_value(x['messageFlags']) == MsgType.ACK_OF_DIRECT or
                                             MsgType.from_value(x['messageFlags']) == MsgType.NACK_OF_DIRECT))
        ack_reply = Channel()

        extras = [direct_ack_channel]
        extras.extend(extra_channels)

        port.write(msg, ack_reply_channel=ack_reply, custom_channels=extras)

        if not ack_reply.wait(3):
            raise InsteonError('No IM reply to send command!')

        if wait_response:
            if not direct_ack_channel.wait(4):
                raise InsteonError('No response to extended query received')

        return direct_ack_channel

    def query_ext(self, cmd1, cmd2, data, flag=MsgType.DIRECT, large_checksum=False,
                    wait_response=True, extra_channels=[], port=None):
        return self.send_ext(cmd1, cmd2, data, flag, large_checksum, wait_response, extra_channels, port)
