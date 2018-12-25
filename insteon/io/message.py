from enum import Enum
import struct

class Direction(Enum):
    TO_MODEM = 'TO_MODEM'
    FROM_MODEM = 'FROM_MODEM'
    INVALID = 'INVALID'

class DataType(Enum):
    BYTE = 1
    INT = 4
    FLOAT = 4
    ADDRESS = 3
    INVALID = -1

class MsgType(Enum):
    BROADCAST = 0x80
    DIRECT = 0x00
    ACK_OF_DIRECT = 0x20
    NACK_OF_DIRECT = 0xa0
    ALL_LINK_BROADCAST = 0xc0
    ALL_LINK_CLEANUP = 0x40
    ALL_LINK_CLEANUP_ACK = 0x60
    ALL_LINK_CLEANUP_NACK = 0xe0

    def from_value(val):
        if not val:
            return None
        return MsgType(val & 0xe0)

    def from_msg(msg):
        if not 'messageFlags' in msg:
            return None

        return MsgType.from_value(msg['messageFlags'])

class AckType(Enum):
    ACK = 0x06
    NACK = 0x15

    def from_value(val):
        if not (val == 0x06 or val == 0x15):
            return None
        return AckType(val)

    def from_msg(msg):
        if not 'ACK/NACK' in msg:
            return None
        return AckType(msg['ACK/NACK'])

# Message behaves like a dictionary
# with a associated definition
class Msg:
    def __init__(self, msg_def, msg=None):
        self._def = msg_def
        self._msg = msg if msg else {} # The message dictionary

    def __contains__(self, name):
        return name in self._msg

    def __getitem__(self, name):
        return self._msg[name]

    def __setitem__(self, name, val):
        self._msg[name] = val

    def __str__(self):
        if self._def is None:
            return '<Unknown>'
        else:
            return self._def.format_msg(self)

    @property
    def type(self):
        return self._def.name

    @property
    def bytes(self):
        if self._def is None:
            return bytes()
        else:
            return self._def.serialize(self)

    def copy(self):
        return Msg(self._def,
                copy.deepcopy(self._msg))

class MsgDecoder:
    def __init__(self, defs = {}, direction=Direction.FROM_MODEM):
        self._buf = bytearray()
        self._direction = direction

        self._all_defs = defs.values()
        self._reset_filtered_defs()

    def _reset_filtered_defs(self):
        self._filtered_defs = list(filter(lambda d: d.direction == self._direction,
                                                        self._all_defs))

    def decode(self, buf):
        if len(self._filtered_defs) < 1:
            self._reset_filtered_defs()

        self._buf += buf

        while True:
            # A one-byte message isn't currently allowed!
            if len(self._buf) < 2:
                break
            
            # If we haven't yet determined the message, filter
            if len(self._filtered_defs) > 1: 
                self._filtered_defs = list(filter(lambda d: d.matches(self._buf), self._filtered_defs))
            # Now check to see if we are done or are totally lost
            if len(self._filtered_defs) == 1 and \
                    self._filtered_defs[0].length <= len(self._buf):
                # Done with message! Get the definition
                d = self._filtered_defs[0]
                # Pop off the required length and convert to immutable bytes
                msg_buf = bytes(self._buf[:d.length])
                self._buf = self._buf[d.length:]

                # Reset filtered defs
                self._reset_filtered_defs()

                return d.deserialize(msg_buf)
            elif len(self._filtered_defs) < 1:
                # Flush a single byte and reset the filter
                self._buf = self._buf[1:]

                self._reset_filtered_defs()
                continue
            # We can't determine anything yet
            break
        return None
