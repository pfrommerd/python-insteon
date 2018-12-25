from . import address
from . import message
from .message import DataType
from .message import Direction

class FieldDef:
    def __init__(self, offset, length,
                 field_type,
                 name=None, default_value=None,
                 header_filter=None): # Header-filter is used for fields in the header
                                      # to see if a message matches a certain msg def
        self.offset = offset
        self.length = length
        self.type = field_type
        self.name = name
        self.default_value = default_value
        self.header_filter = header_filter

    def get(self, buf):
        o = self.offset
        if self.type == DataType.BYTE:
            return int.from_bytes(buf[o:o+1], byteorder='big')
        elif self.type == DataType.INT:
            return int.from_bytes(buf[o:o+4], byteorder='big')
        elif self.type == DataType.FLOAT:
            return struct.unpack('f', buf[o:o+4])[0]
        elif self.type == DataType.ADDRESS:
            return address.Address(int.from_bytes(buf[o:o+1], byteorder='big'),
                                   int.from_bytes(buf[o+1:o+2], byteorder='big'),
                                   int.from_bytes(buf[o+2:o+3], byteorder='big'))
        else:
            return None

    def set(self, buf, val):
        o = self.offset
        if self.type == DataType.BYTE:
            buf[o] = val.to_bytes(1, byteorder='big')[0]
        elif self.type == DataType.INT:
            buf[o:o+4] = val.to_bytes(4, byteorder='big')
        elif self.type == DataType.FLOAT:
            buf[o:o+4] = struct.pack('f', val)
        elif self.type == DataType.ADDRESS:
            buf[o:o+3] = val.bytes

    def format(self, msg=None):
        val = msg[self.name] if msg and self.name in msg else None

        if val is None:
            val = self.default_value

        if val is not None:
            valstr = str(val)
            if self.type == DataType.ADDRESS:
                valstr = val.human
            elif self.type == DataType.BYTE:
                valstr = '0x{:02x}'.format(val)
        else:
            valstr = '???'

        return self.name + ':' + valstr

class MsgDef:
    def __init__(self, name='', direction=Direction.TO_MODEM):
        self.name = name
        self.header_length = 0
        self.length = 0
        self.direction = direction
        self.fields_list = []
        self.fields_map = {}

    # Gets a field definition from
    # a message definition
    def __contains__(self, name):
        return name in self.fields_map

    def __getitem__(self, name):
        return self.fields_map[name]
    
    def append(self, field):
        self.length = self.length + field.length # Extend the length
        if field.name:
            self.fields_map[field.name] = field
        self.fields_list.append(field)

    # Check if the header matches
    def matches(self, buf):
        check_len = min(self.header_length, len(buf))
        for f in filter(lambda x: x.offset + x.type.value <= check_len and x.header_filter,
                                                                        self.fields_list):
            # Check the filter
            val = f.get(buf)
            if not (f.header_filter)(val):
                return False
        return True


    def create(self):
        return message.Msg(self)

    def deserialize(self, buf):
        m = message.Msg(self)
        for f in self.fields_list:
            if f.name is not None:
                val = f.get(buf)
                m[f.name] = val
        return m

    def serialize(self, msg):
        # Assume the 'type' field has already been set
        buf = bytearray(self.length)
        for f in self.fields_list:
            val = msg[f.name] if f.name is not None and \
                                    f.name in msg else \
                                    f.default_value
            if val is not None:
                f.set(buf, val)
        return bytes(buf)

    def format_msg(self, msg):
        sep = ('<','>') if self.direction == Direction.TO_MODEM else ('[',']')
        fields_str = '|'.join(map(lambda x: x.format(msg),
            filter(lambda x: x.name is not None, self.fields_list)))
        return sep[0] + self.name + sep[1] + ':' + fields_str

    def __str__(self):
        sep = ('<','>') if self.direction == Direction.TO_MODEM else ('[',']')
        fields_str = '|'.join(map(lambda x: x.format(),
            filter(lambda x: x.name is not None, self.fields_list)))
        return sep[0] + self.name + sep[1] + '(def):' + fields_str
