import insteon.io.message as msg
from ..io.address import Address

from . import network

import datetime
import json

import logbook
logger = logbook.Logger(__name__)

from warnings import warn

class LinkRecord:
    def __init__(self, offset=None, address=None, group=None, flags=None, data=None,
                        flags_mask=None):
        self.offset = offset
        self.address = address
        self.group = group
        self.flags = flags
        self.data = data
        self._flags_mask = flags_mask

    def mask_link_type(self):
        if not self._flags_mask:
            self._flags_mask = 0
        self._flags_mask |= (1 << 6)

    def mask_active(self):
        if not self._flags_mask:
            self._flags_mask = 0
        self._flags_mask |= (1 << 7)

    @property
    def null(self):
        return sum(self.bytes) == 0

    @property
    def active(self):
        if not self.flags:
            return False
        return self.flags & (1 << 7) > 0

    @active.setter
    def active(self, value):
        if not self.flags:
            self.flags = 0
        self.flags &= ~(1 << 7)
        if value:
            self.flags |= (1 << 7)

    @property
    def high_water(self):
        if not self.flags:
            return False
        return self.flags & (1 << 1) == 0

    @high_water.setter
    def high_water(self, value):
        if not self.flags:
            self.flags = 0
        self.flags &= ~(1 << 1)
        if not value:
            self.flags |= (1 << 1)

    @property
    def controller(self):
        if not self.flags:
            return False
        return self.flags & (1 << 6) > 0

    @controller.setter
    def controller(self, value):
        if not self.flags:
            self.flags = 0
        self.flags &= ~(1 << 6)
        if value:
            self.flags |= (1 << 6)

    @property
    def responder(self):
        return not self.controller

    @responder.setter
    def responder(self, value):
        self.controller = not value

    @property
    def bytes(self):
        return [(self.flags if self.flags else 0), \
                (self.group if self.group else 0)] + \
                self.address.array + \
                (self.data if self.data else [0, 0, 0])

    @property
    def packed(self):
        if self.offset:
            return {'offset': self.offset, 'address': self.address.packed,
                    'group': self.group, 'flags': self.flags, 'data': self.data}
        else:
            return {'address': self.address.packed,
                    'group': self.group, 'flags': self.flags, 'data': self.data}

    @staticmethod
    def unpack(packed):
        if 'offset' in packed:
            return LinkRecord(packed['offset'], Address.unpack(packed['address']),
                              packed['group'], packed['flags'], packed['data'])
        else:
            return LinkRecord(None, Address.unpack(packed['address']),
                              packed['group'], packed['flags'], packed['data'])

    def copy(self):
        return LinkRecord(self.offset, self.address, self.group,
                            self.flags, self.data)

    def matches(self, other):
        if self.offset is not None \
                and other.offset is not None \
                and self.offset != other.offset:
            return False
        if self.address is not None \
                and other.address is not None \
                and self.address != other.address:
            return False
        if self.group is not None \
                and other.group is not None \
                and self.group != other.group:
            return False
        if self.flags is not None \
                and other.flags is not None:
            filter_flags = self._flags_mask if self._flags_mask else other._flags_mask
            if filter_flags and self.flags & filter_flags != other.flags & filter_flags:
                return False
            if not filter_flags and self.flags != other.flags:
                return False
        if self.data is not None \
                and other.data is not None \
                and self.data != other.data:
            return False
        return True

    def __str__(self):
        flags = self.flags if self.flags else 0
        valid = (flags & (1 << 7))
        ltype = 'CTRL' if (flags & (1 << 6)) else 'RESP'
        ctrl = ' ' + ltype + ' ' if valid else '(' + ltype + ')'
        data_str = ' '.join([format(x & 0xFF, '02x') \
                             for x in (self.data if self.data else [0, 0, 0])])
        dev = self.address.human if self.address else 'xx.xx.xx'
        addr = self.address.human if self.address else 'xx.xx.xx'
        offset = self.offset if self.offset else 0xffff
        group = self.group if self.group else 0

        if network.Network.bound():
            device = network.Network.bound().get_by_address(self.address)
            if device:
                dev = device.name

        if self.offset:
            return '{:04x} {:30s} {:8s} {} {:08b} group: {:02x} data: {}'.format(
                    offset, dev, addr, ctrl, flags, group, data_str)
        else:
            return '{:30s} {:8s} {} {:08b} group: {:02x} data: {}'.format(
                    dev, addr, ctrl, flags, group, data_str)


class LinkDB:
    def __init__(self, records=None, timestamp=None):
        self.records = records if records else []
        self.timestamp = timestamp

    def __iter__(self):
        for r in self.records:
            yield r

    def __contains__(self, record):
        for r in self.records:
            if record.matches(r):
                return True
        return False

    @property
    def empty(self):
        return not self.records

    @property
    def valid(self):
        return self.timestamp is not None

    @property
    def size(self):
        return len(self.records)

    @property
    def start_offset(self):
        return 0x0fff

    @property
    def end_offset(self):
        last_off = 0x0fff
        for r in self.records:
            if not r.active:
                continue
            if r.offset and r.offset - 0x08 < last_off:
                last_off = r.offset - 0x08
        return last_off

    # Sets the timestamp of the device (if ts is None, sets it to the current time)
    def set_timestamp(self, ts=None):
        self.timestamp = ts if ts else datetime.datetime.now()

    def set_invalid(self):
        self.timestamp = None

    # Editing commands

    def at(self, offset):
        for r in self.records:
            if r.offset == offset:
                return r
        return None

    def remove(self, rec):
        self.records.remove(rec)

    def add(self, rec):
        if not rec in self.records:
            self.records.append(rec)

    def clear(self):
        self.records.clear()

    # For removing a particular device
    def remove_matching(self, filter_rec):
        for r in list(self.records):
            if filter_rec.matches(r):
                self.remove(r)

    def remove_device(self, dev):
        addr = dev.address
        self.remove_matching(LinkRecord(address=addr))

    def remove_address(self, address):
        self.remove_matching(LinkRecord(address=address))

    def remove_offset(self, offset):
        self.remove_matching(LinkRecord(offset=offset))

    # For adding new records
    def add_address(self, address, controller=False, group=0x01, data=[0, 0, 0]):
        rec = LinkRecord(address=address, group=0x01, data=data)
        rec.active = True
        rec.high_water = False
        rec.controller = controller
        self.add(rec)

    def add_device(self, device, controller=False, group=0x01, data=[0, 0, 0]):
        self.add_address(device.address, controller, group, data)

    def copy(self):
        return LinkDB([x.copy() for x in self.records], self.timestamp)

    # For filtering by a record
    # will return a new database
    def filter(self, filter_rec):
        rec = []
        for r in self.records:
            if filter_rec.matches(r):
                rec.append(r)
        db = LinkDB(rec, self.timestamp)
        return db

    # For serialization/unserialization
    @property
    def packed(self):
        packed = {}
        packed['timestamp'] = self.timestamp.strftime('%b %d %Y %H:%M:%S')
        records = []
        for r in self.records:
            records.append(r.packed)
        packed['records'] = records
        return packed

    @staticmethod
    def unpack(packed):
        timestamp = None
        if 'timestamp' in packed:
            timestamp = datetime.datetime.strptime(packed['timestamp'], '%b %d %Y %H:%M:%S')
        records = []
        if 'records' in packed:
            for r in packed['records']:
                records.append(LinkRecord.unpack(r))
        return LinkDB(records, timestamp)

    def load(self, filename):
        with open(filename, 'r') as i:
            packed = json.load(i)
            self.update(LinkDB.unpack(packed))

    def save(self, filename):
        with open(filename, 'w') as out:
            json.dump(self.packed, out)

    # Adds a bunch of records and sets the timestamp (if records has a timestamp property, it uses
    # that, otherwise it just uses the current time)
    def update(self, records):
        self.clear()
        for r in records:
            self.add(r)

        # If we are looking at another database
        # it will have a timestamp on it
        if hasattr(records, 'timestamp'):
            self.set_timestamp(records.timestamp)
        else:
            self.set_timestamp()

    def print(self, formatter=None):
        if not self.valid:
            logger.warning('LinkDB cache not valid!')
            return

        print(self.timestamp.strftime('Retrieved: %b %d %Y %H:%M:%S'))
        for rec in self.records:
            print(rec)
