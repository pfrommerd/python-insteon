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
                       filter_flags_mask=None, filter_link_type=False, filter_controller=False):
        self.offset = offset
        self.address = address
        self.group = group
        self.flags = flags
        self.data = data

        self._filter_flags = filter_flags_mask
        if filter_link_type and not self._filter_flags:
            self._filter_flags =  (1 << 6)
        if self._filter_flags and filter_controller:
            self.flags = self.flags | (1 << 6)

    @property
    def active(self):
        return self.flags & (1 << 7) > 0

    @property
    def controller(self):
        return self.flags & (1 << 6) > 0

    @property
    def responder(self):
        return not self.controller

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
            filter_flags = self._filter_flags if self._filter_flags else other._filter_flags
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
        valid = (self.flags & (1 << 7))
        ltype = 'CTRL' if (self.flags & (1 << 6)) else 'RESP'
        ctrl = ' ' + ltype + ' ' if valid else '(' + ltype + ')'
        data_str = ' '.join([format(x & 0xFF, '02x') for x in self.data])

        dev = self.address.human
        if network.Network.bound():
            device = network.Network.bound().get_by_address(self.address)
            if device:
                dev = device.name

        if self.offset:
            return '{:04x} {:30s} {:8s} {} {:08b} group: {:02x} data: {}'.format(
                    self.offset, dev, self.address.human, ctrl, self.flags, self.group, data_str)
        else:
            return '{:30s} {:8s} {} {:08b} group: {:02x} data: {}'.format(
                    dev, self.address.human, ctrl, self.flags, self.group, data_str)

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
    def end_offset(self):
        last_off = 0x0fff
        for r in self.records:
            if r.offset and r.offset - 0x08 < last_off:
                last_off = r.offset - 0x08
        return last_off

    # Sets the timestamp of the device (if ts is None, sets it to the current time)
    def set_timestamp(self, ts=None):
        self.timestamp = ts if ts else datetime.datetime.now()

    def set_invalid(self):
        self.timestamp = None

    def add(self, rec):
        if not rec in self.records:
            self.records.append(rec)

    def clear(self):
        self.records.clear()

    # For filtering by a record
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
