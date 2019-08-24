import datetime

from .linkdb import LinkDB,LinkRecord
from . import linkdb
from ..util import InsteonError
from ..io.address import Address

import time

import logbook
logger = logbook.Logger(__name__)

class DBManager:
    def __init__(self, dev):
        self.cache = linkdb.LinkDB()
        self._dev = dev

    # Will update/return the target db,
    # create a new database if None
    async def update_cache(self, targetdb=None, allow_linking=False, port=None):
        port = port if port else self._dev.port

        if not targetdb:
            targetdb = self.cache

        # _retrieve() returns a LinkDB() object
        records = await self._retrieve(port)
        # check if based on the retrieved
        # records we would have the permissions
        # to get a proper db
        if not self._check_permissions(records):
            if not await self._grant_permissions(allow_linking):
                logger.error('Not linked as a controller so cannot retrieve database. Use allow_linking to auto-link the modem as a controller of this device')
                raise InsteonError('Not linked as a controller so cannot access database')
            # Get a fresh copy now that we have permissions
            records = await self._retrieve(port)
            if not self._check_permissions(records):
                raise InsteonError('Failed to get permission to modify the database')

        targetdb.update(records)

        return targetdb

    async def flash_cache(self, srcdb=None, allow_linking=False, port=None):
        port = port if port else self._dev.port

        if not srcdb:
            srcdb = self.cache

        if not srcdb.valid:
            logger.error('The source database provided is not valid in that it has not have')
            logger.error('a timestamp and so is not considered "populated" to prevent accidentally')
            logger.error('wipping the database')
            raise InsteonError('Source database for flashing has not been set as populated')

        logger.trace('Getting latest linkdb')
        currentdb = linkdb.LinkDB()
        # Make a backup ....
        # Retrieve the current DB into a "currentdb" variable
        await self.update_cache(targetdb=currentdb, allow_linking=allow_linking, port=port)

        # Save the currentdb
        backfile_name = '{}.linkdb.bk'.format(datetime.datetime.now().strftime('%b_%d_%Y_%H:%M:%S'))

        logger.warning('Modifying Link Database. This can go catastrophically wrong. A backup of the current database has been written to {}', backfile_name)
        currentdb.save(backfile_name)

        logger.trace('Writing linkdb changes')
        await self._write(port, srcdb, currentdb)

    # Checks if the db has the permissions for
    # us to read
    def _check_permissions(self, currentdb):
        return True

    # Makes a database writable if the permission
    # check fails
    def _grant_permissions(self, allow_linking):
        return True

    # The actual implementation
    # that returns a database
    # retrieved
    async def _retrieve(self, port):
        pass

    # The actual implementation that writes
    # a database given a current database and
    # a source database to write
    async def _write(self, port, srcdb, currentdb):
        pass


#
# -----------------------------------------------------------------
# -------- Now specific database managers--------------------------
# -----------------------------------------------------------------
#

class GenericDBManager(DBManager):
    def __init__(self, dev):
        super().__init__(dev)

    def _check_permissions(self, currentdb):
        modem_addr = self._dev.modem.address

        # Check if the modem is in the currentdb
        # to see if we have the permissions to flash the database
        rec_filter = LinkRecord(address=modem_addr, flags_mask=(1 << 6))
        if rec_filter in currentdb:
            return True
        return False

    def _grant_permissions(self, allow_linking):
        if not allow_linking:
            return False
        # Link these things, check if there is a linker
        # feature for the modem and the device
        if not hasattr(self._dev, 'linker') or \
            not hasattr(self._dev.modem, 'linker'):
            raise InsteonError('Cannot link to device in order to read database since the device is missing the linker feature')

        logger.debug('Linking modem {} as controller to {}'.format(self._dev.modem.address,
                                                                    self._dev.address))

        self._dev.modem.linker.start_linking_controller()
        time.sleep(0.1) # Put a little sleep in there
        self._dev.linker.start_linking_responder()
        time.sleep(1) # Put a little sleep in there so the device has time to change its db
        return True

    def _retrieve(self, port):
        querier = self._dev.querier

        record_channel = Channel(lambda x: x.type == 'ExtendedMessageReceived' and \
                                            x['Cmd'] == 0x51 and \
                                            x['fromAddress'] == self._dev.address)
        response_channel = querier.query_ext(0x2f, 0x00, [], extra_channels=[record_channel])

        if not response_channel.recv(1):
            raise InsteonError('Did not get reply from device')

        db = linkdb.LinkDB()

        # Now keep receiving on the record channel until none are left
        while record_channel.wait(5):
            msg = record_channel.recv(5)
            offset = (msg['userData3'] & 0xFF) << 8 | (msg['userData4'] & 0xFF)
            flags = msg['userData6'] & 0xFF 
            group = msg['userData7'] & 0xFF
            address = Address(msg['userData8'], msg['userData9'], msg['userData10'])
            data = [msg['userData11'], msg['userData12'], msg['userData13']] 

            if not linkdb.LinkRecord(offset=offset) in db:
                db.add(linkdb.LinkRecord(offset, address, group, flags, data))

        return db

    def _write_entry(self, offset, record):
        record = record.copy()
        record.offset = None
        logger.debug('Writing record to {:04x}: {}'.format(offset, record))
        req_data = [0x00, 0x02]
        req_data.append((offset >> 8) & 0xFF)
        req_data.append(offset & 0xFF)
        req_data.append(8) # Set 8 bytes

        record_bytes = record.bytes
        # Make sure if nothing else that the high water
        # bit is set. If it isn't we could really brick the device
        record_bytes[0] = record_bytes[0] | 0x10
        req_data.extend(record_bytes)
        self._dev.querier.query_ext(0x2f, 0x00, req_data)
        time.sleep(1)

    def _null_entry(self, offset):
        logger.debug('Setting database end at {:04x}'.format(offset))
        req_data = [0x00, 0x02]
        req_data.append((offset >> 8) & 0xFF)
        req_data.append(offset & 0xFF)
        req_data.append(8) # Set 8 bytes
        req_data.extend(8*[0x00]) # null out the entry
        self._dev.querier.query_ext(0x2f, 0x00, req_data)
        time.sleep(1)

    def _write(self, port, srcdb, currentdb):
        # When nuking a database unlink the modem last
        modem_addr = self._dev.modem.address

        delete_records = []
        for record in currentdb:
            if not record.active:
                continue
            filter_rec = record.copy()
            filter_rec.offset = None # We don't care about offset
            if any(map(lambda x: x.offset < record.offset and filter_rec.matches(x), currentdb)):
                logger.debug('Found dup  record: {}'.format(record))
                delete_records.append(record)
            elif not filter_rec in srcdb:
                logger.debug('Found del  record: {}'.format(record))
                delete_records.append(record)


        free_records = list(delete_records)
        for record in currentdb:
            if not record.active:
                logger.debug('Found free record: {}'.format(record))
                free_records.append(record)
                if record.null:
                    # Append a bunch of dummy records after
                    for i in range(srcdb.size):
                        if record.offset - (i + 1) * 0x08 > 0x08:
                            free_records.append(LinkRecord(offset=record.offset - (i + 1)*0x08))

        logger.trace('Writing new records')

        # if the modem is being removed, add the modem at the first free offset temporarily
        modem_record = LinkRecord(address=modem_addr)
        modem_record.mask_link_type()
        modem_record.mask_active()
        modem_record.active = True
        modem_record.controller = False
        modem_record.high_water = False

        modem_offset = None
        if any(map(lambda x: modem_record.matches(x), delete_records)):
            if len(free_records) == 0:
                raise InsteonError('Out of database space!')
            modem_offset = free_records.pop(0).offset
            logger.trace('Writing modem record for editing into first free space')
            self._write_entry(modem_offset, modem_record)

        # Write any new entries into the free
        # spaces
        for record in srcdb:
            if not record.active:
                continue
            filter_rec = record.copy()
            filter_rec.offset = None # We don't care about offset
            if not filter_rec in currentdb:
                if len(free_records) == 0:
                    raise InsteonError('Out of database space!')
                free_offset = free_records.pop(0).offset
                self._write_entry(free_offset, record)

        logger.trace('Fetching fresh copy of database')
        currentdb = linkdb.LinkDB()
        self.update_cache(targetdb=currentdb, port=port)

        logger.trace('Disabling inactive records')

        # Disable anything that shouldn't be in the database
        for record in currentdb:
            if not record.active:
                continue
            if any(map(lambda x: record.matches(x), delete_records)):
                if modem_offset and record.offset == modem_offset:
                    continue # We'll handle this one at the end 
                new_record = record.copy()
                new_record.active = False
                self._write_entry(new_record.offset, new_record)

        logger.trace('Fetching fresh copy of database')
        currentdb = linkdb.LinkDB()
        self.update_cache(targetdb=currentdb, port=port)
        logger.trace('Setting end to null entry')

        # Chop off after the end
        self._null_entry(currentdb.end_offset)

        if modem_offset:
            logger.trace('Fetching fresh copy of database')
            currentdb = linkdb.LinkDB()
            self.update_cache(targetdb=currentdb, port=port)
            logger.trace('Disabling modem record')
            # If the modem is the last record, just wipe the database
            if modem_offset == currentdb.end_offset - 0x08:
                self._null_entry(modem_offset)
            else:
                new_record = currentdb.at(modem_offset).copy()
                new_record.active = False
                self._write_entry(new_record.offset, new_record)



class ModemDBManager(DBManager):
    def __init__(self, dev):
        super().__init__(dev)

    async def _retrieve(self, port):
        records = []
        first = True
        while True:
            msg_type = 'GetFirstALLLinkRecord' if first else 'GetNextALLLinkRecord'
            first = False

            with port.write(port.defs[msg_type].create()) as req:
                res = await req.wait_success_fail(timeout=5)
                req.consume(res)
                if not res:
                    raise InsteonError('Did not get reply for modem DB query')
                if res['ACK/NACK'] == 0x15:
                    break # Hit the end
                msg = await req.wait_until(lambda m: m.type == 'ALLLinkRecordResponse', timeout=2)
                req.consume(msg)

                rec = linkdb.LinkRecord(None, msg['LinkAddr'], msg['ALLLinkGroup'],
                                        msg['RecordFlags'],
                                        [msg['LinkData1'], msg['LinkData2'], msg['LinkData3']])
                records.append(rec)
        
        db = linkdb.LinkDB()
        db.update(records)

        return db

    def _write(self, port, srcdb, currentdb):
        for record in currentdb:
            filter_rec = record.copy()
            filter_rec.offset = None
            # If we do not find this record (regardless of offset) in the source
            # database, delete it!
            if not filter_rec in srcdb:
                logger.debug('Deleting record {}', record)

                msg = port.defs['ManageALLLinkRecord'].create()
                msg['controlCode'] = 0x80 # Delete by search
                msg['recordFlags'] = record.flags
                msg['ALLLinkGroup'] = record.group
                msg['linkAddress'] = record.address
                msg['linkData1'] = record.data[0]
                msg['linkData2'] = record.data[1]
                msg['linkData2'] = record.data[2]

                # Send the delete message and wait for a response
                ack_reply = Channel()
                port.write(msg, ack_reply_channel=ack_reply)
                reply_msg = ack_reply.recv(2)
                if reply_msg['ACK/NACK'] != 0x06:
                    raise InsteonError('The modem couldn\'t find the record we wanted to delete!')
                elif not reply_msg:
                    raise InsteonError('No reply to delete message')

        try:
            currentdb = linkdb.LinkDB()
            self.update_cache(targetdb=currentdb, port=port)
        except InsteonError as e:
            raise InsteonError('Unable to get database after removing records!') from e

        for record in srcdb:
            filter_rec = record.copy()
            filter_rec.offset = None
            if not filter_rec in currentdb:
                logger.debug('Adding record {}', record)
                msg = port.defs['ManageALLLinkRecord'].create()
                # Add resp. or controller
                msg['controlCode'] = 0x40 if (record.flags & (1 << 6)) else 0x41
                msg['recordFlags'] = record.flags
                msg['ALLLinkGroup'] = record.group
                msg['linkAddress'] = record.address
                msg['linkData1'] = record.data[0]
                msg['linkData2'] = record.data[1]
                msg['linkData3'] = record.data[2]

                ack_reply = Channel()
                port.write(msg, ack_reply_channel=ack_reply)
                if not ack_reply.wait(2):
                    raise InsteonError('No reply on record add!')
