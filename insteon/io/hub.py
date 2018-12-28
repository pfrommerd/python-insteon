import requests
import os
import threading
import binascii
import time
import sys

import logbook
logger = logbook.Logger(__name__)

class HubConn:
    def __init__(self, host, port, username=None, password=None, poll_time=1, timeout=0.1):
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._poll_time = poll_time
        self._read_timeout = timeout
        self._open = True

        self._lock = threading.RLock()
        self._idx = -1 # Buffer index
        self._read_cond = threading.Condition()
        self._read_buffer = bytearray()

        # Start threads
        self._reader = threading.Thread(target=self._read_thread, daemon=True)
        self._reader.stop = self.close

        self._reader.start()

    def __del__(self):
        if self.is_open:
            self.close()

    @property
    def is_open(self):
        with self._lock:
            return self._open

    def close(self):
        with self._lock:
            self._open = False

    def read(self, size=1):
        with self._read_cond:
            if len(self._read_buffer) < size:
                self._read_cond.wait(self._read_timeout)
            if len(self._read_buffer) >= size:
                data = self._read_buffer[:size]
                self._read_buffer = self._read_buffer[size:]
                return data
            else:
                return bytes()

    def write(self, data):
        if not self.is_open:
            return
        try:
            self._write(data)
        except:
            logger.error('Failed to connect to hub')
            logger.trace(sys.exc_info()[0])
            self.close()

    def flush(self):
        pass

    def _get(self, path):
        url = 'http://{}:{}{}'.format(self._host, self._port, os.path.join('/', path))
        logger.trace('Fetching {}'.format(url))

        if self._username and self._password:
            res = requests.get(url, auth=(self._username, self._password))
        else:
            res = requests.get(url)

        response = res.text
        logger.trace('Got {}: {}'.format(res.status_code, response))
        if res.status_code != 200:
            self.close()
            raise IOError('Could not fetch {}'.format(url))
        return response

    def _write(self, data):
        with self._lock:
            self._poll()
            self._clear()
            self._get('/3?{}=I=3'.format(binascii.hexlify(data).decode('utf-8')))


    def _clear(self):
        with self._lock:
            self._get('/1?XB=M=1')
            self._idx = 0

    def _poll(self):
        with self._lock:
            xml = self._get('/buffstatus.xml')
            bufstatus = xml.split('<BS>')[1].split('</BS>')[0].strip()
            # Convert the buffer status to a buffer and an index
            buf = binascii.unhexlify(bufstatus[:-2])
            index = int(bufstatus[-2:], base=16)

            if self._idx < 0:
                self._idx = index
                return

            msg = bytes()
            if index < self._idx:
                after = buf[self._idx:]
                before = buf[:index]
                msg = after + before
            else:
                msg = buf[self._idx:index]
            self._idx = index
            with self._read_cond:
                self._read_buffer.extend(msg)
                self._read_cond.notifyAll()

    def _read_thread(self):
        while self.is_open:
            try:
                self._poll()
                time.sleep(self._poll_time)
            except:
                self.close()
                logger.error('Failed to connect to hub')
                logger.trace(sys.exc_info()[0])
        logger.trace('Exiting read thread')
