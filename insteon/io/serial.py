import logbook
logger = logbook.Logger(__name__)

import traceback

from ..util import InsteonError


class SerialConn:
    FIVEBITS, SIXBITS, SEVENBITS, EIGHTBITS = 5, 6, 7, 8
    STOPBITS_ONE, STOPBITS_ONE_POINT_FIVE, STOPBITS_TWO = 1, 1.5, 2
    PARITY_NONE, PARITY_EVEN, PARITY_ODD, PARITY_MARK, PARITY_SPACE = 'N', 'E', 'O', 'M', 'S'

    def __init__(self, port, baudrate=19200, bytesize=EIGHTBITS, parity=PARITY_NONE,
                    stopbits=STOPBITS_ONE, timeout=0.1,
                    xonxoff=False, rtscts=False, write_timeout=None, dsrdtr=False,
                    inter_byte_timeout=None):
        self._name = port
        self._port = None
        try:
            import aioserial
            import sys

            ports = {}
            if 'insteonterminal' in sys.modules:
                if not hasattr(sys.modules['insteonterminal'], 'ports__'):
                    sys.modules['insteonterminal'].ports__ = ports
                else:
                    ports = sys.modules['insteonterminal'].ports__

            if port in ports:
                self._port = ports[port]._port
            else:
                self._port = aioserial.AioSerial(port=port, baudrate=baudrate,
                                            bytesize=bytesize, parity=parity,
                                            stopbits=stopbits, timeout=timeout,
                                            xonxoff=xonxoff, rtscts=rtscts,
                                            write_timeout=write_timeout, dsrdtr=dsrdtr,
                                            inter_byte_timeout=inter_byte_timeout)
            ports[port] = self
        except Exception as e:
            raise InsteonError('Could not open serial port {}, bd: {}, bs: {}, parity: {}, stopbits: {}'.format(
                                port, baudrate, bytesize, parity, stopbits))

    
    @property
    def is_open(self):
        if not self._port:
            return False
        return self._port.is_open

    def close(self):
        print('closing')
        traceback.print_stack()
        try:
            if not self.is_open:
                return
            self._port.close()
        except:
            self._port = None

    async def read(self, size=1):
        try:
            if not self.is_open:
                return 
            return await self._port.read_async(size)
        except Exception as e:
            raise EOFError()
            #self.close()
            #raise InsteonError('Error reading from serial port {}'.format(self._name))

    async def write(self, data):
        try:
            if not self.is_open:
                return
            return await self._port.write_async(data)
        except AssertionError:
            raise EOFError()
        except Exception:
            raise InsteonError('Error writing to serial port {}'.format(self._name))

    async def flush(self):
        return
        try:
            if not self.is_open:
                return
            self._port.flush()
        except Exception as e:
            raise InsteonError('Error writing to port {}'.format(port))
