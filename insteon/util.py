from time import time as _time
import asyncio
import threading
import inspect

# CRC stuff

def calc_simple_crc(data):
    return (~(sum(data)) + 1) & 0xFF

def calc_long_crc(data):
    crc = int(0);
    for loop in xrange(0, len(bytes)):
            b = bytes[loop] & 0xFF
            for bit in xrange(0, 8):
                    fb = b & 0x01
                    fb = fb ^ 0x01 if (crc & 0x8000) else fb
                    fb = fb ^ 0x01 if (crc & 0x4000) else fb
                    fb = fb ^ 0x01 if (crc & 0x1000) else fb
                    fb = fb ^ 0x01 if (crc & 0x0008) else fb
                    crc = ((crc << 1) | fb) & 0xFFFF;
                    b = b >> 1
    return crc

# A custom insteon error type
# that the terminal knows can just be printed out
# without a stack trace
# this should be used for when messages are dropped
# (i.e any unexpected behavior on the insteon network
# or with the modem)
class InsteonError(Exception):
    def __init__(self, *args):
        super().__init__(*args)
        self.quiet = True
