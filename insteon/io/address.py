
class Address:
    def __init__(self, hi=0, mid=0, low=0):
        self._hi = hi;
        self._mid = mid;
        self._low = low;

    @property
    def bytes(self):
        return bytes([self._hi, self._mid, self._low])

    @property
    def array(self):
        return [self._hi, self._mid, self._low]

    @property
    def human(self):
        return '{:02x}.{:02x}.{:02x}'.format(self._hi, self._mid, self._low).upper()

    @property
    def packed(self):
        return [self._hi, self._mid, self._low]

    @staticmethod
    def unpack(packed):
        return Address(packed[0], packed[1], packed[2])

    def __str__(self):
        return self.human

    def __repr__(self):
        return self.human

    def __eq__(self, other):
        return self._hi == other._hi and self._mid == other._mid and self._low == other._low

    def __nq__(self, other):
        return self._hi != other._hi or self._mid != other._mid or self._low != other._low

    def __hash__(self):
        return 13 * self._hi + 57 * self._mid + 29*self._low
