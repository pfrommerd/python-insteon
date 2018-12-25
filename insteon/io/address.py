
class Address:
    def __init__(self, hi=0, mid=0, low=0):
        self._hi = hi;
        self._mid = mid;
        self._low = low;

    @property
    def bytes(self):
        return bytes([self._hi, self._mid, self._low])

    @property
    def human(self):
        return hex(self._hi) + '.' + hex(self._mid) + '.' + hex(self._low);

    def __str__(self):
        return self.human

    def __repr__(self):
        return self.human
