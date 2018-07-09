#!/usr/bin/python

import datetime
import ctypes
from vampyr.cephexceptions import CephUnexpectedMagicException


class CephDataType(object):
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __len__(self):
        return self.end - self.start


class CephDict(CephDataType):
    def __init__(self, handle, cls1, len1, cls2, len2):
        start = handle.tell()
        self.elements = {}
        self.num_elements = CephInteger(handle, length=4).value
        for i in range(0, self.num_elements):
            if len1:
                key = cls1(handle, length=len1)
            else:
                key = cls1(handle)
            if len2:
                value = cls2(handle, length=len2)
            else:
                value = cls2(handle)
            self.elements[key] = value
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return "Number of elements: %d (0x%x)" \
               % (self.num_elements, self.num_elements)


class CephList(CephDataType, list):
    def __init__(self, handle, cls1, len1):
        start = handle.tell()
        self.num_elements = CephInteger(handle, 4).value
        self.elements = []
        self._elements_index = 0
        for i in range(0, self.num_elements):
            if len1:
                value = cls1(handle, length=len1)
            else:
                value = cls1(handle)
            self.elements.append(value)
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return "Number of elements: %d (0x%x)" % \
               (self.num_elements, self.num_elements)

    def __iter__(self):
        return self

    def __next__(self):
        self._elements_index += 1
        try:
            return self.elements[self._elements_index - 1]
        except IndexError:
            self._elements_index = 0
            raise StopIteration


class CephIntegerList(CephList):
    def __init__(self, handle, length):
        super().__init__(handle, CephInteger, length)


class CephIntegerPairList(CephList):
    def __init__(self, handle, length):
        super().__init__(handle, CephIntegerPair, length)


class CephPair(CephDataType):
    def __init__(self, handle, cls1, len1, cls2, len2):
        start = handle.tell()
        if len1:
            value = cls1(handle, length=len1)
        else:
            value = cls1(handle)
        self.one = value
        if len2:
            value = cls2(handle, length=len2)
        else:
            value = cls2(handle)
        self.two = value
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return "One: %s, Two: %s" % (self.one, self.two)


class CephIntegerPair(CephPair):
    def __init__(self, handle, length):
        super().__init__(handle, CephInteger, length, CephInteger, length)


class CephString(CephDataType):
    def __init__(self, handle, len_length=4):
        start = handle.tell()
        self.len_length = len_length
        str_length = handle.read(self.len_length)
        self.length = int.from_bytes(str_length, byteorder='little')
        self.value = handle.read(self.length).decode("utf-8")
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return self.value


class CephFixedString(CephDataType):
    def __init__(self, handle, length):
        start = handle.tell()
        self.length = length
        self.value = handle.read(self.length).decode("utf-8")
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return self.value.replace('\n', " \\n ")


class CephUTime(CephDataType):
    def __init__(self, handle):
        start = handle.tell()
        self.timestamp = CephInteger(handle, 4).value
        self.nanosec = CephInteger(handle, 4).value
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        thisdate = datetime.datetime.fromtimestamp(
            self.timestamp).strftime('%Y-%m-%d %H:%M:%S')
        return "%s.%09d" % (thisdate, self.nanosec)


class CephInteger(CephDataType):
    def __init__(self, handle, length, byteorder='little'):
        start = handle.tell()
        self.value = int.from_bytes(handle.read(length), byteorder=byteorder)
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return "%d (0x%x)" % (self.value, self.value)

    def __eq__(self, other):
        return self.value == other.value

    def __lt__(self, other):
        return self.value < other.value

    def __gt__(self, other):
        return self.value > other.value

    def __hash__(self):
        return hash((self.value, self.start, self.end))


class CephBlockHeader(CephDataType):
    def __init__(self, handle):
        start = handle.tell()
        self.v = CephInteger(handle, 1).value
        self.c = CephInteger(handle, 1).value
        if self.v == 0 and self.c == 0:
            raise CephUnexpectedMagicException("Unexpected Magic at 0x%x." %
                                               start)
        self.blength = CephInteger(handle, 4).value
        end = handle.tell()
        super().__init__(start, end)
        self.end_offset = end + self.blength

    def __str__(self):
        return "%d-%d-0x%x: Offset of end: %d (0x%x)" % \
               (self.v, self.c, self.blength, self.end_offset, self.end_offset)


class CephVarInteger(CephDataType):
    def __init__(self, handle):
        start = handle.tell()
        highbit = 1
        shift = 0
        self.value = 0
        self.length = 0
        while highbit == 1:
            v = CephInteger(handle, 1).value
            byte = v & 0x7f
            byte = byte << shift
            shift = shift + 7
            highbit = v >> 7
            self.value += byte
            self.length += 1
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return "%d (0x%x), byte_length: %d" % \
               (self.value, self.value, self.length)


class CephVarIntegerLowz(CephVarInteger):
    def __init__(self, handle):
        super().__init__(handle)
        lowznib = self.value & 3
        self.value = self.value >> 2
        self.value = self.value << (lowznib * 4)


class CephLBA(CephDataType):
    def __init__(self, handle):
        start = handle.tell()
        shift = 0
        # v = 0
        self.value = 0
        self.length = 4
        word = CephInteger(handle, 4).value
        low_zero = word & 7
        if low_zero in [0, 2, 4, 6]:
            v = (word & 0x7ffffffe) << (12 - 1)
            shift = 12 + 30
        elif low_zero in [1, 5]:
            v = (word & 0x7ffffffc) << (16 - 2)
            shift = 16 + 29
        elif low_zero == 3:
            v = (word & 0x7ffffff8) << (20 - 3)
            shift = 20 + 28
        elif low_zero == 7:
            v = (word & 0x7ffffff8) >> 3
            shift = 28
        byte = word >> 24
        while byte & 0x80 == 0x80:
            byte = CephInteger(handle, 1).value
            byte = byte & 0x7f
            byte = byte << shift
            v = v | byte
            shift = shift + 7
            self.length += 1
        self.value = v
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return "%d (0x%x), byte_length: %d" % \
               (self.value, self.value, self.length)


class CephFloat(CephDataType):
    def __init__(self, handle, length=32):
        start = handle.tell()
        self.value = ctypes.c_double.from_buffer_copy(handle.read(length))
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return "%10.10f" % self.value.value


class CephUUID(CephDataType):
    def __init__(self, handle):
        start = handle.tell()
        self.value = handle.read(0x10)
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        s = "".join("{:02x}".format(c) for c in self.value)
        s = "%s-%s-%s-%s-%s" % (s[:8], s[8:12], s[12:16], s[16:20], s[20:])
        return s

    def __eq__(self, other):
        if len(self.value) != len(other.value):
            return False
        for i in range(0, len(self.value)):
            if self.value[i] != other.value[i]:
                return False
        return True


class CephBufferlist(CephDataType):
    def __init__(self, handle, len_length=4):
        start = handle.tell()
        self.len_length = len_length
        self.length = CephInteger(handle, self.len_length).value
        self.value = handle.read(self.length)
        self.raw = ByteHandler(self.value)
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return "Bufferlist of length %d (0x%x)" % (self.length, self.length)

    def print_value(self):
        return "".join("{:02x}".format(c) for c in self.value)


class CephUnknown(CephDataType):
    def __init__(self, handle, length):
        start = handle.tell()
        self.value = handle.read(length)
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return "".join("{:02x}".format(c) for c in self.value)


class CephStringDict(CephDict):
    def __init__(self, handle):
        super().__init__(handle, CephString, None,
                         CephString, None)

    def __str__(self):
        dlist = []
        for k in self.elements.keys():
            dlist.append("%s: %s" % (k, self.elements[k]))
        return "Number of elements: %d (0x%x), Content: %s" % \
               (self.num_elements, self.num_elements, ", ".join(dlist))


class ByteHandler(object):
    def __init__(self, mybytes):
        if isinstance(mybytes, str):
            blist = []
            for i in range(0, len(mybytes), 2):
                sub = mybytes[i:i + 2]
                blist.append(int(sub, 16))
            self.mybytes = bytes(blist)
        else:
            self.mybytes = mybytes
        self._p = 0

    def __str__(self):
        return "".join("{:02x}".format(c) for c in self.mybytes)

    @property
    def p(self):
        return self._p

    @p.setter
    def p(self, value):
        if value > self.length():
            raise IndexError("try to access 0x%x when length is 0x%x." %
                             (value, self.length()))
        self._p = value

    def read(self, length):
        r = self.mybytes[self.p:self.p + length]
        self.p += length
        return r

    def seek(self, pos):
        self.p = pos

    def tell(self):
        return self.p

    def length(self):
        return len(self.mybytes)

    def end(self):
        return self.p >= self.length()

    def __len__(self):
        return self.length()
