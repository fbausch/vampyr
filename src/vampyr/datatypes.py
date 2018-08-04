"""
Defines basic data types to be used by Vampyr.
"""

import datetime
import ctypes
from vampyr.exceptions import VampyrMagicException


class CephDataType(object):
    """
    Class to inherit from when implementing a class that can decode
    data from Ceph/BlueStore/BlueFS/KV store.
    """
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __len__(self):
        return self.end - self.start


class CephDict(CephDataType):
    """
    Decodes dict data types.
    """
    def __init__(self, handle, cls1, len1, cls2, len2):
        """
        handle: File handle to read from.
        cls1: CephDataType class that decodes the keys
        len1: The number of bytes to read, None for data types that don't take
              a length.
        cls2: CephDataType class that decodes the values
        len2: The number of bytes to read, None for data types that don't take
              a length.
        """
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
    """
    Decodes list data types.
    """
    def __init__(self, handle, cls1, len1):
        """
        handle: File handle to read from.
        cls1: CephDataType class that decodes the list elements.
        len1: The number of bytes to read, None for data types that don't take
              a length.
        """
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
    """
    Decodes lists of integers.
    """
    def __init__(self, handle, length):
        """
        handle: File handle to read from.
        length: The number of bytes in the integer.
        """
        super().__init__(handle, CephInteger, length)


class CephIntegerPairList(CephList):
    """
    Decodes lists of integer pairs.
    """
    def __init__(self, handle, length):
        """
        handle: File handle to read from.
        length: The number of bytes in the integers
        """
        super().__init__(handle, CephIntegerPair, length)


class CephPair(CephDataType):
    """
    Decodes pair data types.
    """
    def __init__(self, handle, cls1, len1, cls2, len2):
        """
        handle: File handle to read from.
        cls1: CephDataType class that decodes the first element.
        len1: The number of bytes to read, None for data types that don't take
              a length.
        cls2: CephDataType class that decodes the second element.
        len2: The number of bytes to read, None for data types that don't take
              a length.
        """
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
    """
    Decodes a pairs of integeres.
    """
    def __init__(self, handle, length):
        """
        handle: File handle to read from.
        length: The number of bytes in the integers.
        """
        super().__init__(handle, CephInteger, length, CephInteger, length)


class CephString(CephDataType):
    """
    Decodes strings.
    """
    def __init__(self, handle, len_length=4):
        """
        handle: File handle to read from.
        len_length: The number of bytes of the integer that hold
                    the length of the string. Defaults to 4 and we
                    never saw another value.
        """
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
    """
    Decodes strings that are not preceded by an integer that tells
    us the number of characters in the string. So we need to know
    the length.
    """
    def __init__(self, handle, length):
        """
        handle: File handle to read from.
        length: The number of bytes/characters in the string.
        """
        start = handle.tell()
        self.length = length
        self.value = handle.read(self.length).decode("utf-8")
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return self.value.replace('\n', " \\n ")


class CephUTime(CephDataType):
    """
    Decodes utime data structures.
    It consists of the property timestamp (the UNIX timestamp)
    and the property nanosec (the nanoseconds within timestamp).
    """
    def __init__(self, handle):
        """
        handle: File handle to read from.
        """
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
    """
    Decodes an integer.
    """
    def __init__(self, handle, length, byteorder='little'):
        """
        handle: File handle to read from.
        length: The number of bytes in the integer.
        byteorder: little or big. Integers in the KV are often big, but
                   usually little (the default) is the right choice.
        """
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
    """
    Decodes block headers that tell us about the following
    data structures.
    The property v will contain the encoder version.
    The property c will contain the minimal decoder version.
    The property blength will contain the number of bytes in the
    following data structure.
    The property end_offset will contain the offset were the next
    data structure begins.
    """
    def __init__(self, handle):
        """
        handle: File handle to read from.
        """
        start = handle.tell()
        self.v = CephInteger(handle, 1).value
        self.c = CephInteger(handle, 1).value
        if self.v == 0 and self.c == 0:
            raise VampyrMagicException("Unexpected Magic at 0x%x." % start)
        self.blength = CephInteger(handle, 4).value
        end = handle.tell()
        super().__init__(start, end)
        self.end_offset = end + self.blength

    def __str__(self):
        return "%d-%d-0x%x: Offset of end: %d (0x%x)" % \
               (self.v, self.c, self.blength, self.end_offset, self.end_offset)


class CephVarInteger(CephDataType):
    """
    Decodes a varint integer.
    """
    def __init__(self, handle):
        """
        handle: File handle to read from.
        """
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
    """
    Decodes a varint with low-zero encoding.
    """
    def __init__(self, handle):
        """
        handle: File handle to read from.
        """
        super().__init__(handle)
        lowznib = self.value & 3
        self.value = self.value >> 2
        self.value = self.value << (lowznib * 4)


class CephLBA(CephDataType):
    """
    Decodes an LBA integer.
    """
    def __init__(self, handle):
        """
        handle: File handle to read from.
        """
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
    """
    Decodes a float.
    """
    def __init__(self, handle, length=32):
        """
        handle: File handle to read from.
        length: The number of bytes in the float. Should be 32 (the default).
        """
        start = handle.tell()
        self.value = ctypes.c_double.from_buffer_copy(handle.read(length))
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return "%10.10f" % self.value.value


class CephUUID(CephDataType):
    """
    Decodes a UUID.
    """
    def __init__(self, handle):
        """
        handle: File handle to read from.
        """
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
    """
    Reads a Bufferlist.
    The property length will contain the number of bytes in the bufferlist.
    The property value will contain the bytes of the bufferlist.
    The property raw will contain a ByteHandler containing the value so that
    it can be easily used as input for CephDataType decoders.
    """
    def __init__(self, handle, len_length=4):
        """
        handle: File handle to read from.
        len_length: The number of bytes in the integer that tell us the length
                    of the bufferlist. Defaults to 4 - this should always be
                    correct.
        """
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
        """
        Returns the value property as hex dump.
        """
        return "".join("{:02x}".format(c) for c in self.value)


class CephUnknown(CephDataType):
    """
    Reads a number of bytes where we don't know what their exact meaning is.
    The property value will contain the bytes read.
    """
    def __init__(self, handle, length):
        """
        handle: File handle to read from.
        length: The number of bytes to read.
        """
        start = handle.tell()
        self.value = handle.read(length)
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return "".join("{:02x}".format(c) for c in self.value)


class CephStringDict(CephDict):
    """
    Decodes a dict that contains strings as keys and values.
    """
    def __init__(self, handle):
        """
        handle: File handle to read from.
        """
        super().__init__(handle, CephString, None,
                         CephString, None)

    def __str__(self):
        dlist = []
        for k in self.elements.keys():
            dlist.append("%s: %s" % (k, self.elements[k]))
        return "Number of elements: %d (0x%x), Content: %s" % \
               (self.num_elements, self.num_elements, ", ".join(dlist))


class ByteHandler(object):
    """
    Wraps byte arrays so that they can be consumed like file handles.
    Because of this a ByteHandler object can be used as input for CephDataType
    classes.
    It provides read(), tell(), and seek().
    """
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
        string = "".join("{:02x}".format(c) for c in self.mybytes)
        return string[:2 * self.p] + "<___POS___>" + string[2 * self.p:]

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
        """
        Reads and returns the next *length* bytes of the byte array.
        Will throw an exception if we try to read over the end of
        the array.
        """
        r = self.mybytes[self.p:self.p + length]
        self.p += length
        return r

    def seek(self, pos):
        """
        Goes to position *pos* in the byte array. Will throw an
        exception if the position is behind the end of the array.
        """
        self.p = pos

    def tell(self):
        """
        Returns the current position in the byte array.
        """
        return self.p

    def length(self):
        """
        Number of bytes in the array.
        """
        return len(self.mybytes)

    def end(self):
        """
        Returns True if we are at the end of the array.
        """
        return self.p >= self.length()

    def __len__(self):
        return self.length()
