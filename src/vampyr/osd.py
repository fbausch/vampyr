"""
Module to deal with Ceph OSDs. The OSDs must use BlueStore with a RocksDB
KV store in BlueFS.
"""

import os
import tempfile
import shutil
from vampyr.bluefs import BlueFS
from vampyr.datatypes import CephInteger, CephFixedString,\
    CephBlockHeader, CephUUID, CephUTime, CephString, CephStringDict
from vampyr.kv import RDBKV, CephPExtent
import logging
import sys


class OSD(object):
    """
    The class that gives access to the OSD.

    Open it using the with OSD(...) as ... statement.
    Access the raw binary content using the read and seek methods.
    The OSD image will be open read-only.
    """

    def __init__(self, osdpath, startoffset=0x0, initkv=True):
        """
        osdpath: The path to an image of an OSD.
        startoffset: (int) If the OSD label is not located at the start of the
                     OSD, startoffset can be used to tell where the label is.
        initkv: (bool) If False the KV store will not be opened and analyzed.
        """
        self.offset = startoffset  # Offset on image
        if osdpath is None or not os.path.isfile(osdpath):
            raise Exception('OSD not found')
        else:
            self.osdpath = osdpath

        logging.info("Open OSD in read-binary mode")
        self.rb = open(self.osdpath, 'rb')

        self._bluestorelabel = None
        self.extract_dir = None

        if initkv:
            self.extract_dir = tempfile.mkdtemp()
            logging.info("Find BlueFS data")
            self.bluefs = BlueFS(self)
            logging.info("Extracting BlueFS data to %s" % self.extract_dir)
            self.bluefs.extract_state(self.extract_dir)
            kvdir = os.path.join(self.extract_dir, "db")
            logging.info("Loading KV store")
            self.kv = RDBKV(kvdir)

    def __enter__(self):
        """
        Method needed for with ... as ...
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Close the OSD image and remove temp dir.
        Needed for with ... as ...
        """
        if self.extract_dir and os.path.isdir(self.extract_dir):
            try:
                logging.debug("Removing temporary KV directory")
                shutil.rmtree(self.extract_dir)
            except Exception:
                pass
            logging.info("Closing OSD")
            self.rb.close()
            self.rb = None

    def seek(self, offset):
        """
        Go to this offset of the OSD. This is relative to the
        offset set in the constructor.
        For example: If the constructor got offset 100 and offset
        is 50, it will seek to position 50 relative to the offset 100.
        This means it will seek to position 150 in the image file.

        offset: (int) The offset.
        """
        return self.rb.seek(offset + self.offset)

    def tell(self):
        """
        Get the position relative to the offset from the constructor.

        returns: (int) The current offset.
        """
        return self.rb.tell() - self.offset

    def read(self, length):
        """
        The the next bytes from the current position.

        length: (int) The number of bytes to be read and returned.
        """
        return self.rb.read(length)

    @property
    def bluestorelabel(self):
        if not self._bluestorelabel:
            self._read_bluestore_label()
        return self._bluestorelabel

    def _read_bluestore_label(self):
        """
        Read the bluestore label and save it to self._bluestorelabel.
        """
        # The label has to be at offset 0
        self.seek(0)

        h = {}
        bytesize = os.path.getsize(self.osdpath)

        h['start'] = self.tell()
        h['label'] = CephFixedString(self, 60)
        logging.debug(h['label'].value)
        assert(h['label'].value[0:23] == "bluestore block device\n")
        assert(h['label'].value[59] == "\n")
        h['header'] = CephBlockHeader(self)
        h['uuid'] = CephUUID(self)
        assert(str(h['uuid']) == h['label'].value[23:59])
        h['osdlength'] = CephInteger(self, 8)
        logging.debug("osdlength: 0x%x" % h['osdlength'].value)
        logging.debug("bytesize: 0x%x" % bytesize)
        logging.debug("osd.offset: 0x%x" % self.offset)
        assert(h['osdlength'].value <= bytesize - self.offset)
        if h['osdlength'].value < bytesize - self.offset:
            h['volume_slack'] = self.offset + h['osdlength'].value
        h['fstime'] = CephUTime(self)
        h['main'] = CephString(self)
        pos = self.tell()
        try:
            h['meta'] = CephStringDict(self)
        except UnicodeDecodeError:
            self.seek(pos)
        assert(self.tell() == h['header'].end_offset)
        h['crc'] = CephInteger(self, 4)
        h['end'] = self.tell()

        slack_length = 0x1000 - h['end']  # BlueFS superblock starts at 0x1000
        h['label_slack'] = self.read(slack_length)

        self._bluestorelabel = h

    def bslabel_pretty_print(self):
        """
        Prints the content of the BlueStore label to stdout in a fancy manner.
        """
        data = self.bluestorelabel
        print("---------------------------------")
        print("BlueStore Superblock Information:")
        print("---------------------------------")
        print("Start at: %s" % hex(data['start']))
        print("End at:   %s" % hex(data['end']))
        print("---------------------------------")
        print(data['label'].value)
        print("OSD UUID: %s" % str(data['uuid']))
        print("OSD length: 0x%x B = ~ %d GiB" %
              (data['osdlength'].value,
               data['osdlength'].value / (1024**3)))
        print("Last used at: %s" % str(data['fstime']))
        # main
        print("Metadata information:")
        if 'meta' in data:
            for m, v in sorted(data['meta'].elements.items(),
                               key=lambda x: x[0].value):
                print("- %s: %s" % (m, v))
        print("CRC32 checksum: 0x%08x" % data['crc'].value)
        print("---------------------------------")
        if "volume_slack" in self.bluestorelabel:
            print("Volume slack starts at offset 0x%x of image file" %
                  self.bluestorelabel['volume_slack'])
            print("---------------------------------")
        print("")
        sys.stdout.flush()

    def extract_label_slack(self, edir):
        """
        Extract the slack between BlueStore label und BlueFS superblock
        to a dirctory. The name of the file will be "slack_bslabel".

        edir: (string) The directory to extract to. It must exist.
        """
        slackfile = os.path.join(edir, "slack_bslabel")
        with open(slackfile, 'wb') as s:
            s.write(self.bluestorelabel['label_slack'])

    def pextents_pretty_print(self):
        """
        Print all allocated and unallocated areas of the OSD to stdout
        in a fancy manner.
        """
        self.read_bluestore_label()
        osdlength = self.bluestorelabel['osdlength'].value
        CephPExtent.pretty_print(osdlength)

    def pextents_extract_unallocated(self, extractdir):
        """
        Extract the unallocated areas of the OSD to a directory.

        extractdir: (string) The name of the directory. It must exist.
        """
        self.read_bluestore_label()
        osdlength = self.bluestorelabel['osdlength'].value
        CephPExtent.extract_unallocated(osdlength, self, extractdir)
