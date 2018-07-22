import os
import tempfile
import shutil
from vampyr.cephbluefs import BlueFS
from vampyr.cephdatatypes import CephInteger,\
    CephFixedString, CephBlockHeader, CephUUID, CephUTime, CephString,\
    CephUnknown, CephStringDict
from vampyr.kv import RDBKV, CephPExtent
import logging


class OSD(object):
    def __init__(self, osdpath, startoffset=0x0, initkv=True):
        self.offset = startoffset  # Offset on image
        if osdpath is None or not os.path.isfile(osdpath):
            raise Exception('OSD not found')
        else:
            self.osdpath = osdpath

        logging.info("Open OSD in read-binary mode")
        self.rb = open(self.osdpath, 'rb')

        self.bluestorelabel = None
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
        return self

    def __exit__(self, exc_type, exc_value, traceback):
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
        return self.rb.seek(offset + self.offset)

    def tell(self):
        return self.rb.tell() - self.offset

    def read(self, length):
        return self.rb.read(length)

    def read_bluestore_label(self, seek=0):
        if self.bluestorelabel:
            return
        h = {}
        bytesize = os.path.getsize(self.osdpath)

        self.seek(seek)

        h['start'] = self.tell()
        h['label'] = CephFixedString(self, 60)
        logging.debug(h['label'].value)
        assert(h['label'].value[0:23] == "bluestore block device\n")
        assert(h['label'].value[59] == "\n")
        h['header'] = CephBlockHeader(self)
        h['uuid'] = CephUUID(self)
        h['osdlength'] = CephInteger(self, 8)
        logging.debug("osdlength: 0x%x" % h['osdlength'].value)
        logging.debug("bytesize: 0x%x" % bytesize)
        logging.debug("osd.offset: 0x%x" % self.offset)
        assert(h['osdlength'].value <= bytesize - self.offset)
        self.volume_slack_start_offset = None
        if h['osdlength'].value < bytesize - self.offset:
            self.volume_slack_start_offset = self.offset + h['osdlength'].value
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
        self.label_slack = self.read(slack_length)

        self.bluestorelabel = h

    def bslabel_pretty_print(self):
        if not self.bluestorelabel:
            self.read_bluestore_label()
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
        print("---------------------------------")
        if self.volume_slack_start_offset:
            print("Volume slack starts at offset 0x%x of image file" %
                  self.volume_slack_start_offset)
            print("---------------------------------")
        print("")

    def extract_label_slack(self, edir):
        if not self.bluestorelabel:
            self.read_bluestore_label()
        slackfile = os.path.join(edir, "slack_bslabel")
        with open(slackfile, 'wb') as s:
            s.write(self.label_slack)

    def pextents_pretty_print(self):
        self.read_bluestore_label()
        osdlength = self.bluestorelabel['osdlength'].value
        CephPExtent.pretty_print(osdlength)

    def pextents_extract_unallocated(self, extractdir):
        self.read_bluestore_label()
        osdlength = self.bluestorelabel['osdlength'].value
        CephPExtent.extract_unallocated(osdlength, self, extractdir)
