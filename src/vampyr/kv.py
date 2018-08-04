"""
Module to deal with RocksDB KV stores.
"""

import subprocess
import os
from vampyr.datatypes import CephDataType, CephFixedString, CephInteger,\
    CephUnknown, CephString, CephUTime, CephIntegerPairList, CephDict,\
    ByteHandler, CephBlockHeader, CephVarInteger, CephBufferlist, CephList,\
    CephIntegerList, CephVarIntegerLowz, CephLBA
from vampyr.decoder import CephPG,\
    decode_osdmap, decode_inc_osdmap, decode_osd_super, decode_rbd_id
from vampyr.exceptions import VampyrMagicException
import logging
import re
import hashlib
import sys
# import functools
# print = functools.partial(print, flush=True)


class RDBKV(object):
    """
    Class to load a KV store in a RocksDB. It needs to have either
    ldb in the PATH or RDBKV.ldb has to be set to the ldb tool.

    For every KV store prefix (e.g. O, M) there is a property
    that holds all the KV sets with this prefix. For example pO, pM.
    """
    ldb = None

    def __init__(self, workingdir):
        """
        workingdir: The path to a RocksDB database.
        """
        self.wdir = workingdir
        self.pools = {}
        self._datasets = None
        self._pO = None
        self._pS = None
        self._pT = None
        self._pC = None
        self._pM = None
        self._pP = None
        self._pB = None
        self._pb = None
        self._pL = None
        self._pX = None

    def _load(self, phandler):
        """
        Load all datasets that are handled by a certain
        prefix handler.
        phandler: The prefix handler.
        """
        logging.info("Loading datasets with prefix %s." % phandler.prefix)
        datasets = self.datasets[phandler.prefix]
        for d in sorted(datasets.keys()):
            k = ByteHandler(d)
            v = ByteHandler(datasets[d][0])
            phandler.parse_dataset(k, v)

    @property
    def datasets(self):
        """
        Unparsed datasets loaded using the ldb tool, sorted by prefix.
        """
        if self._datasets:
            return self._datasets
        self._datasets = {'O': {}, 'S': {}, 'T': {}, 'C': {}, 'M': {},
                          'P': {}, 'B': {}, 'b': {}, 'L': {}, 'X': {}}

        # command = ['sst_dump', '--file=%s' % self.wdir,
        #            '--command=scan', '--output_hex']

        if RDBKV.ldb is None:
            RDBKV.ldb = 'ldb'
        else:
            RDBKV.ldb = os.path.join(".", RDBKV.ldb)
        command = [RDBKV.ldb, 'idump', '--db=%s' % self.wdir,
                   '--hex']

        logging.info("Running command: %s" % " ".join(command))
        proc = subprocess.Popen(command, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

        o, e = proc.communicate()
        if proc.returncode != 0:
            logging.error("Could not get content of KV store.")
            logging.error("Returncode was %d." % proc.returncode)
            return self._datasets
        regex = "^'([0-9A-F]+)' seq:([0-9]+), type:([0-9]+) => ([0-9A-F]*)$"
        regex = re.compile(regex)
        for line in o.decode('utf-8').split('\n'):
            m = re.search(regex, line)
            if not m:
                logging.debug("not matching: %s" % line)
                continue
            k = m.group(1)
            assert(k[2:4] == "00")
            seq = int(m.group(2))
            t = int(m.group(3))
            v = m.group(4)
            prefix = chr(int(k[0:2], 16))
            if k in self._datasets[prefix] and\
               self._datasets[prefix][k][1] > seq:
                continue
            else:
                self._datasets[prefix][k] = (v, seq, t)
        return self._datasets

    @property
    def pO(self):
        """
        PrefixHandler that holds all O-rows.
        """
        if not self._pO:
            self._pO = PrefixHandlerO()
            self._load(self._pO)
        return self._pO

    @property
    def pS(self):
        """
        PrefixHandler that holds all S-rows.
        """
        if not self._pS:
            self._pS = PrefixHandlerS()
            self._load(self._pS)
        return self._pS

    @property
    def pT(self):
        """
        PrefixHandler that holds all T-rows.
        """
        if not self._pT:
            self._pT = PrefixHandlerT()
            self._load(self._pT)
        return self._pT

    @property
    def pC(self):
        """
        PrefixHandler that holds all C-rows.
        """
        if not self._pC:
            self._pC = PrefixHandlerC()
            self._load(self._pC)
        return self._pC

    @property
    def pM(self):
        """
        PrefixHandler that holds all M-rows.
        """
        if not self._pM:
            self._pM = PrefixHandlerMP(prefix='M')
            self._load(self._pM)
        return self._pM

    @property
    def pP(self):
        """
        PrefixHandler that holds all P-rows.
        """
        if not self._pP:
            self._pP = PrefixHandlerMP(prefix='P')
            self._load(self._pP)
        return self._pP

    @property
    def pB(self):
        """
        PrefixHandler that holds all B-rows.
        """
        if not self._pB:
            self._pB = PrefixHandlerB()
            self._load(self._pB)
        return self._pB

    @property
    def pb(self):
        """
        PrefixHandler that holds all b-rows.
        """
        if not self._pb:
            self._pb = PrefixHandlerb()
            self._load(self._pb)
        return self._pb

    @property
    def pL(self):
        """
        PrefixHandler that holds all L-rows.
        """
        if not self._pL:
            self._pL = PrefixHandlerL()
            self._load(self._pL)
        return self._pL

    @property
    def pX(self):
        """
        PrefixHandler that holds all X-rows.
        """
        if not self._pX:
            self._pX = PrefixHandlerX()
            self._load(self._pX)
        return self._pX


class GenericPrefixHandler(object):
    """
    Abstract class for prefix handlers.
    """

    def parse_dataset(self, k, v):
        """
        Parse a dataset.
        k: The key
        v: The value
        """
        raise NotImplementedError()


class PrefixHandlerO(GenericPrefixHandler):
    """
    The PrefixHandler to load O-rows.
    """

    counter = 0

    def __init__(self):
        self.onode_map = {}
        self.poolids = []
        self.oid_map = {}
        self.prefix = 'O'

    def parse_dataset(self, k, v):
        """
        Parses a dataset
        """
        k.seek(2)
        thiskey = KVObjectNameKey(k)
        assert(k.end() or k.tell() + 5 == k.length())
        PrefixHandlerO.counter += 1
        if PrefixHandlerO.counter % 10000 == 0:
            logging.info("O: reading %dth KV" % PrefixHandlerO.counter)
        logging.debug("thiskey: %s" % str(thiskey))

        if thiskey.poolid not in self.poolids:
            self.poolids.append(thiskey.poolid)

        if len(v) == 0:
            self.onode_map[thiskey] = None
            logging.debug("Add empty onode to map")
            return

        if k.end():
            # onode, followed by extent map
            onode = KVONode(v)
            self.onode_map[thiskey] = onode
            self.oid_map[onode.oid] = (thiskey, onode)
            logging.debug("onode: %s" % str(onode))

            KVExtentMap(v, onode)

        else:
            # just extent map
            onode = self.onode_map[thiskey]

            if not onode:
                return

            offset = CephInteger(k, 4, byteorder='big').value
            tail = k.read(1)
            assert(tail == b'x')

            for s in onode.extent_map_shards:
                if s.offset == offset and s.bytes == len(v):
                    assert(not s.used)
                    s.used = True
                    break

            KVExtentMap(v, onode, noheader=True)

    def pretty_print(self, fltr):
        """
        Prints object names to stdout.
        fltr: A regex that filters object names. Only matching objects are
              printed.
        """
        print("-----------------------")
        print("Object List:")
        print("-----------------------")
        print("Prefix        -> Object")
        print("-----------------------")
        if not fltr:
            fltr = ".*"
        logging.info("Using filter: %s" % fltr)
        fltr = re.compile(fltr)
        m = {}
        for key in sorted(self.onode_map.keys(), key=lambda x: x.key):
            oid = key.oid
            if not re.match(fltr, oid):
                continue
            stripe = key.stripe
            stripe_sort = key.stripe_sort
            if not stripe:
                m[oid] = []
            else:
                if oid not in m:
                    m[oid] = []
                m[oid].append((stripe, stripe_sort))
        for oid in sorted(m.keys()):
            stripes = sorted(m[oid], key=lambda x: x[1])
            stripes = ", ".join([s[0] for s in stripes])
            print("%s -> %s" % (oid, stripes))
        print("-----------------------")
        print("")
        sys.stdout.flush()

    def print_decoded(self, read, fltr):
        """
        Prints decoded objects to stdout. For example osdmap objects.
        read: The OSD/open file to read the object data from.
        fltr: A regex that filters object names. Only matching objects are
              handled.
        """
        if not fltr:
            fltr = ".*"
        logging.info("Using filter: %s" % fltr)
        fltr = re.compile(fltr)
        for key in sorted(self.onode_map.keys(), key=lambda x: x.key):
            oid = key.oid
            if not re.match(fltr, oid):
                continue
            onode = self.onode_map[key]
            stripe = key.stripe
            if stripe is None:
                stripe = ""
            else:
                stripe = ".%s" % stripe

            try:
                if onode and oid == "osdmap":
                    decoded = decode_osdmap(onode, read)
                elif onode and oid == "inc_osdmap":
                    decoded = decode_inc_osdmap(onode, read)
                elif onode and oid == "osd_superblock":
                    decoded = decode_osd_super(onode, read)
                elif onode and oid == "rbd_id":
                    decoded = decode_rbd_id(onode, read)
                else:
                    decoded = None
            except VampyrMagicException:
                sys.stdout.flush()
                logging.warn("Error decoding: %s%s" % (key.oid, stripe))
                logging.warn("Object key: %s" % str(key))
                logging.warn("Object onode: %s" % str(onode))
                decoded = None
                print("-----------------\n")
            if decoded:
                print("Decoded: %s%s" % (key.oid, stripe))
                print("Object key: %s" % str(key))
                print("Object onode: %s" % str(onode))
                print(decoded[0])
                print("-----------------\n")
                sys.stdout.flush()

    def decode_object_data(self, read, edir, pMP, fltr):
        """
        Decodes object data to a directory. It will try to extract as many
        information as possible.
        read: The OSD/open file.
        edir: The directory to extract to. It must exist.
        pMP: A PrefixHandler for M-rows or P-rows. The matching M-rows or
             P-rows are looked up using the OID.
        fltr: A regex that filters object names. Only matching objects are
              handled.
        """
        if not fltr:
            fltr = ".*"
        logging.info("Using filter: %s" % fltr)
        fltr = re.compile(fltr)
        for key in sorted(self.onode_map.keys(), key=lambda x: x.key):
            oid = key.oid
            if not re.match(fltr, oid):
                continue
            logging.debug("Handling %s" % key)
            onode = self.onode_map[key]
            stripe = key.stripe
            if stripe is None:
                stripe = ""

            oedir = os.path.join(edir, oid)
            if not os.path.isdir(oedir):
                os.makedirs(oedir)

            fstripe = os.path.join(oedir, "object_%s" % stripe)
            fmd5 = os.path.join(oedir, "md5_object_%s" % stripe)
            fmeta = os.path.join(oedir, "vampyrmeta_%s" % stripe)
            fslack = os.path.join(oedir, "slack_%s" % stripe)
            fdec = os.path.join(oedir, "decoded_%s" % stripe)
            fdata = os.path.join(oedir, "data_%s" % stripe)
            fcrush = os.path.join(oedir, "crush_%s" % stripe)

            if onode and os.path.exists(fstripe) and \
               "_" in onode.attrs and \
               os.path.getmtime(fstripe) >= onode.attrs["_"].mtime.timestamp:
                logging.info("Skipping object")
                continue

            try:
                if onode and oid == "osdmap":
                    decoded, raw = decode_osdmap(onode, read)
                    crush = raw['crush_raw'].value
                    if len(crush) > 0:
                        with open(fcrush, 'wb') as f:
                            f.write(crush)
                elif onode and oid == "inc_osdmap":
                    decoded, raw = decode_inc_osdmap(onode, read)
                    crush = raw['crush_raw'].value
                    if len(crush) > 0:
                        with open(fcrush, 'wb') as f:
                            f.write(crush)
                elif onode and oid == "osd_superblock":
                    decoded, raw = decode_osd_super(onode, read)
                elif onode and oid == "rbd_id":
                    decoded, raw = decode_rbd_id(onode, read)
                    dec_rbd_id = raw['rbd_id'].value
                    destrel = os.path.join('..', "rbd_data.%s" % dec_rbd_id)
                    destabs = os.path.join(edir, "rbd_data.%s" % dec_rbd_id)
                    if not os.path.isdir(destabs):
                        os.makedirs(destabs)
                    os.symlink(destrel, fdata)
                    rbd_id = os.path.join(edir,
                                          "rbd_data.%s" % dec_rbd_id,
                                          "rbd_id_%s" % stripe)
                    with open(rbd_id, 'w') as f:
                        pass
                else:
                    decoded = None
                    logging.debug("Not decoding: %s" % oid)
            except VampyrMagicException:
                decoded = 'Error while decoding.\n'

            if decoded is not None:
                with open(fdec, 'w') as f:
                    f.write(decoded)

            if onode:
                with open(fstripe, 'wb') as w, open(fslack, 'wb') as s,\
                        open(fmd5, 'w') as m:
                    onode.extract(read, w, s, m)
                if "_" in onode.attrs:
                    ts = onode.attrs["_"].mtime.timestamp
                    os.utime(fstripe, (ts, ts))
                onode.create_tree(edir)
            with open(fmeta, 'w') as w:
                w.write("Key: %s\n" % str(key))
                w.write("Value:\n")
                if onode:
                    w.write("%s\n" % str(onode))
                    for a, v in onode.attrs.items():
                        w.write("%10s: %s\n" % (a, v))
                    if "_parent" in onode.attrs:
                        filename = onode.attrs["_parent"].filename()
                        fullpath = onode.attrs["_parent"].fullpath()
                        inode = hex(onode.attrs["_parent"].inode)
                        inodes = [hex(e.inode) for e in
                                  onode.attrs["_parent"].ancestors]
                        inodes = "/".join(reversed(inodes))
                        w.write("Filename: %s\n" % filename)
                        w.write("Fullpath: %s\n" % fullpath)
                        w.write("Own inode: %s\n" % inode)
                        w.write("Inodes in path: %s\n" % inodes)
                    if len(onode.lextents) > 0:
                        w.write("\nLogical extents:\n")
                        for le in onode.lextents:
                            w.write(le.pretty())
                            w.write('\n')
                    if onode.oid in pMP.meta_map:
                        w.write("\nAdditional Metadata from KV ")
                        w.write("Store (M prefix)\n")
                        for k, v in sorted(pMP.meta_map[onode.oid].items(),
                                           key=lambda x: x[0]):
                            if isinstance(v, tuple):
                                v = "(%s, %s)" % (str(v[0]), str(v[1]))
                            else:
                                v = str(v)
                            w.write("%s: %s\n" % (str(k), v))


class PrefixHandlerS(GenericPrefixHandler):
    """
    The PrefixHandler to load S-rows.
    """

    counter = 0

    def __init__(self):
        self.metadata_map = {}
        self.prefix = 'S'

    def parse_dataset(self, k, v):
        k.seek(2)
        PrefixHandlerS.counter += 1
        if PrefixHandlerS.counter % 1000 == 0:
            logging.info("S: reading %dth KV" % PrefixHandlerS.counter)
        length = k.length() - 2
        thiskey = CephFixedString(k, length).value
        if thiskey == "freelist_type":
            m = CephFixedString(v, v.length())
        elif thiskey == "bluefs_extents":
            m = CephIntegerPairList(v, 8)
        elif thiskey in ["blobid_max", "ondisk_format", "nid_max",
                         "min_compat_ondisk_format", "min_alloc_size"]:
            m = CephInteger(v, v.length())
        else:
            logging.warn("Unknown key %s" % k)
            m = v
        self.metadata_map[thiskey] = m

    def pretty_print(self):
        print("---------------------------")
        print("OSD Metadata from KV Store:")
        print("---------------------------")
        for m in sorted(self.metadata_map.items(), key=lambda x: x[0]):
            print("%16s: %s" % (m[0], str(m[1])))
        print("")
        sys.stdout.flush()


class PrefixHandlerT(GenericPrefixHandler):
    """
    The PrefixHandler to load T-rows.
    """

    counter = 0

    def __init__(self):
        self.statfs_map = {}
        self.prefix = 'T'

    def parse_dataset(self, k, v):
        k.seek(2)
        PrefixHandlerT.counter += 1
        if PrefixHandlerT.counter % 1000 == 0:
            logging.info("T: reading %dth KV" % PrefixHandlerT.counter)
        length = k.length() - 2
        thiskey = CephFixedString(k, length).value
        val = CephStatfs(v)
        logging.debug("T: %s - %s" % (thiskey, val))
        self.statfs_map[thiskey] = val

    def pretty_print(self):
        print("------------")
        print("Statfs Data:")
        print("------------")
        for m in sorted(self.statfs_map.items(), key=lambda x: x[0]):
            print("%16s: %s" % (m[0], str(m[1])))
        print("")
        sys.stdout.flush()


class PrefixHandlerC(GenericPrefixHandler):
    """
    The PrefixHandler to load C-rows.
    """

    counter = 0

    def __init__(self):
        self.cnode_map = {}
        self.prefix = 'C'

    def parse_dataset(self, k, v):
        k.seek(2)
        PrefixHandlerC.counter += 1
        if PrefixHandlerC.counter % 1000 == 0:
            logging.info("C: reading %dth KV" % PrefixHandlerC.counter)
        length = k.length() - 2
        thiskey = CephFixedString(k, length).value
        if thiskey != "meta":
            _k = CephPG(None)
            _k.m_pool = int(thiskey.split("_")[0].split(".")[0], 10)
            _k.m_seed = int(thiskey.split("_")[0].split(".")[1], 16)
            thiskey = _k
        if v.length() > 0:
            val = KVCNode(v)
        else:
            val = None
        self.cnode_map[thiskey] = val

    def pretty_print(self):
        print("------------")
        print("PG Metadata:")
        print("------------")
        for m in sorted(self.cnode_map.items(), key=lambda x: x[0]):
            print("%16s: %s" % (str(m[0]), str(m[1])))
        print("")
        sys.stdout.flush()


class PrefixHandlerMP(GenericPrefixHandler):
    """
    The PrefixHandler to load M-rows or P-rows.
    """

    counter = 0

    def __init__(self, prefix):
        self.meta_map = {}
        self.header_map = {}
        self.inode_map = {}
        self.prefix = prefix

    def parse_dataset(self, k, v):
        k.seek(2)
        PrefixHandlerMP.counter += 1
        if self.prefix == 'P' or PrefixHandlerMP.counter % 10000 == 0:
            logging.info("%s: reading %dth KV" %
                         (self.prefix, PrefixHandlerMP.counter))
        oid = CephInteger(k, 8, byteorder='big').value
        nextchar = k.read(1)
        assert(nextchar in [b'.', b'-'])

        if nextchar == b"-":
            # We found CephFS directory metadata
            if v.length() == 0:
                return
            val = None
            try:
                val = KVFNode(v)
            except IndexError:
                logging.warn("Skipping omap(?) with oid %d." % oid)
                return
            self.header_map[oid] = val
            logging.debug("0x%x: %s" % (oid, self.header_map[oid]))
            if oid not in self.meta_map:
                self.meta_map[oid] = {}
            self.meta_map[oid]["-"] = val
            return

        length = k.length() - k.tell()
        key = CephFixedString(k, length).value
        logging.debug("%s: %s %s" % (self.prefix, str(oid), key))
        if v.length() == 0:
            val = None
        read = v.length() != 0
        if key == "may_include_deletes_in_missing":
            val = True
        elif key.endswith("_head"):  # dentry
            if read:
                fnode = self.header_map[oid]
                v.read(8)
                ntype = v.read(1)
                if ntype == b'I':
                    val = KVINode(v)
                else:
                    raise NotImplementedError()
                fname = key[:-5]
                fnode.dentries[fname] = val
                self.inode_map[val.inode] = val
                logging.debug("0x%x: %s, %s" % (oid, val, fname))
                v.seek(v.length())
        elif key == "_info":
            if read:
                val = CephPGInfo(v)
        elif key == "_biginfo":
            if read:
                val = (CephPastIntervals(v),
                       CephDict(v, CephInteger, 8, CephInteger, 8))
        elif key in ["_infover", "order"]:
            if read:
                val = CephInteger(v, 1)
        elif key == "_epoch":
            if read:
                val = CephInteger(v, 4)
        elif (key == "object_prefix" or
              key.startswith("name_") or
              key.startswith("id_")):
            if read:
                val = CephString(v).value
        elif key == "create_timestamp":
            if read:
                val = CephUTime(v)
        elif key in ["size", "flags", "snap_seq", "features"]:
            if read:
                val = CephInteger(v, 8)
        elif key.startswith("dup_"):
            if read:
                val = CephPGLogDup(v)
        elif re.match(r"[0-9]{10}\.[0-9]{20}", key):
            if read:
                val = CephPGLogEntry(v, checksum=True)
                chk = val.version.get_key_name()
                assert(key == chk)
        elif key == "_fastinfo":
            if read:
                val = CephPGFastinfo(v)
        elif key.startswith("missing/"):
            assert(v.length() == 0)
        else:
            logging.error("Error: %x,%s" % (oid, key))
            val = v
            logging.error(val)
        if not isinstance(val, ByteHandler):
            if not v.end():
                pos = v.tell()
                readlen = v.length() - pos
                logging.error(str(CephUnknown(v, readlen)))
                v.seek(pos)
                logging.error("End not reached: %d" % v.tell())
                logging.error("Length: %d" % v.length())
                logging.error("Key: %x,%s" % (oid, key))
                logging.error(v)
            assert(v.end())
        if oid not in self.meta_map:
            self.meta_map[oid] = {}
        self.meta_map[oid][key] = val

    def decode_object_data(self, read, edir, pO, fltr):
        if not fltr:
            fltr = ".*"
        logging.info("Using filter: %s" % fltr)
        fltr = re.compile(fltr)
        metafile = os.path.join(edir, "kvmetadata_%s" % self.prefix)
        logging.debug("Metafile %s" % metafile)
        with open(metafile, 'w') as f:
            f.write("Metadata from Key Value Store:\n")
            for oid, kv in sorted(self.meta_map.items(), key=lambda x: x[0]):
                f.write("oid: %s\n" % str(oid))
                for k, v in sorted(kv.items(), key=lambda x: x[0]):
                    if isinstance(v, tuple):
                        v = "(%s, %s)" % (str(v[0]), str(v[1]))
                    else:
                        v = str(v)
                    f.write("- %s: %s\n" % (str(k), v))

        for oid, kv in sorted(self.meta_map.items(), key=lambda x: x[0]):
            if oid not in pO.oid_map:
                logging.debug("%s not found in O-keys" % oid)
                continue
            key, onode = pO.oid_map[oid]
            if not re.match(fltr, key.oid):
                continue
            if not onode:
                logging.info("%s has an empty onode" % oid)
                continue
            for k, v in sorted(kv.items(), key=lambda x: x[0]):
                if not isinstance(v, KVINode):
                    continue
                if not k.endswith("_head"):
                    continue
                k = k[:-5]
                oiddir = os.path.join(edir, "%x" % v.inode)
                if not os.path.isdir(oiddir):
                    os.makedirs(oiddir)
                parentdir = os.path.join(edir, key.oid)
                if not os.path.isdir(parentdir):
                    os.makedirs(parentdir)
                parentlnk = os.path.join(oiddir, "parent")
                if not os.path.exists(parentlnk):
                    parentdest = os.path.join('..', key.oid)
                    os.symlink(parentdest, parentlnk)
                childlnk = os.path.join(parentdir, "child_%s" % k)
                if not os.path.exists(childlnk):
                    childdest = os.path.join('..', "%x" % v.inode)
                    os.symlink(childdest, childlnk)
                fmeta = os.path.join(oiddir, 'vampyrmeta_dir')
                with open(fmeta, 'a') as f:
                    f.write("Metadata extracted from CephFS ")
                    f.write("directory metadata in KV store")
                    f.write(" (prefix %s):\n" % self.prefix)
                    f.write("%s\n" % str(v))
                fself = os.path.join(oiddir, 'self_%s' % k)
                with open(fself, 'w') as f:
                    pass


class PrefixHandlerB(GenericPrefixHandler):
    """
    The PrefixHandler to load B-rows.
    """

    counter = 0

    def __init__(self):
        self.bnode_map = {}
        self.prefix = 'B'

    def parse_dataset(self, k, v):
        k.seek(2)
        PrefixHandlerB.counter += 1
        if PrefixHandlerB.counter % 1000 == 0:
            logging.info("B: reading %dth KV" % PrefixHandlerB.counter)
        length = k.length() - 2
        thiskey = CephFixedString(k, length).value
        self.bnode_map[thiskey] = CephInteger(v, 8).value

    def pretty_print(self):
        print("----------------")
        print("Bitmap Metadata:")
        print("----------------")
        for k in sorted(self.bnode_map.keys()):
            print("%s --> 0x%x" % (k, self.bnode_map[k]))
        sys.stdout.flush()


class PrefixHandlerb(GenericPrefixHandler):
    """
    The PrefixHandler to load b-rows.
    """

    counter = 0

    def __init__(self):
        self.alloc_map = {}
        self.prefix = 'b'

    def parse_dataset(self, k, v):
        k.seek(2)
        PrefixHandlerb.counter += 1
        if PrefixHandlerb.counter % 10000 == 0:
            logging.info("b: reading %dth KV" % PrefixHandlerb.counter)
        thiskey = CephInteger(k, 8, byteorder='big').value
        self.alloc_map[thiskey] = CephInteger(v, v.length(),
                                              byteorder='big').value

    def pretty_print(self, pB):
        blocks_per_key = pB.bnode_map['blocks_per_key']
        print("--------")
        print("Bitmaps:")
        print("--------")
        fstring = "{0:{fill}%db}" % blocks_per_key
        for k in sorted(self.alloc_map.keys()):
            mask = fstring.format(self.alloc_map[k], fill="0")
            print("0x%016x --> %s" % (k, mask))
        sys.stdout.flush()

    def extract_unallocated(self, osd, pB, edir):
        header = "Extracting unallocated areas to %s" % edir
        print("-" * len(header))
        print(header)
        print("-" * len(header))
        extracting = False
        lastname = None
        bpb = pB.bnode_map["bytes_per_block"]
        blpk = pB.bnode_map["blocks_per_key"]
        bypk = bpb * blpk
        size = pB.bnode_map["size"]
        fstring = "{0:{fill}%db}" % blpk

        alloc_map = self.alloc_map

        for k in range(0, size, bypk):
            if k not in alloc_map:
                off = k
                osd.seek(off)
                mode = 'ab'
                if not extracting:
                    mode = 'wb'
                    lastname = "0x%016x" % off
                    extracting = True
                fname = os.path.join(edir, lastname)
                with open(fname, mode) as out:
                    out.write(osd.read(bypk))
                continue
            mask = fstring.format(alloc_map[k], fill="0")
            for b in range(0, len(mask)):
                if mask[b] == "1":
                    extracting = False
                else:
                    off = k + bpb * b
                    osd.seek(off)
                    mode = 'ab'
                    if not extracting:
                        mode = 'wb'
                        lastname = "0x%016x" % off
                        extracting = True
                    fname = os.path.join(edir, lastname)
                    with open(fname, mode) as out:
                        out.write(osd.read(bpb))


class PrefixHandlerL(GenericPrefixHandler):
    """
    The PrefixHandler to load L-rows.
    """

    counter = 0

    def __init__(self):
        self.l_map = {}
        self.prefix = 'L'

    def parse_dataset(self, k, v):
        k.seek(2)
        PrefixHandlerL.counter += 1
        if PrefixHandlerL.counter % 1000 == 0:
            logging.info("L: reading %dth KV" % PrefixHandlerL.counter)
        thiskey = CephInteger(k, 8, byteorder='big').value
        if v.length() > 0:
            logging.error("Decoding deferred transactions not implemented")
        self.l_map[thiskey] = v  # TODO


class PrefixHandlerX(GenericPrefixHandler):
    """
    The PrefixHandler to load X-rows.
    """

    counter = 0

    def __init__(self):
        self.x_map = {}
        self.prefix = 'X'

    def parse_dataset(self, k, v):
        k.seek(2)
        PrefixHandlerX.counter += 1
        if PrefixHandlerX.counter % 1000 == 0:
            logging.info("X: reading %dth KV" % PrefixHandlerX.counter)
        logging.info("X: k: %s, v %s" % (k, v))
        self.x_map[k] = v  # TODO


class KVObjectNameKey(CephDataType):
    """
    This loads the keys of O-rows in the KV store.
    """
    def __init__(self, handle):
        start = handle.tell()
        self.shard = CephInteger(handle, 1).value
        self.shard -= 0x80
        self.poolid = CephInteger(handle, 8, byteorder='big').value
        self.poolid -= 0x8000000000000000
        self.hash = CephInteger(handle, 4, byteorder='big').value
        self.ns = CephEscapedString(handle).value
        self.key = CephEscapedString(handle).value
        self._set_oid_and_stripe()
        operator = handle.read(1)
        assert(operator in [b"<", b"=", b">"])
        if operator == b"=":
            self.name = self.key
        else:
            self.name = CephEscapedString(handle).value
        self.snap = CephInteger(handle, 8, byteorder='big').value
        self.generation = CephInteger(handle, 8, byteorder='big').value
        trailing = handle.read(1)
        assert(trailing == b"o")
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return "shard: 0x%x, ns: %s, key: %s, name: %s, poolid: 0x%x, snap: 0x%x, gen: 0x%x" % \
            (self.shard, self.ns, self.key, self.name, self.poolid, self.snap,
                self.generation)

    def __eq__(self, other):
        return self.key == other.key

    def __hash__(self):
        return hash((self.key, self.poolid, self.snap, self.generation))

    def _set_oid_and_stripe(self):
        if '.' not in self.key:
            self.oid = self.key
            self.stripe = None
            self.stripe_sort = None
        else:
            if self.key.endswith('.inode'):
                self.oid = '.'.join(self.key.split('.')[:-2])
                self.stripe = '.'.join(self.key.split('.')[-2:])
            else:
                self.oid = '.'.join(self.key.split('.')[:-1])
                self.stripe = self.key.split('.')[-1]
            try:
                self.stripe_sort = int(self.stripe, 16)
            except ValueError:
                self.stripe_sort = self.stripe


class CephEscapedString(CephDataType):
    """
    Reads Escaped object names from the KV store.
    """
    def __init__(self, handle):
        start = handle.tell()
        ra = handle.read(1)
        self.value = ""
        while ra != b"!":
            if ra == b"#" or ra == b"~":
                x = CephInteger(handle, 2, byteorder='big').value
                ra = chr(x)
            else:
                ra = ra.decode("utf-8")
            self.value += ra
            ra = handle.read(1)
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return self.value


class KVONode(CephDataType):
    """
    Reads onode data structures.
    """
    shared_blob_map = {}

    def __init__(self, handle):
        self.spanning_blob_map = {}
        # self.blobs = []
        self.lextents = []
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.oid = CephVarInteger(handle).value
        self.size = CephVarInteger(handle).value
        self.attrs_raw = CephDict(handle, CephString, None,
                                  CephBufferlist, None)
        self.attrs = {}
        klasses = {"_parent": KVINodeBacktrace,
                   "_": KVObjectInfo,
                   "snapset": KVSnapSet,
                   "_layout": KVFileLayout,
                   "_lock.rbd_lock": KVLock}
        for a in self.attrs_raw.elements:
            if a.value in klasses:
                self.attrs[a.value] =\
                    klasses[a.value](self.attrs_raw.elements[a].raw)
            else:
                logging.error("unknown xattr %s: %s" %
                              (a.value, self.attrs_raw.elements[a].raw))

        self.flags = CephInteger(handle, 1).value
        self.extent_map_shards = CephList(handle, KVShardInfo, None)
        self.expected_object_size = CephVarInteger(handle).value
        self.expected_write_size = CephVarInteger(handle).value
        self.alloc_hint_flags = CephVarInteger(handle).value
        end = handle.tell()
        assert(end == self.header.end_offset)

        # Spanning blobs:
        v = CephInteger(handle, 1).value
        assert(v == 2)
        n = CephVarInteger(handle).value
        while n > 0:
            bid = CephVarInteger(handle).value
            logging.debug("spanning blob with id: %d" % bid)
            b = KVBlob(handle, include_ref_map=(v, True))
            self.spanning_blob_map[bid] = b
            n -= 1
        super().__init__(start, end)

    def __str__(self):
        return "oid: %d, object_size: %d, shards: %s" % \
               (self.oid, self.size,
                ", ".join([str(e) for e in self.extent_map_shards]))

    def extract(self, read, write, slack_write, md5_write):
        """
        Extract an object.
        read: The OSD/open file.
        write: The file handle to write the content to.
        slack_write: The file handle to write the slack space to.
        md5_write: The file handle to write the md5 sum of the object content
                   to.
        """
        md5sum = hashlib.md5()
        for le in self.lextents:
            loff = le.logical_offset
            write.seek(loff)
            r, slack = le.read(read)
            logging.debug("length of le %s at offset %s" %
                          (hex(len(r)), hex(loff)))
            md5sum.update(r)
            if len(slack) > 0:
                logging.debug("Found slack of length %s" % hex(len(slack)))
                slack_write.write(slack)
            write.write(r)
        missing = self.size - write.tell()
        if missing > 0:
            missing = b'\x00' * missing
            write.write(missing)
            md5sum.update(missing)
        assert(write.tell() == self.size)
        md5_write.write(md5sum.hexdigest())
        md5_write.write('\n')
        return self.size

    def extract_raw(self, read):
        """
        Get the content of all extents.
        read: The OSD/open file.
        """
        rtotal = b''
        for le in self.lextents:
            r, slack = le.read(read)
            rtotal += r
        return rtotal

    def create_tree(self, edir):
        """
        If an object contains CephFS metadata (in _parent xattr)
        we will create some symlinks to parents and also symlink
        the parents to their child.
        edir: The directory to work in.
        """
        if "_parent" not in self.attrs:
            return
        childinode = self.attrs["_parent"].inode

        for a in self.attrs["_parent"].ancestors:
            path = os.path.join(edir, "%x" % a.inode)
            pathrel = os.path.join('..', "%x" % a.inode)
            childlnk = os.path.join(path, "child_%s" % a.dname)
            child = os.path.join(edir, "%x" % childinode)
            childrel = os.path.join('..', "%x" % childinode)
            parentlnk = os.path.join(edir, "%x" % childinode, "parent")
            slf = os.path.join(edir, "%x" % childinode, "self_%s" % a.dname)
            with open(slf, 'w'):
                pass
            if not os.path.isdir(path):
                os.makedirs(path)
            if not os.path.isdir(child):
                os.makedirs(child)
            if not os.path.exists(childlnk):
                os.symlink(childrel, childlnk)
            if not os.path.exists(parentlnk):
                os.symlink(pathrel, parentlnk)
            childinode = a.inode


class KVExtentMap(CephDataType):
    """
    Handles extent map shards and adds the info to the respective onode.
    """
    CONTIGUOUS = 0x1
    ZEROOFFSET = 0x2
    SAMELENGTH = 0x4
    SPANNING = 0x8
    SHIFTBITS = 0x4

    def __init__(self, handle, onode, noheader=False):
        logging.debug("noheader = %s" % str(noheader))
        self.onode = onode
        start = handle.tell()
        x = min(0x20, handle.length() - handle.tell())
        pos = 0
        if x < 4:
            handle.seek(handle.length())
            super().__init__(start, handle.tell())
            return
        self.end_offset = handle.length()
        if not noheader:
            self.extentmap_length = CephInteger(handle, 4).value
            self.end_offset = self.extentmap_length + handle.tell()

        self.v = CephInteger(handle, 1).value
        assert(self.v == 2)

        self.num = CephVarInteger(handle).value
        self.blobs = [None] * self.num
        prev_len = 0

        for n in range(0, self.num):
            logging.debug("Reading n: %d" % n)
            le = self.LExtent()

            self.blobid = CephVarInteger(handle).value

            if (self.blobid & KVExtentMap.CONTIGUOUS) == 0:
                gap = CephVarIntegerLowz(handle).value
                pos += gap
            le.logical_offset = pos
            le.blob_offset = 0
            if (self.blobid & KVExtentMap.ZEROOFFSET) == 0:
                le.blob_offset = CephVarIntegerLowz(handle).value

            if (self.blobid & KVExtentMap.SAMELENGTH) == 0:
                prev_len = CephVarIntegerLowz(handle).value
            le.length = prev_len
            if (self.blobid & KVExtentMap.SPANNING) != 0:
                blobshift = self.blobid >> KVExtentMap.SHIFTBITS
                le.assign_blob(onode.spanning_blob_map[blobshift])
            else:
                self.blobid >>= KVExtentMap.SHIFTBITS
                if self.blobid != 0:
                    le.assign_blob(self.blobs[self.blobid - 1])
                else:
                    b = KVBlob(handle)
                    self.blobs[n] = b

                    le.assign_blob(b)

            pos += prev_len
            self.onode.lextents.append(le)

        end = handle.tell()
        if not noheader:
            assert(end == self.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "blobs: %s --- extents: %s" %\
               (", ".join([str(b) for b in self.blobs]),
                ", ".join([str(e) for e in self.onode.lextents]))

    class LExtent(object):
        """
        Reads logical extent information.
        """
        def __init__(self):
            self.logical_offset = None
            self.blob_offset = None
            self.length = None
            self.blob = None

        def __str__(self):
            return "0x%x-0x%x-0x%x: %s" %\
                   (self.logical_offset, self.blob_offset,
                    self.length, str(self.blob))

        def assign_blob(self, blob):
            assert(self.blob is None)
            self.blob = blob

        def read(self, read):
            content = self.blob.read(read)
            return (content[:self.length], content[self.length:])

        def pretty(self):
            pes = ", ".join(str(pe) for pe in self.blob.extents)
            return "Logical offset: %s, length: %s, Physical extents: %s" %\
                   (hex(self.logical_offset), hex(self.length), pes)


class KVBlob(CephDataType):
    """
    Read blob info.
    """
    COMPRESSED = 0x2
    CSUM = 0x4
    HAS_UNUSED = 0x8
    SHARED = 0x10

    def __init__(self, handle, include_ref_map=(2, False)):
        logging.debug("include_ref_map = %s" % str(include_ref_map))
        start = handle.tell()
        self.extentsvector_num = CephVarInteger(handle).value
        self.extents = []
        self.sbid = None
        for i in range(0, self.extentsvector_num):
            pe = CephPExtent(handle)
            logging.debug("found pe: %s" % str(pe))
            if pe.valid:
                self.extents.append(pe)
        self.flags = CephVarInteger(handle).value
        logging.debug("flags: 0x%x" % self.flags)
        self.compressed_length = None
        if self.flags & KVBlob.COMPRESSED != 0:
            self.logical_length = CephVarIntegerLowz(handle).value
            self.compressed_length = CephVarIntegerLowz(handle).value
        else:
            x = 0
            for e in self.extents:
                x += e.length
            self.logical_length = x

        if self.flags & KVBlob.CSUM != 0:
            self.csum_type = CephInteger(handle, 1).value
            self.csum_chunk_order = CephInteger(handle, 1).value
            csumlen = CephVarInteger(handle).value
            self.csum_data = CephUnknown(handle, csumlen)

        if self.flags & KVBlob.HAS_UNUSED != 0:
            self.unused = CephInteger(handle, 2).value

        if self.flags & KVBlob.SHARED != 0:
            self.sbid = CephInteger(handle, 8).value

        if include_ref_map[1]:
            assert(include_ref_map[0] > 1)
            au_size = CephVarInteger(handle).value
            if au_size > 0:
                num_au = CephVarInteger(handle).value
                if num_au == 0:
                    total_bytes = CephVarInteger(handle).value
                else:
                    for i in range(0, num_au):
                        byte_per_au = CephVarInteger(handle).value

        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        if self.compressed_length is not None:
            return "llength: 0x%x, clength: 0x%x, extents: %s" %\
                   (self.logical_length, self.compressed_length,
                    [str(e) for e in self.extents])
        return "llength: 0x%x, extents: %s" %\
               (self.logical_length, [str(e) for e in self.extents])

    def read(self, read):
        read_ext = []
        for pe in self.extents:
            read_ext.append(pe.read(read))
        return b''.join(read_ext)


class KVCNode(CephDataType):
    """
    Read cnode data structures.
    """
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.bits = CephInteger(handle, 4).value
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "cnode bits: %d (0x%x)" % (self.bits, self.bits)


class KVINodeBacktrace(CephDataType):
    """
    Read inode backtrace data structures.
    """
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.inode = CephInteger(handle, 8).value
        self.ancestors = CephList(handle, KVINodeBackpointer, None)
        self.pool = CephInteger(handle, 8).value
        self.old_pools = CephList(handle, CephInteger, 8)
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        ancestors = "->".join([str(a) for a in self.ancestors])
        return "inode: 0x%x -> ancestors: %s, pool: 0x%x" %\
               (self.inode, ancestors, self.pool)

    def path(self):
        p = "/".join(reversed([a.dname for a in self.ancestors][1:]))
        return p

    def filename(self):
        p = self.ancestors.elements[0].dname
        return p

    def fullpath(self):
        p = "/".join(reversed([a.dname for a in self.ancestors]))
        p = "<CephFSroot>/%s" % p
        return p


class KVINodeBackpointer(CephDataType):
    """
    Read inode backpointer data structures.
    """
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.inode = CephInteger(handle, 8).value
        self.dname = CephString(handle).value
        self.version = CephInteger(handle, 8).value
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "ino: 0x%x, dname: %s, ver: 0x%x" %\
               (self.inode, self.dname, self.version)


class KVFileLayout(CephDataType):
    """
    Read file layout information.
    """
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.stripe_unit = CephInteger(handle, 4).value
        self.stripe_count = CephInteger(handle, 4).value
        self.object_size = CephInteger(handle, 4).value
        self.poolid = CephInteger(handle, 8).value
        self.pool_ns = CephString(handle).value
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "objectsize: 0x%x, poolid: 0x%x, pool_ns: %s" %\
               (self.object_size, self.poolid, self.pool_ns)


class KVObjectInfo(CephDataType):
    """
    Read object info data structures.
    """
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.soid = CephHObject(handle)
        myolocheader = CephBlockHeader(handle)
        handle.seek(myolocheader.end_offset)
        handle.read(4)  # don't use
        self.version = CephEversion(handle)
        self.prior_version = CephEversion(handle)
        self.last_reqid = CephReqID(handle)
        self.size = CephInteger(handle, 8).value
        self.mtime = CephUTime(handle)
        handle.seek(self.header.end_offset)
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "size: %s, mtime: %s, soid: %s" %\
               (hex(self.size), self.mtime, self.soid)


class KVSnapSet(CephDataType):
    """
    Read snapset info.
    """
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.snapid = CephInteger(handle, 8).value
        handle.read(1)
        self.snaps = CephIntegerList(handle, 8)
        self.clones = CephIntegerList(handle, 8)
        handle.seek(self.header.end_offset)
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "snapid: 0x%x, snaps: %s, clones: %s" %\
               (self.snapid, self.snaps, self.clones)


class KVLock(CephDataType):
    """
    Read lock information.
    """
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.desc = CephString(handle).value
        self.type = CephInteger(handle, 1).value
        self.tag = CephString(handle).value
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "desc: %s, type: %d, tag: %s" %\
               (self.desc, self.type, self.tag)


class KVShardInfo(CephDataType):
    """
    Read shard info.
    """
    def __init__(self, handle):
        self.used = False
        start = handle.tell()
        self.offset = CephVarInteger(handle).value
        self.bytes = CephVarInteger(handle).value
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return "shard: offset-bytes: 0x%x-0x%x" % (self.offset, self.bytes)


class KVFNode(CephDataType):
    """
    Read fnode data structures.
    """
    def __init__(self, handle):
        logging.debug(handle)
        self.dentries = {}
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.version = CephInteger(handle, 8).value
        self.snap_purged_thru = CephInteger(handle, 8).value
        self.fragstat = CephFragInfo(handle)
        self.accounted_fragstat = CephFragInfo(handle)
        self.rstat = CephNestInfo(handle)
        self.accounted_rstat = CephNestInfo(handle)
        if self.header.v >= 3:
            self.damage_flags = CephInteger(handle, 4).value
        if self.header.v >= 4:
            self.recursive_scrub_version = CephInteger(handle, 8).value
            self.recursive_scrub_stamp = CephInteger(handle, 8).value
            self.localized_scrub_version = CephInteger(handle, 8).value
            self.localized_scrub_stamp = CephInteger(handle, 8).value
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "fnode: fragstat: %s, rstat: %s" %\
               (self.fragstat, self.rstat)


class KVINode(CephDataType):
    """
    Read inode data structures.
    """
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.inode = CephInteger(handle, 8).value
        self.rdev = CephInteger(handle, 4).value
        self.ctime = CephUTime(handle)
        self.mode = CephInteger(handle, 4).value
        self.uid = CephInteger(handle, 4).value
        self.gid = CephInteger(handle, 4).value
        self.nlink = CephInteger(handle, 4).value
        CephInteger(handle, 1)  # forget anchored
        if self.header.v >= 4:
            self.dir_layout = CephInteger(handle, 8).value
        else:
            raise NotImplementedError()
        self.layout = KVFileLayout(handle)
        self.size = CephInteger(handle, 8).value
        self.truncate_seq = CephInteger(handle, 4).value
        self.truncate_size = CephInteger(handle, 8).value
        self.truncate_from = CephInteger(handle, 8).value
        if self.header.v >= 5:
            self.truncate_pending = CephInteger(handle, 4).value
        else:
            self.truncate_pending = 0
        self.mtime = CephUTime(handle)
        self.atime = CephUTime(handle)
        self.time_warp_seq = CephInteger(handle, 4).value
        if self.header.v >= 3:
            self.number_client_ranges = CephInteger(handle, 4).value
            if self.number_client_ranges > 0:
                raise NotImplementedError()
        else:
            raise NotImplementedError()
        self.dirstat = CephFragInfo(handle)
        self.rstat = CephNestInfo(handle)
        self.accounted_rstat = CephNestInfo(handle)
        self.version = CephInteger(handle, 8).value
        self.file_data_version = CephInteger(handle, 8).value
        self.xattr_version = CephInteger(handle, 8).value
        handle.seek(self.header.end_offset)
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "inode: 0x%x, size: %d, rdev: %d, ctime: %s, mode: %o, uid: %d, gid: %d, nlink: %d, dir_layout: 0x%x" %\
               (self.inode, self.size, self.rdev, self.ctime, self.mode, self.uid, self.gid, self.nlink, self.dir_layout)


class CephEversion(CephDataType):
    """
    Read eversion data structures.
    """
    def __init__(self, handle):
        start = handle.tell()
        self.version = CephInteger(handle, 8).value
        self.epoch = CephInteger(handle, 4).value
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return "0x%x.0x%x (%d.%d)" %\
               (self.version, self.epoch, self.version, self.epoch)

    def get_key_name(self):
        return "%010d.%020d" % (self.epoch, self.version)


class CephPGInfo(CephDataType):
    """
    Read pg info data structures.
    """
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.pgid = CephPG(handle)
        self.last_update = CephEversion(handle)
        self.last_complete = CephEversion(handle)
        self.log_tail = CephEversion(handle)
        self.hobject_block_header = CephBlockHeader(handle)
        handle.seek(self.hobject_block_header.end_offset)
        # self.unknown = CephUnknown(handle, 0x20)
        handle.seek(self.header.end_offset)
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        # return "pgid: %s, unknown: %s" % \
        #     (self.pgid, self.unknown)
        return "pgid: %s" % \
            (self.pgid)


class CephPGFastinfo(CephDataType):
    """
    Read pg fast info data structures.
    """
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.last_update = CephEversion(handle)
        self.last_complete = CephEversion(handle)
        self.last_user_version = CephInteger(handle, 8).value
        self.stats_version = CephEversion(handle)
        self.stats_reported_seq = CephInteger(handle, 8).value
        self.stats_last_fresh = CephUTime(handle)
        self.stats_last_active = CephUTime(handle)
        self.stats_last_peered = CephUTime(handle)
        self.stats_last_clean = CephUTime(handle)
        self.stats_last_unstable = CephUTime(handle)
        self.stats_last_undegraded = CephUTime(handle)
        self.stats_fullsized = CephUTime(handle)
        self.stats_log_size = CephInteger(handle, 8).value
        self.sss_num_bytes = CephInteger(handle, 8).value
        self.sss_num_objects = CephInteger(handle, 8).value
        self.sss_num_object_copies = CephInteger(handle, 8).value
        self.sss_num_rd = CephInteger(handle, 8).value
        self.sss_num_rd_kb = CephInteger(handle, 8).value
        self.sss_num_wr = CephInteger(handle, 8).value
        self.sss_num_wr_kb = CephInteger(handle, 8).value
        self.sss_num_objects_dirty = CephInteger(handle, 8).value
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "last_update: %s, last_active: %s, log_size: %d, objects: %d" %\
            (self.last_update, self.stats_last_active, self.stats_log_size,
                self.sss_num_objects)


class CephPastIntervals(CephDataType):
    """
    Read past intervals.
    """
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.type = CephInteger(handle, 1)
        self.header2 = CephBlockHeader(handle)
        self.first = CephInteger(handle, 4).value
        self.last = CephInteger(handle, 4).value
        self.all_participants = CephList(handle, CephPGShard, None)
        self.intervals = CephList(handle, CephCompactIntervals, None)
        self.unknown = CephUnknown(handle, handle.length() - handle.tell())
        handle.seek(self.header.end_offset)
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "past intervals. first: %d, last: %d, p: %d, intervals: %d" % \
               (self.first, self.last, self.all_participants.num_elements,
                self.intervals.num_elements)


class CephCompactIntervals(CephDataType):
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.first = CephInteger(handle, 4).value
        self.last = CephInteger(handle, 4).value
        self.acting = CephList(handle, CephPGShard, None)
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)


class CephPGShard(CephDataType):
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.osd = CephInteger(handle, 4).value
        self.shard = CephInteger(handle, 1).value
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)


class CephPGLogEntry(CephDataType):
    def __init__(self, handle, checksum=False):
        start = handle.tell()
        if checksum:
            self.entrylength = CephInteger(handle, 4).value
        self.header = CephBlockHeader(handle)
        self.op = CephInteger(handle, 4).value
        v = self.header.v
        if v < 2:
            raise NotImplementedError("Cannot decode CephPGLogEntry version %d"
                                      % self.header.v)
        self.soid = CephHObject(handle)
        self.version = CephEversion(handle)
        if v >= 6 and self.op == 5:  # LOST_REVERT
            self.revert_to = CephEversion(handle)
        else:
            self.prior_version = CephEversion(handle)
        self.reqid = CephReqID(handle)
        self.mtime = CephUTime(handle)
        if self.op == 5:  # LOST_REVERT
            self.prior_version = CephEversion(handle)
        self.snaps = CephBufferlist(handle)
        self.user_version = CephInteger(handle, 8).value
        handle.seek(self.header.end_offset)
        end = handle.tell()
        assert(end == self.header.end_offset)
        if checksum:
            assert(handle.tell() == start + 4 + self.entrylength)
            self.crc = CephInteger(handle, 4).value
            # TODO CRC checking
        super().__init__(start, end)

    def __str__(self):
        return "soid: %s, mtime: %s, reqid: %s, version: %s" %\
               (self.soid, self.mtime, self.reqid, self.version)


class CephPGLogDup(CephDataType):
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.reqid = CephReqID(handle)
        self.version = CephEversion(handle)
        self.user_version = CephInteger(handle, 8)
        self.return_code = CephInteger(handle, 4)
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "reqid: %s, version: %s, user_version: %s, return_code: %s" %\
               (self.reqid, self.version, self.user_version, self.return_code)


class CephHObject(CephDataType):
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.key = CephString(handle).value
        self.oid = CephString(handle).value
        self.snap = CephInteger(handle, 8).value
        self.hash = CephInteger(handle, 4).value
        if self.header.v >= 2:
            self.max = CephInteger(handle, 1).value
        if self.header.v >= 4:
            self.nspace = CephString(handle).value
            self.pool = CephInteger(handle, 8).value
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "key: %s, oid: %s, nspace: %s, pool: %s" %\
               (self.key, self.oid, self.nspace, hex(self.pool))


class CephReqID(CephDataType):
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.name_type = CephInteger(handle, 1).value
        self.name_num = CephInteger(handle, 8).value
        self.tid = CephInteger(handle, 8).value
        self.incarnation = CephInteger(handle, 4).value
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "type: %d, num: %d, tid: %d, inc: %d" %\
               (self.name_type, self.name_num, self.tid, self.incarnation)


class CephPExtent(CephDataType):
    """
    Read physical extent information.
    """
    pextentlist = []
    alloc = None
    unalloc = None
    INVALID_OFFSET = 0xfffffffff

    def __init__(self, handle):
        start = handle.tell()
        readahead = CephInteger(handle, 10).value
        if readahead == 0x01FFFFFFFFFFFFFFFFFF:
            self.offset = CephPExtent.INVALID_OFFSET
            self.valid = False
        else:
            handle.seek(start)
            self.offset = CephLBA(handle).value
            self.valid = True
        self.length = CephVarIntegerLowz(handle).value
        end = handle.tell()
        super().__init__(start, end)
        if self.valid:
            CephPExtent.pextentlist.append(self)

    def __str__(self):
        return "0x%x-0x%x" % (self.offset, self.length)

    def __eq__(self, other):
        return self.offset == other.offset

    def __lt__(self, other):
        return self.offset < other.offset

    def __gt__(self, other):
        return self.offset > other.offset

    def read(self, read):
        """
        Get a physical extent's content.
        read: The OSD/open file.
        """
        if not self.valid:
            raise ValueError()
        logging.debug("read pextent: %s" % str(self))
        read.seek(self.offset)
        return read.read(self.length)

    @staticmethod
    def _init_alloc_state(osdlength):
        logging.info("Getting allocated and unallocated area")
        alloc = [(0, 0x2000)]  # Reserved (see BlueStore.cc)
        unalloc = []
        for pe in sorted(CephPExtent.pextentlist):
            logging.debug(str(pe))
            e = alloc[-1]
            if pe.offset > e[0] + e[1]:
                alloc.append((pe.offset, pe.length))
                uoff = e[0] + e[1]
                ulen = pe.offset - uoff
                unalloc.append((uoff, ulen))
            elif pe.offset == e[0] + e[1]:
                alloc[-1] = (e[0], e[1] + pe.length)
            else:
                raise Exception("should not happen")
        last_offset = alloc[-1][0] + alloc[-1][1]
        if last_offset < osdlength:
            unalloc.append((last_offset, osdlength - last_offset))
        elif last_offset == osdlength:
            pass
        else:
            raise Exception("whoops. More extents than OSD")
        CephPExtent.alloc = alloc
        CephPExtent.unalloc = unalloc

    @staticmethod
    def pretty_print(osdlength):
        if not CephPExtent.alloc or not CephPExtent.unalloc:
            CephPExtent._init_alloc_state(osdlength)
        print("----------------")
        print("Allocated areas:")
        print("----------------")
        for e in CephPExtent.alloc:
            print("0x%x-0x%x" % e)
        print("------------------")
        print("Unallocated areas:")
        print("------------------")
        for e in CephPExtent.unalloc:
            print("0x%x-0x%x" % e)
        sys.stdout.flush()

    @staticmethod
    def extract_unallocated(osdlength, r, edir):
        if not CephPExtent.alloc or not CephPExtent.unalloc:
            CephPExtent._init_alloc_state(osdlength)
        for e in CephPExtent.unalloc:
            logging.debug("Extracting: 0x%x-0x%x" % e)
            fname = os.path.join(edir, "0x%016x" % e[0])
            with open(fname, 'wb') as w:
                amount = e[1]
                while amount > 0:
                    read = min(amount, 0x20000000)  # Read 512M
                    w.write(r.read(read))
                    amount -= read

    def analyze_unallocated(osdlength, r):
        if not CephPExtent.alloc or not CephPExtent.unalloc:
            CephPExtent._init_alloc_state(osdlength)
        print("------------------------------------------------")
        print("Empty and non-empty blocks in unallocated areas:")
        print("------------------------------------------------")
        print("(Blocksize is 512kiB)")
        print("Please wait...")
        bsize = 0x80000  # 512kiB
        check = bsize * b'\x00'
        empty = 0
        nonempty = 0
        for e in CephPExtent.unalloc:
            logging.info("Reading address %s, length %s" %
                         (hex(e[0]), hex(e[1])))
            r.seek(e[0])
            while r.tell() < e[0] + e[1]:
                content = r.read(bsize)
                if content == check:
                    empty += 1
                else:
                    nonempty += 1

        total = empty + nonempty
        print("Total:                       %10d" % total)
        print("Empty blocks (absolute):     %10d" % empty)
        print("Empty blocks (percent):      %10d" % (empty / total * 100))
        print("Non-empty blocks (absolute): %10d" % nonempty)
        print("Non-empty blocks (percent):  %10d" % (nonempty / total * 100))


class CephStatfs(CephDataType):
    def __init__(self, handle):
        start = handle.tell()
        self.allocated = CephInteger(handle, 8).value
        self.stored = CephInteger(handle, 8).value
        self.compressed_original = CephInteger(handle, 8).value
        self.compressed = CephInteger(handle, 8).value
        self.compressed_allocated = CephInteger(handle, 8).value
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        s = "allocated: 0x%x, stored: 0x%x, compressed_original: 0x%x," +\
            " compressed: 0x%x, compressed_allocated: 0x%x"
        return s % (self.allocated, self.stored, self.compressed_original,
                    self.compressed, self.compressed_allocated)


class CephFragInfo(CephDataType):
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.version = CephInteger(handle, 8).value
        self.mtime = CephUTime(handle)
        self.nfiles = CephInteger(handle, 8).value
        self.nsubdirs = CephInteger(handle, 8).value
        self.change_attr = CephInteger(handle, 8).value
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "mtime: %s, nfiles: %d, nsubdirs: %d, change_attr: %d" %\
               (self.mtime, self.nfiles, self.nsubdirs, self.change_attr)


class CephNestInfo(CephDataType):
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.version = CephInteger(handle, 8).value
        self.rbytes = CephInteger(handle, 8).value
        self.rfiles = CephInteger(handle, 8).value
        self.rsubdirs = CephInteger(handle, 8).value
        CephInteger(handle, 8)  # forget ranchors
        self.rsnaprealms = CephInteger(handle, 8).value
        self.rctime = CephUTime(handle)
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "rbytes: %d, rfiles: %d, rsubdirs: %d, rctime: %s" %\
               (self.rbytes, self.rfiles, self.rsubdirs, self.rctime)
