"""
Module to decode osdmaps, inc_osdmaps, osd_superblock, rbd_ids.
"""

from vampyr.datatypes import ByteHandler, CephBlockHeader, CephUUID,\
    CephInteger, CephUTime, CephDict, CephString, CephList, CephIntegerList,\
    CephBufferlist, CephStringDict, CephIntegerPairList, CephFloat,\
    CephUnknown, CephDataType
from vampyr.exceptions import VampyrMagicException
import datetime
import logging


class CephPG(CephDataType):
    """
    Reads PG data structures.
    """

    def __init__(self, handle):
        if handle is None:
            self.v = 0
            self.m_pool = 0
            self.m_seed = 0
            super().__init__(0, 0)
        else:
            start = handle.tell()
            self.v = CephInteger(handle, 1).value
            self.m_pool = CephInteger(handle, 8).value
            self.m_seed = CephInteger(handle, 4).value
            self._end = CephInteger(handle, 4).value
            end = handle.tell()
            super().__init__(start, end)

    def __str__(self):
        return "PG pool: %x, seed: 0x%04x" % \
               (self.m_pool, self.m_seed)

    def __eq__(self, other):
        return str(self) == str(other)

    def __gt__(self, other):
        return str(self) > str(other)

    def __lt__(self, other):
        return str(self) < str(other)

    def __hash__(self):
        return hash((self.m_pool, self.m_seed, self.v))


class CephEntityAddr(CephDataType):
    """
    Reads entity addresses. (e.g. IPs)
    """
    def __init__(self, handle):
        start = handle.tell()
        self.first = CephInteger(handle, 1).value
        self.header = CephBlockHeader(handle)
        self.type = CephInteger(handle, 4).value
        self.nonce = CephInteger(handle, 4).value
        self.elen = CephInteger(handle, 4).value
        self.sa_family = CephInteger(handle, 2).value
        self.sa_port = CephInteger(handle, 2).value
        sa = []
        for i in range(0, 4):
            sa.append(str(CephInteger(handle, 1).value))
        self.sockaddr = ".".join(sa)
        self.ignore = CephUnknown(handle, self.elen - 8)
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "EntityAddr: %s, Port: %d, AF_INET: %d" % \
               (self.sockaddr, self.sa_port, self.sa_family)


class CephOSDInfo(CephDataType):
    """
    Reads OSD info data structures.
    """
    def __init__(self, handle):
        start = handle.tell()
        self.compat = CephInteger(handle, 1).value
        self.last_clean_begin = CephInteger(handle, 4).value
        self.last_clean_end = CephInteger(handle, 4).value
        self.up_from = CephInteger(handle, 4).value
        self.up_thru = CephInteger(handle, 4).value
        self.down_at = CephInteger(handle, 4).value
        self.lost_at = CephInteger(handle, 4).value
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return "last_clean_begin: %d, last_clean_end: %d, up_from: %d" % \
               (self.last_clean_begin, self.last_clean_end,
                self.up_from)


class CephOSDXInfo(CephDataType):
    """
    Reads extended OSD infos.
    """
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.down_stamp = CephUTime(handle)
        self.laggy_probability = CephInteger(handle, 4).value
        self.laggy_interval = CephInteger(handle, 4).value
        self.features = CephInteger(handle, 8).value
        self.old_weight = CephInteger(handle, 4).value
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        fstring = "down_stamp: %s, laggy_probability: %x, " + \
                  "laggy_interval: %d, features: %x"
        return fstring % \
            (str(self.down_stamp), self.laggy_probability,
             self.laggy_interval, self.features)


class CephPGPool(CephDataType):
    """
    Reads PG pool infos.
    """
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
#        v = self.header.v
        self.type = CephInteger(handle, 1).value
        self.size = CephInteger(handle, 1).value
        self.crush_rule = CephInteger(handle, 1).value
        self.object_hash = CephInteger(handle, 1).value
        self.pg_num = CephInteger(handle, 4).value
        self.pgp_num = CephInteger(handle, 4).value
        self.lpg_num = CephInteger(handle, 4).value
        self.lpgp_num = CephInteger(handle, 4).value
        self.last_change = CephInteger(handle, 4).value
        self.snap_seq = CephInteger(handle, 8).value
        self.snap_epoch = CephInteger(handle, 4).value
#        if v >= 3:
#            pass  # TODO
#        else:
#            print("Error")
#            return

#        if v >= 4:
#            self.flags = CephInteger(handle, 8).value
#            self.crash_replay_interval = CephInteger(handle, 4).value
#        else:
#            self.flags = 0

#        if v >= 7:
#            self.min_size = CephInteger(handle, 1).value
#        else:
#            self.min_size = self.size - self.size / 2

        handle.seek(self.header.end_offset)  # TODO
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "PGP: version %d, size 0x%x" % \
               (self.header.v, self.size)


class CephCrush(CephDataType):
    """
    Reads crush information
    """
    def __init__(self, handle):
        start = handle.tell()
        self.magic = CephInteger(handle, 4).value
        if self.magic != 0x00010000:
            logging.error("Magic number is 0x%0x" % self.magic)
            raise VampyrMagicException("Crush error")
        self.max_buckets = CephInteger(handle, 4).value
        self.max_rules = CephInteger(handle, 4).value
        self.max_devices = CephInteger(handle, 4).value
        self.buckets = []
        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        ret = "Crush: max_buckets %d, max_rules %d, max_devices %d" % \
              (self.max_buckets, self.max_rules, self.max_devices)
        return ret


class CephOSDState(CephInteger):
    """
    Reads the OSD state.
    """
    flags = {0x1: "EXISTS",
             0x2: "UP",
             0x4: "AUTOOUT",
             0x8: "NEW",
             0x10: "FULL",
             0x20: "NEARFULL",
             0x40: "BACKFILLFULL",
             0x80: "DESTROYED",
             0x100: "NOUP",
             0x200: "NODOWN",
             0x400: "NOIN",
             0x800: "NOOUT"}

    def __init__(self, handle):
        super().__init__(handle, 4)

    def __str__(self):
        keys = sorted(CephOSDState.flags.keys())
        hints = []
        for k in keys:
            if k & self.value == k:
                hints.append(CephOSDState.flags[k])
        return "0x%04x (%s)" % (self.value, ", ".join(hints))


class CephFSLogEntry(CephDataType):
    """
    Reads crush information
    """
    def __init__(self, handle):
        start = handle.tell()
        self.length = CephInteger(handle, 4).value
        self.type = CephFSLogEventType(handle)
        logging.error(str(self.type))
        assert(self.type.value == 0)  # We only understand new encoding
        self.header2 = CephBlockHeader(handle)
        assert(self.header2.v == 1)
        self.type2 = CephFSLogEventType(handle)
        if self.type2.value == 20:  # Update operation:
            self.log_entry = CephFSLogEntryUpdate(handle)
        else:
            self.log_entry = "<Not Implemented>"
            handle.seek(start + self.length + 4)
        end = handle.tell()
        assert(end == start + self.length + 4)
        super().__init__(start, end)

    def __str__(self):
        return "%s" % str(self.log_entry)


class CephFSLogEntryUpdate(CephDataType):
    """
    Reads crush information
    """
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        if self.header.v >= 2:
            self.stamp = CephUTime(handle)
        else:
            self.stamp = ""
        self.type = CephString(handle).value
        self.metablob = CephFSLogMetaBlob(handle)
        self.client_map = CephBufferlist(handle)
        if self.header.v >= 3:
            self.cmapv = CephInteger(handle, 8).value
        else:
            self.cmapv = 0
        self.reqid_name = CephUnknown(handle, 6)
        self.reqid_tid = CephInteger(handle, 8).value
        self.had_slaves = CephInteger(handle, 4).value
        end = handle.tell()
        logging.error(end)
        logging.error(self.header.end_offset)
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "CephFSLogEntry: type %s" % self.type


class CephFSLogMetaBlob(CephDataType):
    """
    Reads crush information
    """
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        handle.seek(self.header.end_offset)
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "Metablob"


class CephFSLogEventType(CephInteger):
    """
    Reads the CephFS Log Event type.
    """
    flags = {0: "NEW_ENECODING",
             1: "UNUSED",
             2: "SUBTREEMAP",
             3: "EXPORT",
             4: "IMPORTSTART",
             5: "IMPORTFINISHED",
             6: "FRAGMENT",
             9: "RESETJOURNAL",
             10: "SESSION",
             11: "SESSIONS_OLD",
             12: "SESSIONS",
             20: "UPDATE",
             21: "SLAVEUPDATE",
             22: "OPEN",
             23: "COMMITTED",
             42: "TABLECLIENT",
             43: "TABLESERVER",
             50: "SUBTREEMAP_TEST",
             51: "NOOP"}

    def __init__(self, handle):
        super().__init__(handle, 4)

    def __str__(self):
        if self.value not in self.flags:
            hint = "<INVALID OP CODE>"
        else:
            hint = "EVENT_%s" % self.flags[self.value]
        return "%d (%s)" % (self.value, hint)


def decode_osdmap(onode, read):
    """
    Decode an osdmap.
    onode: If not None, we read from the physical extents of the object.
           If None, we read from 'read'.
    read: The OSD/open file. If onode is None, we must be at the position
          where the osdmap should be read from.

    returns: Tuple of readable output and dictionary of values.
    """
    if onode:
        o = ByteHandler(onode.extract_raw(read))
    else:
        o = read
    h = {}
    now = datetime.datetime.now().timestamp()

    # start = o.tell()
    h['header'] = CephBlockHeader(o)
    assert(h['header'].blength < 0x100000)
    h['client_usable_header'] = CephBlockHeader(o)
    assert(h['client_usable_header'].blength < 0x100000)
    v = h['client_usable_header'].v
    h['fsid'] = CephUUID(o)
    h['epoch'] = CephInteger(o, 4)
    h['created'] = CephUTime(o)
    assert(h['created'].timestamp < now)
    h['modified'] = CephUTime(o)
    assert(h['modified'].timestamp < now)
    assert(h['modified'].timestamp >= h['created'].timestamp)
    h['pools'] = CephDict(o, CephInteger, 8, CephPGPool, None)
    h['pool_name'] = CephDict(
        o, CephInteger, 8, CephString, None)
    h['pool_max'] = CephInteger(o, 4)
    h['flags'] = CephInteger(o, 4)
    h['max_osd'] = CephInteger(o, 4)
    h['osd_state'] = CephList(o, CephOSDState, None)
    h['osd_weight'] = CephList(o, CephInteger, 4)
    h['client_addr'] = CephList(o, CephEntityAddr, None)
    h['pg_temp'] = CephDict(o, CephPG, None, CephIntegerList, 4)
    h['primary_temp'] = CephDict(o, CephPG, None, CephInteger, 4)
    h['osd_primary_affinity'] = CephList(o, CephInteger, 4)
    h['crush_raw'] = CephBufferlist(o)
    if h['crush_raw'].raw.length() > 0:
        start = h['crush_raw'].start + 4
        end = h['crush_raw'].end
        logging.info("Start: %s" % hex(start))
        logging.info("End: %s" % hex(end))
        # logging.info("Crush %s" % h['crush_raw'].print_value())
        o.seek(start)
        h['crush'] = CephCrush(o)
        o.seek(end)
        logging.info("crush: %s" % str(h['crush']))
    h['erasure_code_profiles'] = CephDict(
        o, CephString, None, CephStringDict, None)
    if v >= 4:
        h['pg_upmap'] = CephDict(o, CephPG, None, CephIntegerList, 4)
        h['pg_upmap_items'] = CephDict(
            o, CephPG, None, CephIntegerPairList, 4)
    else:
        raise NotImplementedError('v < 4 not implemented')

    if v >= 6:
        h['crush_version'] = CephInteger(o, 4)
    if v >= 7:
        h['new_removed_snaps'] = CephDict(
            o, CephInteger, 8, CephInteger, 4)  # TODO
        h['new_purged_snaps'] = CephDict(
            o, CephInteger, 8, CephInteger, 4)  # TODO

    assert(o.tell() == h['client_usable_header'].end_offset)

    h['osd_only_header'] = CephBlockHeader(o)
    assert(h['osd_only_header'].blength < 0x100000)
    v = h['osd_only_header'].v
    assert(h['osd_only_header'].c == 1)
    h['hb_back_addr'] = CephList(o, CephEntityAddr, None)
    h['osdinfo'] = CephList(o, CephOSDInfo, None)
    h['blacklickt_map'] = CephDict(o, CephEntityAddr,
                                   None, CephUTime, None)
    h['cluster_addr'] = CephList(o, CephEntityAddr, None)
    h['cluster_snapshot_epoch'] = CephInteger(o, 4)
    h['cluster_snapshot'] = CephString(o)
    h['osd_uuid'] = CephList(o, CephUUID, None)
    h['osdxinfo'] = CephList(o, CephOSDXInfo, None)
    h['hb_front_addr'] = CephList(o, CephEntityAddr, None)

    if v >= 2:
        h['nearfull_ratio'] = CephInteger(o, 4)
        h['full_ratio'] = CephInteger(o, 4)
        h['backfillfull_ratio'] = CephInteger(o, 4)
    if v >= 5:
        h['require_min_compat_client'] = CephInteger(o, 1)
        h['require_osd_release'] = CephInteger(o, 1)
    if v >= 6:
        h['removed_snaps_queue'] = CephDict(
            o, CephInteger, 8, CephInteger, 4)  # TODO
    assert(o.tell() == h['osd_only_header'].end_offset)

    h['crc'] = CephInteger(o, 4)
    assert(o.tell() == h['header'].end_offset)
    # end = o.tell()

    return _format_decode_output(h), h


def decode_inc_osdmap(onode, read):
    """
    Decode an inc_osdmap.
    onode: If not None, we read from the physical extents of the object.
           If None, we read from 'read'.
    read: The OSD/open file. If onode is None, we must be at the position
          where the inc_osdmap should be read from.

    returns: Tuple of readable output and dictionary of values.
    """
    if onode:
        o = ByteHandler(onode.extract_raw(read))
    else:
        o = read
    h = {}
    now = datetime.datetime.now().timestamp()

    # start = o.tell()
    h['header'] = CephBlockHeader(o)
    assert(h['header'].blength < 0x100000)
    h['client_usable_header'] = CephBlockHeader(o)
    assert(h['client_usable_header'].blength < 0x100000)
    v = h['client_usable_header'].v
    h['fsid'] = CephUUID(o)
    h['epoch'] = CephInteger(o, 4)
    h['modified'] = CephUTime(o)
    assert(h['modified'].timestamp < now)
    h['new_pool_max'] = CephInteger(o, 8)
    h['new_flags'] = CephInteger(o, 4)
    assert(h['new_flags'].value == 0xffffffff)
    h['fullmap_raw'] = CephBufferlist(o)
    if h['fullmap_raw'].raw.length() > 0:
        start = h['fullmap_raw'].start + 4
        end = h['fullmap_raw'].end
        logging.info("Start: %s" % hex(start))
        logging.info("End: %s" % hex(end))
        logging.info("Fullmap %s" % h['fullmap_raw'].print_value())
        o.seek(start)
        fullmap = decode_osdmap(None, o)[1]
        for k in fullmap:
            h['fullmap_%s' % k] = fullmap[k]
        o.seek(end)
    h['crush_raw'] = CephBufferlist(o)
    if h['crush_raw'].raw.length() > 0:
        start = h['crush_raw'].start + 4
        end = h['crush_raw'].end
        logging.info("Start: %s" % hex(start))
        logging.info("End: %s" % hex(end))
        logging.info("Crush %s" % h['crush_raw'].print_value())
        o.seek(start)
        h['crush'] = CephCrush(o)
        o.seek(end)
        logging.info("crush: %s" % str(h['crush']))
    h['new_max_osd'] = CephInteger(o, 4)
    h['new_pools'] = CephDict(o, CephInteger, 8, CephPGPool, None)
    h['new_pool_names'] = CephDict(
        o, CephInteger, 8, CephString, None)
    h['old_pools'] = CephList(o, CephInteger, 8)
    h['new_up_client'] = CephDict(
        o, CephInteger, 4, CephEntityAddr, None)
    if v < 5:
        raise NotImplementedError('v < 5 not implemented')
    h['new_state'] = CephDict(o, CephInteger, 4, CephInteger, 4)
    h['new_weight'] = CephDict(o, CephInteger, 4, CephInteger, 4)
    h['number_new_pg_temp'] = CephDict(
        o, CephPG, None, CephIntegerList, 4)
    h['new_primary_temp'] = CephDict(o, CephPG, None, CephInteger, 4)
    if v >= 4:
        h['new_primary_affinity'] = CephDict(
            o, CephInteger, 4, CephInteger, 4)
        h['new_erasure_code_profiles'] = CephDict(
            o, CephString, None, CephStringDict, None)
        h['old_erasure_code_profiles'] = CephList(
            o, CephString, None)
        h['new_pg_upmap'] = CephDict(
            o, CephPG, None, CephIntegerList, 4)
        h['old_pg_upmap'] = CephList(o, CephPG, None)
        h['new_pg_upmap_items'] = CephDict(
            o, CephPG, None, CephIntegerPairList, 4)
        h['old_pg_upmap_items'] = CephList(o, CephPG, None)
    if v >= 6:
        h['new_removed_snaps'] = CephDict(
            o, CephInteger, 8, CephString, None)  # TODO
        h['new_purged_snaps'] = CephDict(
            o, CephInteger, 8, CephString, None)  # TODO
    assert(o.tell() == h['client_usable_header'].end_offset)

    h['osd_only_header'] = CephBlockHeader(o)
    assert(h['osd_only_header'].blength < 0x100000)
    o.seek(h['osd_only_header'].end_offset)  # TODO
    assert(o.tell() == h['osd_only_header'].end_offset)
    h['inc_crc'] = CephInteger(o, 4)
    h['full_crc'] = CephInteger(o, 4)
    assert(o.tell() == h['header'].end_offset)
    # end = o.tell()

    return _format_decode_output(h), h


def decode_osd_super(onode, read):
    """
    Decode an osd_superblock.
    onode: If not None, we read from the physical extents of the object.
           If None, we read from 'read'.
    read: The OSD/open file. If onode is None, we must be at the position
          where the osd_superblock should be read from.

    returns: Tuple of readable output and dictionary of values.
    """
    if onode:
        o = ByteHandler(onode.extract_raw(read))
    else:
        o = read
    h = {}

    # start = o.tell()
    h['header'] = CephBlockHeader(o)
    assert(h['header'].blength < 0x100000)
    h['cluster_fsid'] = CephUUID(o)
    h['whoami'] = CephInteger(o, 4)
    h['current_epoch'] = CephInteger(o, 4)
    h['oldest_map'] = CephInteger(o, 4)
    h['newest_map'] = CephInteger(o, 4)
    h['weight'] = CephFloat(o)
    h['features_mask'] = CephInteger(o, 8)
    h['features'] = CephDict(
        o, CephInteger, 8, CephString, None)
    h['clean_thru'] = CephInteger(o, 4)
    h['mounted'] = CephInteger(o, 4)
    h['osd_fsid'] = CephUUID(o)
    h['last_epoch_marked_full'] = CephInteger(o, 4)
    h['number_pool_last_epoch_marked_full'] = CephInteger(o, 4)
    assert(o.tell() == h['header'].end_offset)
    # end = o.tell()

    return _format_decode_output(h), h


def decode_rbd_id(onode, read):
    """
    Decode an rbd_id.
    onode: If not None, we read from the physical extents of the object.
           If None, we read from 'read'.
    read: The OSD/open file. If onode is None, we must be at the position
          where the rbd_id should be read from.
    """
    if onode:
        o = ByteHandler(onode.extract_raw(read))
    else:
        o = read
    h = {}

    h['rbd_id'] = CephString(o)

    return _format_decode_output(h), h


def decode_mds_inotable(onode, read):
    """
    Decode an mds_inotable.
    onode: If not None, we read from the physical extents of the object.
           If None, we read from 'read'.
    read: The OSD/open file. If onode is None, we must be at the position
          where the osd_superblock should be read from.

    returns: Tuple of readable output and dictionary of values.
    """
    if onode:
        o = ByteHandler(onode.extract_raw(read))
    else:
        o = read
    h = {}

    # start = o.tell()
    h['version'] = CephInteger(o, 8)
    h['header'] = CephBlockHeader(o)
    assert(h['header'].blength < 0x100000)
    h['free'] = CephIntegerPairList(o, 8)
    assert(o.tell() == h['header'].end_offset)
    # end = o.tell()

    return _format_decode_output(h), h


def decode_journal(onode, read):
    """
    Decode an mds_inotable.
    onode: If not None, we read from the physical extents of the object.
           If None, we read from 'read'.
    read: The OSD/open file. If onode is None, we must be at the position
          where the osd_superblock should be read from.

    returns: Tuple of readable output and dictionary of values.
    """
    if onode:
        o = ByteHandler(onode.extract_raw(read))
    else:
        o = read
    h = {}

    count = 0
    while True:
        if o.tell() + 8 + 4 > o.length():
            break
        sentinel = CephInteger(o, 8)
        if sentinel.value != 0x3141592653589793:
            break
        h['sentinel%08x' % count] = sentinel
        h['entry%08x' % count] = CephFSLogEntry(o)
        h['end%08x' % count] = CephInteger(o, 8)
        count += 1
    return _format_decode_output(h), h


def _format_decode_output(data):
    """
    Format readable output.
    """
    ret = ""
    for k, v in sorted(data.items(), key=lambda x: x[1].start):
        ret += _format_line(k, v)
        if isinstance(v, list):
            i = 0
            for e in v:
                ret += _format_line(i, e)
                i += 1
        elif isinstance(v, CephDict):
            for k1, v1 in sorted(v.elements.items(),
                                 key=lambda x: x[1].start):
                ret += _format_line(k1, v1)
    return ret


def _format_line(k, v):
    """
    Format a readable line.
    """
    return "0x%08x: %20s --> %s\n" % (v.start, str(k), str(v))
