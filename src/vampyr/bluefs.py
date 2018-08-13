"""
Module to deal with BlueFS. Locates the superblock, reads the transaction log
and extracts files from BlueFS.
"""

import os
import shutil
from vampyr.datatypes import CephDataType, CephBlockHeader, CephUUID,\
    CephInteger, CephUnknown, CephVarInteger, CephLBA, CephList, CephUTime,\
    CephString, CephVarIntegerLowz
from vampyr.exceptions import VampyrMagicException
import logging
import hashlib


class BlueFS(object):
    """
    Contains the information about the BlueFS.
    """
    def __init__(self, osd):
        """
        osd: The file handle where to read the data from.
        """
        self.initialized = False
        self.allocated_areas = {}
        self.allocated_ino = []
        self.deallocated_ino = []
        self.dirlist = []
        self.allocated_extents = []
        self.deallocated_extents = []
        self.ino_to_file_map = {}

        self.osd = osd

        o = osd
        self.superblock = BlueFSSuperblock(o)
        self.ino_to_file_map[self.superblock.data['log_fnode'].ino] =\
            BlueFSFile(self.superblock.data['log_fnode'].ino,
                       self.superblock.data['log_fnode'].size,
                       self.superblock.data['log_fnode'].mtime,
                       self.superblock.data['log_fnode'].extents)
        self.transactions = {}
        self.skipped_transactions = {}

        self.next_offset = 0

        self.blocksize = self.superblock.data['block_size'].value

        extents = self.superblock.data['log_fnode'].extents
        oldextents = []
        assert(len(extents) > 0)
        logical_offset = 0
        while len(extents) > 0:
            oldextents += extents
            logical_offset = self.read_bluefs_extents(extents, logical_offset)
            newextents = self.get_file(1).extents
            extents = [x for x in newextents if x not in oldextents]
        assert(self._validate_extent_location())

    def _validate_extent_location(self):
        """
        An exent should never be outside of the allocated BlueFS areas.
        """
        for e in self.allocated_extents:
            match = False
            for a in self.allocated_areas.values():
                if e.offset >= a[0] and e.offset + e.length <= a[0] + a[1]:
                    match = True
            if not match:
                return False
        return True

    def read_bluefs_extents(self, extents, logical_offset):
        for e in extents:
            block = 0
            fnodesize = e.length
            offset = e.offset
            # self.next_offset = 0
            while block * self.blocksize < fnodesize:
                try:
                    thisoff = offset + block * self.blocksize
                    if self.next_offset > logical_offset:
                        self.read_bluefs_transaction(thisoff, skip=True)
                    else:
                        self.next_offset = 0
                        self.read_bluefs_transaction(thisoff)
                except (VampyrMagicException, AssertionError) as e:
                    # print(traceback.format_exc())
                    pass
                block += 1
                logical_offset += self.blocksize
        return logical_offset

    def read_bluefs_transaction(self, seek, skip=False):
        h = {}
        handle = self.osd
        handle.seek(seek)
        h['header'] = CephBlockHeader(handle)
        assert(h['header'].blength <= self.blocksize)
        h['uuid'] = CephUUID(handle)
        assert(h['uuid'] == self.superblock.data['uuid'])
        h['seq'] = CephInteger(handle, 8).value
        h['transaction'] = BlueFSTransaction(handle, self)
        h['crc'] = CephInteger(handle, 4)
        assert(handle.tell() == h['header'].end_offset)
        h['unknown'] = CephUnknown(handle, 0x10)

        if not skip:
            self.transactions[seek] = h
        else:
            self.skipped_transactions[seek] = h

    def op_init(self):
        self.initialized = True

    def op_alloc_add(self, id, offset, length):
        assert(self.initialized)
        self.allocated_areas[id] = (offset, length)

    def op_alloc_rm(self, id, offset, length):
        assert(self.initialized)
        raise NotImplementedError()

    def op_dir_create(self, dirname):
        assert(self.initialized)
        if dirname not in self.dirlist:
            self.dirlist.append(BlueFSDir(dirname))

    def op_dir_remove(self, dirname):
        assert(self.initialized)
        raise NotImplementedError()

    def op_dir_link(self, dirname, filename, ino):
        assert(self.initialized)
        for bfsdir in self.dirlist:
            if bfsdir.dirname == dirname:
                bfsdir.link(filename, ino)

    def op_dir_unlink(self, dirname, filename):
        assert(self.initialized)
        for bfsdir in self.dirlist:
            if bfsdir.dirname == dirname:
                bfsdir.unlink(filename)

    def op_file_update(self, ino, size, mtime, extents):
        assert(self.initialized)
        if ino not in self.allocated_ino:
            self.allocated_ino.append(ino)
        if ino in self.deallocated_ino:
            self.deallocated_ino.remove(ino)
        for e in extents:
            if e not in self.allocated_extents:
                self.allocated_extents.append(e)
            if e in self.deallocated_extents:
                self.deallocated_extents.remove(e)
        if ino not in self.ino_to_file_map:
            self.ino_to_file_map[ino] = BlueFSFile(ino, size, mtime, extents)
        else:
            self.ino_to_file_map[ino].update(size, mtime, extents)

    def op_file_remove(self, ino):
        assert(self.initialized)
        if ino in self.allocated_ino:
            self.allocated_ino.remove(ino)
        if ino not in self.deallocated_ino:
            self.deallocated_ino.append(ino)
        assert(ino in self.ino_to_file_map)
        f = self.ino_to_file_map[ino]
        del self.ino_to_file_map[ino]
        for e in f.extents:
            if e in self.allocated_extents:
                self.allocated_extents.remove(e)
            if e not in self.deallocated_extents:
                self.deallocated_extents.append(e)

    def op_jump(self, next_seq, offset):
        assert(self.initialized)
        if self.next_offset == 0:
            self.next_offset = offset
        else:
            logging.debug("There still is an offset. Not jumping.")

    def op_jump_seq(self):
        assert(self.initialized)
        raise NotImplementedError()

    def get_file(self, ino):
        return self.ino_to_file_map[ino]

    def dump_state(self, verbose=False):
        print("----------------")
        print("State of BlueFS:")
        print("----------------")
        print("Allocated areas:")
        for mid, alloc in self.allocated_areas.items():
            print("Alloc %d: 0x%x+0x%x" % (mid, alloc[0], alloc[1]))
        print("----------------")
        print("Transaction log:")
        print("Transaction log --> %s" % self.ino_to_file_map[1])
        print("----------------")
        print("Files:")
        for d in self.dirlist:
            for ino, filename in sorted(d.ino_to_file_map.items(),
                                        key=lambda x: x[0]):
                o = "file %s/%s --> ino %d" % (d.dirname, filename, ino)
                if ino in self.ino_to_file_map:
                    file = self.ino_to_file_map[ino]
                    print("%s --> %s" % (o, file))
                else:
                    print(o)
        print("----------------")
        if verbose:
            print("Allocated extents:")
            for e in sorted(self.allocated_extents, key=lambda x: x.offset):
                print(e)
            print("----------------")
            print("Deallocated extents:")
            for e in sorted(self.deallocated_extents, key=lambda x: x.offset):
                print(e)
            print("----------------")
        print("")

    def print_transactions(self, skipped=False):
        transactions = self.transactions
        header = "BlueFS Transaction List:"
        if skipped:
            transactions = self.skipped_transactions
            header = "BlueFS Skipped Transaction List:"
        print("-" * len(header))
        print(header)
        print("-" * len(header))
        for t in sorted(transactions.items(), key=lambda x: x[1]['seq']):
            print("0x%016x -> seq: 0x%08x: %s" %
                  (t[0], t[1]['seq'], t[1]['transaction']))
        print("")

    def extract_state(self, destination):
        for d in self.dirlist:
            d.mkdir(destination)
        self.superblock.extract_slack(destination)
        for ino, file in self.ino_to_file_map.items():
            filename = None
            for d in self.dirlist:
                filename = d.get_file(ino)
                if filename:
                    file.mkfile(filename, d.dirname, destination, self.osd)


class BlueFSDir(object):
    def __init__(self, dirname):
        self.dirname = dirname
        self.ino_to_file_map = {}

    def link(self, filename, ino):
        self.ino_to_file_map[ino] = filename

    def unlink(self, filename):
        rm = None
        for ino in self.ino_to_file_map.keys():
            if self.ino_to_file_map[ino] == filename:
                rm = ino
                break
        if rm:
            del self.ino_to_file_map[rm]

        assert(filename not in self.ino_to_file_map.values())

    def list_files(self):
        for ino, filename in self.ino_to_file_map.items():
            print("file %s/%s --> ino %d" %
                  (self.dirname, filename, ino))

    def mkdir(self, destination):
        assert(os.path.isdir(destination))
        destinationfull = os.path.join(destination, self.dirname)
        if os.path.isdir(destinationfull):
            shutil.rmtree(destinationfull)
        assert(not os.path.isdir(destinationfull))
        os.makedirs(destinationfull)
        assert(os.path.isdir(destinationfull))

    def get_file(self, ino):
        rc = None
        if ino in self.ino_to_file_map:
            rc = self.ino_to_file_map[ino]
        return rc


class BlueFSFile(object):
    def __init__(self, ino, size, mtime, extents):
        self.ino = ino
        self.size = size
        self.mtime = mtime
        self.extents = extents

    def update(self, size, mtime, extents):
        self.size = size
        self.mtime = mtime
        self.extents = extents

    def __str__(self):
        extents = " | ".join(str(e) for e in self.extents)
        return "ino: %d, size: %d, mtime: %s, extents: %s" % \
               (self.ino, self.size, self.mtime, extents)

    def mkfile(self, filename, dirname, destination, osd):
        logging.info("Make file %s" % filename)
        destinationfull = os.path.join(destination, dirname)
        assert(os.path.isdir(destinationfull))
        filenamefull = os.path.join(destinationfull, filename)
        filenameslack = os.path.join(destinationfull, "%s_slack" % filename)
        filenamemd5 = os.path.join(destinationfull, "%s.md5" % filename)
        assert(not os.path.exists(filenamefull))
        logging.debug("create %s" % filenamefull)
        logging.debug("- size:  %d (0x%x)" % (self.size, self.size))
        logging.debug("- mtime: %s" % str(self.mtime))

        bsize = 0
        if self.size:
            bsize = self.size

        md5sum = hashlib.md5()

        o = osd
        with open(filenamefull, 'wb') as f, open(filenameslack, 'wb') as s:
            for e in self.extents:
                o.seek(e.offset)
                length = e.length
                logging.info("read extent %s, bsize: 0x%x" % (str(e), bsize))
                if bsize == 0:
                    break
                elif bsize <= length:
                    chunk = o.read(bsize)
                    md5sum.update(chunk)
                    f.write(chunk)
                    slack = length - bsize
                    s.write(o.read(slack))
                    bsize = 0
                else:
                    chunk = o.read(length)
                    md5sum.update(chunk)
                    f.write(chunk)
                    bsize = bsize - length
        with open(filenamemd5, 'w') as f:
            f.write(md5sum.hexdigest())
            f.write('\n')
        assert(bsize == 0)

        ts = self.mtime.timestamp
        os.utime(filenamefull, (ts, ts))


class BlueFSSuperblock(object):
    def __init__(self, handle):
        h = {}
        handle.seek(0x1000)  # Superblock starts at 0x1000
        self.start = handle.tell()
        h['header'] = CephBlockHeader(handle)
        h['uuid'] = CephUUID(handle)
        h['osd_uuid'] = CephUUID(handle)
        h['version'] = CephInteger(handle, 8)
        h['block_size'] = CephInteger(handle, 4)
        h['log_fnode'] = BlueFSFNode(handle)
        assert(handle.tell() == h['header'].end_offset)
        self.end = handle.tell()
        h['crc'] = CephInteger(handle, 4).value
        self.data = h

        slack_length = 0x2000 - handle.tell()  # Reserved block to 8k offset
        assert(slack_length >= 0)
        self.superblock_slack = handle.read(slack_length)

    def pretty_print(self):
        print("------------------------------")
        print("BlueFS Superblock Information:")
        print("------------------------------")
        print("Start at: %s" % hex(self.start))
        print("End at:   %s" % hex(self.end))
        print("------------------------------")

        print("BlueFS UUID: %s" % str(self.data['uuid']))
        print("OSD UUID:    %s" % str(self.data['osd_uuid']))
        print("Version: %d" % self.data['version'].value)
        print("Block size: %s" % hex(self.data['block_size'].value))
        print("Log fnode Information:")
        f = self.data['log_fnode']
        print("- ino: %d" % f.ino)
        print("- size: %d, %s" % (f.size, hex(f.size)))
        print("- mtime: %s" % str(f.mtime))
        print("- prefer block device: %d" % f.prefer_bdev)
        print("- extents:")
        for e in f.extents:
            print("  - %s" % (str(e)))
            print("    (at ~ %d GiB offset)" % (e.offset / 2014**3))
        print("CRC32 checksum: 0x%08x" % self.data['crc'])

        print("------------------------------")
        print("")

    def extract_slack(self, edir):
        slackfile = os.path.join(edir, "slack_bfssuperblock")
        with open(slackfile, 'wb') as s:
            s.write(self.superblock_slack)


class BlueFSFNode(CephDataType):
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.ino = CephVarInteger(handle).value
        self.size = CephVarInteger(handle).value
        self.mtime = CephUTime(handle)
        self.prefer_bdev = CephInteger(handle, 1).value
        self.extents = CephList(handle, BlueFSExtent, None).elements
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        offsets = ", ".join("0x%x+0x%x" %
                            (x.offset, x.length)
                            for x in self.extents)
        return "ino: %d, size: %d, mtime: %s, extents: %s" % \
               (self.ino, self.size, self.mtime, offsets)


class BlueFSExtent(CephDataType):
    def __init__(self, handle):
        start = handle.tell()
        self.header = CephBlockHeader(handle)
        self.offset = CephLBA(handle).value
        self.length = CephVarIntegerLowz(handle).value
        self.bdev = CephInteger(handle, 1).value
        end = handle.tell()
        assert(end == self.header.end_offset)
        super().__init__(start, end)

    def __str__(self):
        return "0x%x+0x%x (bdev %d)" % \
               (self.offset, self.length, self.bdev)

    def __eq__(self, other):
        return self.offset == other.offset


class BlueFSTransaction(CephDataType):
    def __init__(self, handle, bluefs):
        start = handle.tell()
        self.operations = []
        self.structlen = CephInteger(handle, 4).value
        end_offset = handle.tell() + self.structlen
        while handle.tell() < end_offset:
            t = BlueFSOperation(handle, bluefs)
            self.operations.append(t)
        end = handle.tell()
        assert(end == end_offset)
        super().__init__(start, end)

    def __str__(self):
        return " | ".join(str(x) for x in self.operations)


class BlueFSOperation(CephDataType):
    def __init__(self, handle, bluefs):
        start = handle.tell()
        self.op = BlueFSOperationCode(handle)
        op = self.op.value

        if op == 0:
            self.hint = "none"
        elif op == 1:
            self.hint = "init"
            bluefs.op_init()
        elif op == 2 or op == 3:
            # ALLOC_ADD
            # ALLOC_RM
            self.id = CephInteger(handle, 1).value
            self.offset = CephInteger(handle, 8).value
            self.length = CephInteger(handle, 8).value
            self.hint = "id: 0x%x, offset: 0x%x, length: 0x%x" % \
                (self.id, self.offset, self.length)
            if op == 2:
                bluefs.op_alloc_add(self.id, self.offset,
                                    self.length)
            else:
                bluefs.op_alloc_rm(self.id, self.offset,
                                   self.length)
        elif op == 4:
            # DIR_LINK
            self.dir = CephString(handle).value
            self.file = CephString(handle).value
            self.ino = CephInteger(handle, 8).value
            self.hint = "dir: %s, file: %s, ino: %d" % \
                (self.dir, self.file, self.ino)
            bluefs.op_dir_link(self.dir, self.file, self.ino)
        elif op == 5:
            # DIR_UNLINK
            self.dir = CephString(handle).value
            self.file = CephString(handle).value
            self.hint = "dir: %s, file: %s" % \
                (self.dir, self.file)
            bluefs.op_dir_unlink(self.dir, self.file)
        elif op == 6 or op == 7:
            # DIR_CREATE
            # DIR_REMOVE
            self.dir = CephString(handle).value
            self.hint = "dir: %s" % self.dir
            if op == 6:
                bluefs.op_dir_create(self.dir)
            else:
                bluefs.op_dir_remove(self.dir)
        elif op == 8:
            # FILE_UPDATE
            self.file = BlueFSFNode(handle)
            self.hint = str(self.file)
            bluefs.op_file_update(self.file.ino, self.file.size,
                                  self.file.mtime, self.file.extents)
        elif op == 9:
            # FILE_REMOVE
            self.ino = CephInteger(handle, 8).value
            self.hint = "ino: %d" % self.ino
            bluefs.op_file_remove(self.ino)
        elif op == 10:
            # JUMP
            self.next_seq = CephInteger(handle, 8).value
            self.offset = CephInteger(handle, 8).value
            self.hint = "next_seq: %d, offset: 0x%x" % \
                (self.next_seq, self.offset)
            bluefs.op_jump(self.next_seq, self.offset)
        elif op == 11:
            # JUMP_SEQ
            self.next_seq = CephInteger(handle, 8).value
            self.hint = "next_seq: %d" % self.next_seq
            bluefs.op_jump_seq()

        end = handle.tell()
        super().__init__(start, end)

    def __str__(self):
        return "OP: %s, %s" % (str(self.op), self.hint)


class BlueFSOperationCode(CephInteger):
    translation_map = ["NONE", "INIT", "ALLOC_ADD", "ALLOC_RM",
                       "DIR_LINK", "DIR_UNLINK", "DIR_CREATE",
                       "DIR_REMOVE", "FILE_UPDATE", "FILE_REMOVE",
                       "JUMP", "JUMP_SEQ"]

    def __init__(self, handle):
        super().__init__(handle, 1)

    def __str__(self):
        return BlueFSOperationCode.translation_map[self.value]
