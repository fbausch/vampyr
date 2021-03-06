#!/usr/bin/python3

import sys
import getopt
import os
import shutil
import logging
from vampyr.osd import OSD
from vampyr.decoder import decode_osdmap, decode_inc_osdmap, decode_osd_super
from vampyr.kv import RDBKV


def usage():
    print("""
Vampyr (short for Vampyroteuthis infernalis == vampire squid from hell)
is a utility to analyze a Ceph OSD when it is offline.

It may be used to extract and restore data from an OSD
or to analyze metadata that can be found in the KV store
or in other places.

It is not necessary to install Ceph to run Vampyr.

* https://github.com/fbausch/vampyr
* https://github.com/fbausch/vampyr/wiki

Prerequesites:
--------------
* Python 3
* The OSD uses Bluestore with RocksDB KV backend.
* Vampyr needs the tool "ldb" from RocksDB to analyze the KV store.


Usage:
------
vampyr.py --help
vampyr.py --image <OSD> [OPTIONS]

<OSD>         Path to a file containing an image of an unencrypted OSD.
              If the start of the OSD is not at the start of the image
              use --offset.

OPTIONS:
--offset      Offset of the OSD within the file. Dec or hex.
--verbose     More output.
--logging [INFO|DEBUG]
              Turn on info or debug logging.

--ldb <path>  The path of the ldb executable. If not given,
              ldb must be in PATH.

--clear       Clear directories before extracting data to them.

--scan <DIR>  Scans image for known data structures and extracts
              them to the directory.
              Options listed below will be ignored.

--bslabel     Print Bluestore label/superblock information.
--bfssuper    Print information about the BlueFS superblock.
--bfstx       Print information about the BlueFS transaction log.
--xbfs <DIR>  Extract BlueFS content to this directory.
--lspes       List physical extents, allocated and non-allocated.
--xpes <DIR>  Extract unallocated physical extents to this directory.
--analyzepes  Looks at all unallocated areas of the OSD and will check
              how many 512 KiB blocks are actually empty (filles with 0)
              and how many contain data.
--lsobjects   List objects. (See also --objfilter.)
--decobjects  Print decoded objects. (See also --objfilter.)
--objfilter   A regex to filter object names. Only objects matching
              this regex will be extracted/printed/...
--lsbitmap    Show the Bitmap from KV store.
--xbitmap     Extract all blocks that are marked as unallocated in
              KV store bitmap.
--xall <DIR>  Extract all matching objects to this directory.
              This includes slack space and metadata.
--lspgs       List PG information
        """)


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:v",
                                   ["help",
                                    "image=",
                                    "verbose",
                                    "bfssuper",
                                    "bfstx",
                                    "bslabel",
                                    "lspes",
                                    "xpes=",
                                    "analyzepes",
                                    "xbfs=",
                                    "lsobjects",
                                    "decobjects",
                                    "objfilter=",
                                    "lsbitmap",
                                    "xbitmap=",
                                    "xall=",
                                    "clear",
                                    "lspgs",
                                    "scan=",
                                    "offset=",
                                    "logging=",
                                    "ldb="])
    except getopt.GetoptError as exception:
        print(str(exception))
        sys.exit(1)

    osdpath = None
    bfsextract_dest = None
    verbose = False
    offset = 0
    actions = ["bfssuper", "bslabel"]
    actions_o = []
    objectfilter = None
    clearextract = False
    loglevel = None
    for opt, arg in opts:
        if opt in ("-v", "--verbose"):
            verbose = True
        elif opt in ("-h", "--help"):
            usage()
            sys.exit(0)
        elif opt in ("-i", "--image"):
            osdpath = arg
        elif opt in ("--bfssuper"):
            actions_o.append("bfssuper")
        elif opt in ("--bfstx"):
            actions_o.append("bfstx")
        elif opt in ("--bslabel"):
            actions_o.append("bslabel")
        elif opt in ("--xbfs"):
            bfsextract_dest = arg

        elif opt in ("--lspes"):
            actions_o.append("lspes")
        elif opt in ("--xpes"):
            actions_o.append("xpes")
            extractpes = arg
        elif opt in ("--analyzepes"):
            actions_o.append("analyzepes")

        elif opt in ("--lsobjects"):
            actions_o.append("lsobjects")
        elif opt in ("--decobjects"):
            actions_o.append("decobjects")
        elif opt in ("--objfilter"):
            objectfilter = arg

        elif opt in ("--lsbitmap"):
            actions_o.append("lsbitmap")
        elif opt in ("--xbitmap"):
            actions_o.append("xbitmap")
            unallocextract = arg
        elif opt in ("--xall"):
            actions_o.append("xall")
            allextract = arg
        elif opt in ('--clear'):
            clearextract = True
        elif opt in ("--lspgs"):
            actions_o.append("lspgs")
        elif opt in ("--offset"):
            offset = int(arg, 0)

        elif opt in ("--logging"):
            loglevel = arg
        elif opt in ("--scan"):
            actions_o.append("scan")
            scandir = arg

        elif opt in ("--ldb"):
            RDBKV.ldb = arg

        else:
            usage()
            sys.exit(1)
    if len(actions_o) > 0:
        actions = actions_o

    loglvl = logging.WARN
    logfrmt = '[%(levelname)s:%(filename)10s:%(lineno)4s - %(funcName)10s]  %(message)s'
    if loglevel and loglevel in ["INFO", "DEBUG"]:
        if loglevel == "INFO":
            loglvl = logging.INFO
        elif loglevel == "DEBUG":
            loglvl = logging.DEBUG
    elif loglevel:
        usage()
        sys.exit(1)
    logging.basicConfig(format=logfrmt, level=loglvl)

    logging.debug(actions)

    # Scan for osdmap, inc_osdmap and osd_superblock data structures.
    # Does not load BlueFS and the KV store.
    if "scan" in actions:
        if clearextract and os.path.isdir(scandir):
            logging.info("Delete %s" % scandir)
            shutil.rmtree(scandir)
        dirs = [scandir,
                os.path.join(scandir, "osdmap"),
                os.path.join(scandir, "inc_osdmap"),
                os.path.join(scandir, "osd_super")]
        for d in dirs:
            if not os.path.isdir(d):
                os.makedirs(d)
        with OSD(osdpath, startoffset=offset, initkv=False) as osd:
            osdlength = osd.bluestorelabel['osdlength'].value
            osd.seek(0)
            perc = 0
            blength = 0x10000
            block = -1
            while osd.tell() < osdlength - blength:
                block += 1
                pos = blength * block
                osd.seek(pos)
                old_perc = perc
                perc = (pos * 100) // osdlength
                if perc != old_perc:
                    print("Scanned %03d percent of drive." % perc)
                    sys.stdout.flush()
                try:
                    d, r = decode_osdmap(None, osd)
                    epoch = r['epoch'].value
                    out = os.path.join(scandir, "osdmap",
                                       "decoded_%010d" % epoch)
                    with open(out, 'w') as f:
                        f.write("Found at 0x%016x\n\n" % pos)
                        f.write(d)
                    logging.info("Found osdmap at 0x%016x" % pos)
                    block = osd.tell() // blength
                    continue
                except Exception:
                    pass
                osd.seek(pos)
                try:
                    d, r = decode_inc_osdmap(None, osd)
                    epoch = r['epoch'].value
                    out = os.path.join(scandir, "inc_osdmap",
                                       "decoded_%010d" % epoch)
                    with open(out, 'w') as f:
                        f.write("Found at 0x%016x\n\n" % pos)
                        f.write(d)
                    logging.info("Found inc_osdmap at 0x%016x" % pos)
                    block = osd.tell() // blength
                    continue
                except Exception:
                    pass
                osd.seek(pos)
                try:
                    d, r = decode_osd_super(None, osd)
                    epoch = r['current_epoch'].value
                    out = os.path.join(scandir, "osd_super",
                                       "decoded_%010d" % epoch)
                    with open(out, 'w') as f:
                        f.write("Found at 0x%016x\n\n" % pos)
                        f.write(d)
                    logging.info("Found osd_super at 0x%016x" % pos)
                    block = osd.tell() // blength
                    continue
                except Exception:
                    pass

        return

    # All actions that require BlueFS and KV store.
    with OSD(osdpath, startoffset=offset) as osd:
        if "bslabel" in actions:
            osd.bslabel_pretty_print()
            osd.kv.pS.pretty_print()
            osd.kv.pT.pretty_print()

        if "bfssuper" in actions:
            osd.bluefs.superblock.pretty_print()
            osd.bluefs.dump_state(verbose=verbose)
        if "bfstx" in actions:
            osd.bluefs.print_transactions()
            osd.bluefs.print_transactions(skipped=True)

        if bfsextract_dest:
            if not os.path.isdir(bfsextract_dest):
                print("error")
                return
            osd.bluefs.extract_state(bfsextract_dest)

        if "lspes" in actions:
            osd.pextents_pretty_print()
        if "xpes" in actions:
            osd.pextents_extract_unallocated(extractpes)
        if "analyzepes" in actions:
            osd.pextents_analyze_unallocated()

        if "lspgs" in actions:
            osd.kv.pC.pretty_print()

        if "lsobjects" in actions:
            osd.kv.pO.pretty_print(objectfilter)

        if "decobjects" in actions:
            osd.kv.pO.print_decoded(osd, objectfilter)

        if "lsbitmap" in actions:
            osd.kv.pB.pretty_print()
            osd.kv.pb.pretty_print(osd.kv.pB)
        if "xbitmap" in actions:
            osd.kv.pB.extract_unallocated(osd, osd.kv.pB, unallocextract)
        if "xall" in actions:
            print("")
            print("------------")
            print("Extract all:")
            print("------------")
            sys.stdout.flush()
            if os.path.isdir(allextract) and clearextract:
                logging.info("Delete %s" % allextract)
                shutil.rmtree(allextract)
            if not os.path.isdir(allextract):
                logging.info("Create %s" % allextract)
                os.makedirs(allextract)
            print("Please wait...")
            sys.stdout.flush()
            osd.extract_label_slack(allextract)
            osd.bluefs.superblock.extract_slack(allextract)
            osd.kv.pO.decode_object_data(osd, allextract,
                                         osd.kv.pM, objectfilter)
            print("Please wait a bit longer...")
            sys.stdout.flush()
            osd.kv.pM.decode_object_data(osd, allextract,
                                         osd.kv.pO, objectfilter)
            print("Almost done...")
            sys.stdout.flush()
            # osd.kv.pM.decode_object_data(osd, allextract,
            #                              osd.kv.pO, objectfilter)
            osd.kv.pP.decode_object_data(osd, allextract,
                                         osd.kv.pO, objectfilter)

            print("Done")
            sys.stdout.flush()
        if loglevel == "DEBUG":
            # Load all KV store prefixes to make sure that the parsers work:
            print("Debug mode: load the whole KV store...")
            sys.stdout.flush()
            osd.kv.pO
            osd.kv.pS
            osd.kv.pT
            osd.kv.pC
            osd.kv.pM
            osd.kv.pP
            osd.kv.pB
            osd.kv.pb
            osd.kv.pL
            osd.kv.pX


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error("Something went wrong:")
        logging.exception(e)
