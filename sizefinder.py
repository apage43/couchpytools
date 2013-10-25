#!/usr/bin/env python
from couchstore import CouchStore, CouchStoreException, DocumentInfo, SizedBuf
import os
from os import listdir
from os.path import isfile, join
from sys import stderr
import optparse
import re

parser = optparse.OptionParser(usage="usage: %prog [options] <data directory>")
parser.add_option("-M", "--memory", help="use decompressed (memory) size, otherwise physical (disk) size.", action="store_true")
parser.add_option("-t", "--threshold", default = 0, type=int, help="threshold in bytes")

(args, rest) = parser.parse_args()

def process(filename):
    store = CouchStore(filename, 'r')
    for doc_info in store.changesSince(0):
        size = 0
        if args.memory:
            size = len(doc_info.getContents(options = CouchStore.DECOMPRESS))
        else:
            size = doc_info.physSize
        if size >= args.threshold:
            print doc_info.id
    store.close()

def main():
   print >> stderr, "Listing items >=", args.threshold, "bytes"
   if len(rest) is not 1:
      parser.print_usage()
      exit(2)
   datadir = rest[0]
   for cfile in listdir(datadir):
       srcfile = join(datadir, cfile)
       m = re.search("\d+\.couch", cfile)
       if m:
          process(srcfile)

if __name__ == '__main__':
    main()

