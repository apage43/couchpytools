#!/usr/bin/env python
from couchstore import CouchStore, CouchStoreException, DocumentInfo, SizedBuf
import os
from os import listdir
from os.path import isfile, join
from sys import stderr, exit
from math import ceil, floor, log
import pprint
import optparse
import re

parser = optparse.OptionParser(usage="usage: %prog [options] <data directory>")
parser.add_option("-M", "--memory",
                  help="use decompressed (memory) size, otherwise physical (disk) size.",
                  action="store_true")

(args, rest) = parser.parse_args()

sizeHisto = {}

def bucketize(size):
   if(size <= 0):
      return 0
   return floor(log(size, 2))

def process(filename):
    store = CouchStore(filename, 'r')
    for doc_info in store.changesSince(0):
        size = 0
        if args.memory:
            size = len(doc_info.getContents(options = CouchStore.DECOMPRESS))
        if not args.memory:
            size = doc_info.physSize

        if bucketize(size) not in sizeHisto:
           sizeHisto[bucketize(size)] = 0
        sizeHisto[bucketize(size)] += 1

    store.close()

def humanize_bytes(bytes, precision=0):
    abbrevs = (
        (1<<50L, 'PB'),
        (1<<40L, 'TB'),
        (1<<30L, 'GB'),
        (1<<20L, 'MB'),
        (1<<10L, 'kB'),
        (1, 'b')
    )
    if bytes == 1:
        return '1 byte'
    for factor, suffix in abbrevs:
        if bytes >= factor:
            break
    return '%.*f%s' % (precision, bytes / factor, suffix)

def main():
   if len(rest) is not 1:
      parser.print_usage()
      exit(2)
   datadir = rest[0]
   for cfile in listdir(datadir):
       srcfile = join(datadir, cfile)
       m = re.search("\d+\.couch", cfile)
       if m:
          process(srcfile)
   histo = [(k,v) for k,v in sizeHisto.iteritems()]
   histo.sort(key=lambda tup: tup[0])
   for bucket, count in sizeHisto.iteritems():
      print '%s - %s   \t %d' % (humanize_bytes(2 ** bucket), humanize_bytes(2 ** (bucket + 1)), count)

if __name__ == '__main__':
    main()

