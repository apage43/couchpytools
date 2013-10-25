Make sure the Couchstore library is on the linker path

    export LD_LIBRARY_PATH=/opt/couchbase/lib

## histogram

    ./histor.py /path/to/data/dir

## items over N bytes

    ./sizefinder.py -t N /path/to/data/dir

Both programs take an optional flag `-M` which will cause them to read
and decompress items to get their size in memory rather than their disk
size.
