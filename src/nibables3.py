#!/usr/bin/env python
import sys
import s3fs
import nibabel as nib
from memory_profiler import profile

#@profile
def main():
    if "s3://" in sys.argv[1]:
        fs = s3fs.S3FileSystem()

        with fs.open(sys.argv[1]) as f:
            trk = nib.streamlines.load(f)
            print(trk.header)
    else:
        trk = nib.streamlines.load(sys.argv[1])
        hdr = trk.streamlines
        print(hdr[0])

if __name__=="__main__":
    main()
