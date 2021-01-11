#!/usr/bin/env python
import gzip
import hashlib
import nibabel as nib
import numpy as np


def _setup(cls):
    # create image for tests
    cls.uncompressed_file = "randdata.nii"
    cls.compressed_file = "randdata.nii.gz"
    cls.clevel = 9
    cls.mtime = 1610313072
    cls.recompressed_file = f"rgzip-{cls.compressed_file}"

    data = np.random.randint(100, size=(100, 100, 100), dtype=np.uint16)
    img = nib.Nifti1Image(data, np.eye(4))
    nib.save(img, cls.uncompressed_file)

    # not using nibabel.save() here because of issues reproducing md5sum
    # seems to partially be due to a floating point issue when extracting
    # mtime from file
    c_data = gzip.compress(
        open(cls.uncompressed_file, "rb").read(),
        mtime=cls.mtime,
        compresslevel=cls.clevel,
    )
    cls.compressed_md5 = _md5(c_data)

    with open(cls.compressed_file, "wb") as f:
        f.write(c_data)

    cls.uncompressed_md5 = _md5(open(cls.uncompressed_file, "rb").read())

def _md5(data):
    return hashlib.md5(data).hexdigest()
