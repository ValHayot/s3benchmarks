#!/usr/bin/env python
import gzip
import pytest
import hashlib
import pathlib
import numpy as np
import nibabel as nib
from os import remove
from ..code import measure_compression as mc


class TestCompression:
    @classmethod
    def setup_class(cls):
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

    @classmethod
    def teardown_class(cls):
        remove(cls.uncompressed_file)
        remove(cls.compressed_file)
        remove(cls.recompressed_file)

    def test_compress(self):
        c_data = mc.compress(
            open(self.uncompressed_file, "rb").read(),
            mtime=self.mtime,
            clevel=self.clevel,
        )
        assert _md5(c_data) == self.compressed_md5

    def test_decompress(self):
        d_data = mc.decompress(open(self.compressed_file, "rb").read())
        assert _md5(d_data) == self.uncompressed_md5

    def test_readfile(self):
        d_data = mc.read_file(self.compressed_file)
        assert _md5(d_data) == self.uncompressed_md5

    def test_writefile(self):
        c_fp = mc.write_file(
            self.compressed_file,
            open(self.uncompressed_file, "rb").read(),
            self.mtime,
            self.clevel,
        )

        assert c_fp == self.recompressed_file
        # cannot compare gzipped md5sums as the filenames are different
        assert _md5(gzip.decompress(open(c_fp, "rb").read())) == self.uncompressed_md5


def _md5(data):
    return hashlib.md5(data).hexdigest()
