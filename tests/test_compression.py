#!/usr/bin/env python
import gzip
import pytest
import pathlib
import numpy as np
import nibabel as nib
from os import remove
from .utils import _md5, _setup
from ..src import measure_compression as mc


class TestCompression:
    @classmethod
    def setup_class(cls):
        _setup(cls)

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


