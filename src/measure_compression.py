#!/usr/bin/env python
import click
import gzip
import numpy
import hashlib
import pathlib
import nibabel as nib
from src.helpers import setup_bench, benchmark, drop_caches
from os import path as op, utime, remove


@benchmark
def compress(data, mtime, clevel=9, **kwargs):
    return gzip.compress(data, mtime=mtime, compresslevel=clevel)


@benchmark
def decompress(gz_data, **kwargs):
    return gzip.decompress(gz_data)


def write_file(fp, data, mtime, clevel=9, bfile=None):
    fp = op.join(op.dirname(fp), f"rgzip-{op.basename(fp)}")

    gz_data = compress(data, fp=fp, bfile=bfile, mtime=mtime, clevel=clevel)
    with open(fp, "wb") as f:
        f.write(gz_data)

    utime(fp, (mtime, mtime))

    return fp


def read_file(fp, bfile=None):
    with open(fp, "rb") as f:
        gz_data = f.read()
        data = decompress(gz_data, fp=fp, bfile=bfile)
    return data


@click.command()
@click.argument("filename", type=str)
@click.option("--repetitions", type=int, default=5, help="Number of repetitions")
@click.option(
    "--compression_level",
    type=click.Choice(range(0, 10)),
    default=6,
    help="Compression level (as per gzip documentation)",
)
@click.option(
    "--benchmark_file",
    type=str,
    default="gzip-benchmarks.csv",
    help="Filename where to store benchmarks to",
)
def main(filename, repetitions, benchmark_file, compression_level):

    setup_bench(benchmark_file)

    # to consistently reproduce compression hash
    mtime = pathlib.Path(filename).stat().st_mtime

    for i in range(repetitions):
        drop_caches()
        data = read_file(filename, bfile=benchmark_file)
        gz_fn = write_file(
            filename, data, bfile=benchmark_file, mtime=mtime, clevel=compression_level
        )

        print("Compressed output file: ", gz_fn)

        # Cleanup
        remove(gz_fn)


if __name__ == "__main__":
    main()
