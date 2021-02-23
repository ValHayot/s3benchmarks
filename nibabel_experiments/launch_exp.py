#!/usr/bin/env python
import os
import s3fs
import inspect
import subprocess as sp
from os import getpid
from pathlib import Path
from time import perf_counter, strftime
from functools import wraps
from random import shuffle
from nibabel.streamlines import S3TrkFile, TrkFile

ex_path = "s3://hydi-tractography/hydi_tracks.12_58_7.trk"
tmpfs = "/dev/shm"
disk = "/home/ec2-user/"
reps = 5
pf_fn = "prefetch.log"

def setup_bench(file=None):
    try:
        with open(file, "w+") as f:
            f.write("action,file,timestamp,repetition\n")
    except Exception as e:
        out_string = f"""
                     Action\t\tFile\t\t\t\tTimestamp\t\tRepetition
                     {'-'*100}
                     """
        print(inspect.cleandoc(out_string))


# decorator
def benchmark(func):
    @wraps(func)
    def _benchmark(*args, **kwargs):
        start = perf_counter()

        try:
            return func(*args, **kwargs)
        finally:
            end = perf_counter()
            try:
                with open(kwargs["bfile"], "a+") as f:
                    f.write(
                        f"{kwargs['fname']},{end - start},{kwargs['rep']}\n"
                    )
            except Exception as e:
                out_string = f"""
                {kwargs['fname']}\t{(end-start)}\t{kwargs['rep']}
                """
                print(inspect.cleandoc(out_string))

    return _benchmark


def drop_caches():
    print("** DROPPING CACHES **")
    out = sp.run(
        "echo 3 | sudo tee /proc/sys/vm/drop_caches", capture_output=True, shell=True
    )
    print("STDOUT: ", out.stdout.decode("UTF-8"), end="")
    print("STDERR: ", out.stderr.decode("UTF-8"))
    print("** DROPPING CACHES COMPLETED **")


@benchmark
def read_trk(f, lazy, fetch=False, **kwargs):
    if fetch:

        streamlines = S3TrkFile.load(f,lazy_load=lazy, caches=kwargs["caches"], prefetch_size=kwargs["prefetch_size"]).streamlines

    else:
        streamlines = TrkFile.load(f,lazy_load=lazy).streamlines

    for sl in streamlines:
        s = sl

@benchmark
def copy_local(filename, mem_path, **kwargs):
    fs = s3fs.S3FileSystem()
    fs.get(filename, mem_path)


def cleanup(fn_prefix):
    for fn in Path(tmpfs).glob(".*" + fn_prefix + "*"):
        fn.unlink()
    for fn in Path(disk).glob(".*" + fn_prefix + "*"):
        fn.unlink()

def read_local(filename, path, lazy=True, bfile=None, rep=0, fname="read_me"):

    drop_caches()
    local_path = os.path.join(path, os.path.basename(filename))
    copy_local(filename, local_path, fname=fname.replace("read", f"copy"), bfile=bfile, rep=rep)

    with open(local_path, 'rb') as f:
        read_trk(f, lazy, fname=fname, bfile=bfile, rep=rep)

    cleanup(os.path.basename(filename))


def read_pf(filename, lazy=True, caches={"/dev/shm": 7*1024}, prefetch_size=32*1024**2, bfile=None, rep=0, fname="read-pf"):
    drop_caches()

    fs = s3fs.S3FileSystem()
    with fs.open(filename, 'rb') as f:
        read_trk(f, lazy, prefetch_size=prefetch_size, caches=caches, fetch=True, fname=fname, bfile=bfile, rep=rep)


    if bfile is not None:
        mb=fname.split("_")[-1]
        os.rename(pf_fn, bfile.replace("benchmarks.out", f"rep{rep}-{pf_fn}-{mb}"))

    cleanup(os.path.basename(filename))


def read_s3(filename, lazy=True, bfile=None, rep=0, fname="read_s3", **s3_kwargs):
    drop_caches()

    fs = s3fs.S3FileSystem()
    with fs.open(filename, 'rb', **s3_kwargs) as f:
        read_trk(f, lazy, fname=fname, bfile=bfile, rep=rep)

    cleanup(os.path.basename(filename))


def read_all():
    bfile = os.path.join("results", f"read-all-{strftime('%Y%m%d-%H%M%S')}-benchmarks.out")

    experiments = ["mem", "disk", "pf", "s3"]
    shuffle(experiments)

    for i in range(reps):

        for e in experiments:
            if e == "mem":
                read_local(ex_path, path=tmpfs, bfile=bfile, rep=i, fname="read_me")
            elif e == "disk":
                read_local(ex_path, path=disk, bfile=bfile, rep=i, fname="read_di")
            elif e == "pf":
                read_pf(ex_path, bfile=bfile, rep=i)
            else:
                read_s3(ex_path, bfile=bfile, rep=i)

def block_size():
    bfile = os.path.join("results", f"block_size-{strftime('%Y%m%d-%H%M%S')}-benchmarks.out")
    experiments = [(2**i)*1024**2 for i in range(1, 11)]
    shuffle(experiments)

    for i in range(reps):
        for e in experiments:
            mb = int(e/1024**2)
            print(f"Running experiment read_pf_{mb}")
            read_pf(ex_path, prefetch_size=e, bfile=bfile, rep=i, fname=f"read_pf_{mb}")
            print(f"Experiment read_pf_{mb} completed")

def s3block_size():
    bfile = os.path.join("results", f"s3block_size-{strftime('%Y%m%d-%H%M%S')}-benchmarks.out")
    experiments = [(2**i)*1024**2 for i in range(1, 11)]
    shuffle(experiments)

    for i in range(reps):
        for e in experiments:
            mb = int(e/1024**2)
            print(f"Running experiment read_s3_{mb}")
            read_s3(ex_path, bfile=bfile, rep=i, fname=f"read_s3_{mb}", block_size=e)
            print(f"Experiment read_s3_{mb} completed")

if __name__=="__main__":
    #read_all()
    #block_size()
    s3block_size()




    




