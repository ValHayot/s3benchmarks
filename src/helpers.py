#!/usr/bin/env python
import inspect
import subprocess as sp
from os import getpid
from time import time_ns
from functools import wraps


def setup_bench(file=None):
    try:
        with open(file, "w+") as f:
            f.write("action,file,timestamp,pid,runtime\n")
    except Exception as e:
        out_string = f"""
                     Action\t\tFile\t\t\t\tTimestamp\t\tPID\t\tRuntime
                     {'-'*100}
                     """
        print(inspect.cleandoc(out_string))


# decorator
def benchmark(func):
    @wraps(func)
    def _benchmark(*args, **kwargs):
        start = time_ns()

        if "fp" not in kwargs:
            kwargs["fp"] = ""

        try:
            return func(*args, **kwargs)
        finally:
            end = time_ns()
            try:
                with open(kwargs["bfile"], "a+") as f:
                    f.write(
                        f"{func.__name__}_start,{kwargs['fp']},{start},{getpid()},\n"
                    )
                    f.write(
                        f"{func.__name__}_end,{kwargs['fp']},{end},{getpid()},{(end-start)*10**-9}\n"
                    )
            except Exception as e:
                out_string = f"""
                {func.__name__}_start\t{kwargs['fp']}\t{start}\t{getpid()}
                {func.__name__}_end\t{kwargs['fp']}\t{end}\t{getpid()}\t{(end-start)*10**-9}
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
