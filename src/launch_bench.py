#!/usr/bin/env python
import click
import json
import s3fs
import shutil
import subprocess as sp
from os import makedirs, path as op
from pathlib import PurePath
from random import shuffle
from src import helpers

executable = "src/inc.py"


def clear_bucket(bucket):

    #bucket = op.join(bucket)
    fs = s3fs.S3FileSystem()

    # Get list of files to delete
    files = fs.ls(bucket)

    fs.rm(files)


def launch_command(exp):
    # remove second to last and last element of list if empty
    if exp[-3] == "":
        exp = exp[0:-3] + exp[-2:]
    if exp[-2] == "":
        exp = exp[0:-2] + [exp[-1]]
    if exp[-1] == "":
        exp = exp[0:-1]

    print("Launching command: ", " ".join([el for el in exp]))
    out = sp.run(args=exp, capture_output=True)

    print("STDOUT: ", out.stdout.decode("utf-8"))
    print("STDERR: ", out.stderr.decode("utf-8"))
    print("Command completed")


def gen_benchfile(bucket, it, files, cache, use_dask):
    bench_file = "benchmark_{0}i_{1}f_{2}_{3}_{4}.csv".format(
        it, files, "cache" if cache is True else "nocache", PurePath(bucket).parts[1],
        "dask" if use_dask is True else "sequential" 
    )
    return bench_file


@click.command()
@click.argument("condition_json", type=click.File())
@click.argument("results_fldr", type=str)
@click.option("--repetitions", type=int, default=5, help="number of repetitions to run")
def main(condition_json, results_fldr, repetitions):
    conditions = json.load(condition_json)
    out_bucket = conditions["out_bucket"]
    in_bucket = []
    iterations = []
    n_files = []
    cache = []
    dask = []
    anon = []

    # create current execution results folder and update the results_fldr variable
    name, ext = op.splitext(op.basename(condition_json.name))
    results_fldr = op.join(results_fldr, name)

    n_items = len(conditions["items"])

    for items in conditions["items"]:
        in_bucket.append(items["in_bucket_rgx"])
        iterations.append(items["iterations"])
        n_files.append(items["n_files"])
        cache.append(items["cache"])
        dask.append(items["dask"])
        anon.append(items["anon"])

    exp = [
        [
            "python",
            executable,
            bucket,
            out_bucket,
            "--it",
            str(i),
            "--n_files",
            str(f),
            "--bench_file",
            gen_benchfile(bucket, i, f, c, d),
            "--cache" if c is True else "",
            "--use_dask" if d is True else "",
            "--anon" if anon[x] is True else ""
        ]
        for x in range(n_items)
        for bucket in in_bucket
        for i in iterations[x]
        for f in n_files[x]
        for c in cache[x]
        for d in dask[x]
    ]

    for r in range(repetitions):

        # randomize experiment executions
        shuffle(exp)

        # fix results folder
        rep_fldr = op.join(results_fldr, f"rep-{r}")

        if op.exists(rep_fldr):
            shutil.rmtree(rep_fldr)
        makedirs(rep_fldr)

        # run experiments
        for e in exp:

            # fix benchmark file name
            e[-4] = op.join(rep_fldr, op.basename(e[-4]))

            # Clear cache before launching
            helpers.drop_caches()
            launch_command(e)

            # delete s3 bucket contents post execution
            clear_bucket(out_bucket)


if __name__ == "__main__":
    main()
