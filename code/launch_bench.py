#!/usr/bin/env python
import click
import json
import s3fs
import subprocess as sp
from os import makedirs, path as op
from pathlib import PurePath
from random import shuffle

executable = "./inc.py"


def clear_bucket(bucket):

    bucket = op.join(bucket)
    fs = s3fs.S3FileSystem()

    # Get list of files to delete
    files = fs.ls(bucket)
    fs.rm(files)


def launch_command(exp):
    # remove last element of list if empty
    if exp[-1] == "":
        exp = exp[0:-1]
    print("Launching command: ", " ".join([el for el in exp]))
    out = sp.run(args=exp, capture_output=True)

    print("STDOUT: ", out.stdout.decode("utf-8"))
    print("STDERR: ", out.stderr.decode("utf-8"))
    print("Command completed")


def gen_benchfile(bucket, it, files, cache, results_fldr):
    bench_file = "benchmark_{0}i_{1}f_{2}_{3}".format(
        it, files, "cache" if cache is True else "nocache", PurePath(bucket).parts[1]
    )
    return op.join(results_fldr, bench_file)


@click.command()
@click.argument("condition_json", type=click.File())
@click.argument("results_fldr", type=str)
def main(condition_json, results_fldr):
    conditions = json.load(condition_json)
    out_bucket = conditions["out_bucket"]
    in_bucket = []
    iterations = []
    n_files = []
    cache = []

    # create current execution results folder and update the results_fldr variable
    name, ext = op.splitext(condition_json.name)
    results_fldr = op.join(results_fldr, name)
    makedirs(results_fldr, exist_ok=True)

    n_items = len(conditions["items"])

    for items in conditions["items"]:
        in_bucket.append(items["in_bucket_rgx"])
        iterations.append(items["iterations"])
        n_files.append(items["n_files"])
        cache.append(items["cache"])

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
            gen_benchfile(bucket, i, f, c, results_fldr),
            "--cache" if c is True else "",
        ]
        for x in range(n_items)
        for bucket in in_bucket
        for i in iterations[x]
        for f in n_files[x]
        for c in cache[x]
    ]

    # randomize experiment executions
    shuffle(exp)

    # run experiments
    for e in exp:
        launch_command(e)

        # delete s3 bucket contents post execution
        clear_bucket(out_bucket)


if __name__ == "__main__":
    main()
