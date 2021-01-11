# /usr/bin/env python
import dask
import s3fs
import gzip
import click
import nibabel as nib
import numpy as np
from code.helpers import benchmark, setup_bench
from io import BytesIO
from os import path as op
from time import time_ns


@benchmark
def reads3(fp, anon, cache, **kwargs):

    fs = s3fs.S3FileSystem(anon=anon)
    if cache is True:
        cache_options = {"cache_storage": "/dev/shm"}

        with fs.open(fp, "rb", cache_options=cache_options) as f:
            data = f.read()

    else:
        with fs.open(fp, "rb") as f:
            data = f.read()

    return data


@benchmark
def writes3(out_fp, data, cache, **kwargs):
    fs = s3fs.S3FileSystem()

    if cache is True:
        cache_options = {"cache_storage": "/dev/shm"}
        with fs.open(out_fp, "wb", cache_options=cache_options) as f:
            f.write(data)
    else:
        with fs.open(out_fp, "wb") as f:
            f.write(data)


@benchmark
def read(fp, anon=False, cache=False, **kwargs):

    data = None

    # Cheat way to determine whether to decompress or not
    if ".gz" in fp[-3:]:
        data = gzip.decompress(reads3(fp=fp, anon=anon, cache=cache, **kwargs))
    else:
        data = reads3(fp=fp, anon=anon, cache=cache, **kwargs)

    fh = nib.FileHolder(fileobj=BytesIO(data))

    im = nib.Nifti1Image.from_file_map({"header": fh, "image": fh})
    return im


@benchmark
def increment(im, fp, **kwargs):
    data = np.asanyarray(im.dataobj) + 1
    return nib.Nifti1Image(data, im.affine, im.header)


@benchmark
def write(im, fp, bucket, i, cache=False, clevel=9, **kwargs):

    bio = BytesIO()
    file_map = im.make_file_map({"image": bio, "header": bio})
    im.to_file_map(file_map)

    if ".gz" in fp[-3:]:
        data = gzip.compress(bio.getvalue(), compresslevel=clevel)
    else:
        data = bio.getvalue()

    if i == 0:
        out_fp = op.join(bucket, f"inc_{i}_{op.basename(fp)}")
    else:
        out_fp = op.join(bucket, f"inc_{i}_{'_'.join(op.basename(fp).split('_')[2:])}")

    writes3(out_fp, data=data, cache=cache, fp=fp, **kwargs)
    return out_fp


@click.command()
@click.argument("input_bucket_rgx", type=str)
@click.argument("output_bucket", type=str)
@click.option("--it", type=int, default=1, help="Number of iterations")
@click.option("--cache", is_flag=True, help="enable file caching")
@click.option("--n_files", type=int, default=1, help="Number of files to process")
@click.option(
    "--compression_level",
    type=click.Choice([str(i) for i in range(10)]),
    default="6",
    help="Compression level",
)
@click.option(
    "--bench_file",
    type=str,
    help="file to output benchmark results to. STDOUT otherwise",
)
@click.option("--use_dask", is_flag=True, help="run as a dask pipeline")
def main(
    input_bucket_rgx,
    output_bucket,
    it,
    cache,
    n_files,
    compression_level,
    bench_file,
    use_dask,
):

    # potentially create another decorate or fix the benchmark one for this
    start = time_ns()
    # create new benchmark file
    setup_bench(bench_file)

    makespan_dir = op.dirname(bench_file)
    bench_name = op.basename(bench_file)

    fs = s3fs.S3FileSystem(anon=True)
    all_f = fs.glob(input_bucket_rgx)

    files = all_f[:n_files]
    outfiles = []
    clevel = int(compression_level)

    for fp in files:
        for i in range(it):
            anon = True if i == 0 else False

            if use_dask is True:
                im = dask.delayed(read)(fp=fp, anon=anon, cache=cache, bfile=bench_file)
                inc = dask.delayed(increment)(im=im, fp=fp, bfile=bench_file)
                fp = dask.delayed(write)(
                    im=inc,
                    fp=fp,
                    bucket="vhs-testbucket",
                    i=i,
                    cache=cache,
                    clevel=clevel,
                    bfile=bench_file,
                )
            else:
                im = read(fp=fp, anon=anon, cache=cache, bfile=bench_file)
                inc = increment(im=im, fp=fp, bfile=bench_file)
                fp = write(
                    im=inc,
                    fp=fp,
                    bucket="vhs-testbucket",
                    i=i,
                    cache=cache,
                    clevel=clevel,
                    bfile=bench_file,
                )

        outfiles.append(fp)

    if use_dask is True:
        outfiles = dask.delayed(lambda x: x)(outfiles).compute()

    print(", ".join(outfiles))

    end = time_ns()

    with open(op.join(makespan_dir, "makespan.csv"), "a+") as f:
        f.write(f"{bench_name},{start},{end},{(end-start)*10**-9}\n")


if __name__ == "__main__":
    main()
