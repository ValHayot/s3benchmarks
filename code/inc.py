import s3fs
import gzip
import click
import nibabel as nib
import numpy as np
from io import BytesIO
from functools import wraps
from time import time
from os import path as op, getpid

b_file = "benchmark-singlefile.txt"

# decorator
def benchmark(file):
    def param_bench(func):
        @wraps(func)
        def _benchmark(*args, **kwargs):
            start = time()
            try:
                return func(*args, **kwargs)
            finally:
                with open(file, "w+") as f:
                    f.write(
                        f"{func.__name__}_start,{kwargs['fp']},{start},{getpid()}\n"
                    )
                    f.write(f"{func.__name__}_end,{kwargs['fp']},{time()},{getpid()}\n")

        return _benchmark

    return param_bench


@benchmark(file=b_file)
def reads3(fp, anon):
    fs = s3fs.S3FileSystem(anon=anon)
    with fs.open(fp, "rb") as f:
        im = gzip.open(f).read()
    return im


@benchmark(file=b_file)
def writes3(fp, data):
    fs = s3fs.S3FileSystem()
    with fs.open(fp, "wb") as f:
        f.write(data)


@benchmark(file=b_file)
def read(fp, anon=False):
    fh = nib.FileHolder(fileobj=BytesIO(reads3(fp=fp, anon=anon)))
    im = nib.Nifti1Image.from_file_map({"header": fh, "image": fh})
    return im


@benchmark(file=b_file)
def increment(im, fp):
    data = np.asanyarray(im.dataobj) + 1
    return nib.Nifti1Image(data, im.affine, im.header)


@benchmark(file=b_file)
def write(im, fp, bucket, i):

    bio = BytesIO()
    file_map = im.make_file_map({"image": bio, "header": bio})
    im.to_file_map(file_map)
    data = gzip.compress(bio.getvalue())

    if i == 0:
        out_fp = op.join(bucket, f"inc_{i}_{op.basename(fp)}")
    else:
        out_fp = op.join(bucket, f"inc_{i}_{'_'.join(op.basename(fp).split('_')[2:])}")

    writes3(fp=out_fp, data=data)
    return out_fp

@click.command()
@click.option('--it', type=int, default=1, help="Number of iterations")
@click.option('--cache', default=False, help="enable file caching")
def main(it, cache):
    fs = s3fs.S3FileSystem(anon=True)
    all_f = fs.glob("openneuro.org/ds000113/sub-*/ses-forrestgump/dwi/*dwi.nii.gz")

    fp = all_f[1]

    for i in range(it):

        anon = True if i == 0 else False

        im = read(fp=fp, anon=anon)
        inc = increment(im=im, fp=fp)
        fp = write(im=inc, fp=fp, bucket="vhs-testbucket", i=i)
    print(fp)

if __name__ == "__main__":
    main()
