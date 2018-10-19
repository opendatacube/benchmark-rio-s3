import numpy as np
import hashlib
import sys
from types import SimpleNamespace
from . import pprio_bench
from .reports import gen_stats_report


def find_next_available_file(fname_pattern, max_n=1000, start=1):
    """
    :param str fname_pattern: File name pattern using "%d" style formatting e.g. "result-%03d.png"
    :param int max_n: Check at most that many files before giving up and returning None
    :param int start: Where to start counting from, default is 1
    """
    from pathlib import Path

    for i in range(start, max_n):
        fname = fname_pattern % i
        if not Path(fname).exists():
            return fname

    return None


def mk_fname(params, ext='pickle', prefix=None):
    if prefix is None:
        prefix = 'results'

    fmt = ('{prefix}_{p.block[0]:d}_{p.block[1]:d}B{p.band}'
           '__{p.nthreads:02d}_%03d.{ext}').format(prefix=prefix,
                                                   p=params,
                                                   ext=ext)

    return find_next_available_file(fmt)


def slurp_lines(fname, *args, **kwargs):
    if len(args) > 0 or len(kwargs) > 0:
        fname = fname.format(*args, **kwargs)

    def slurp(f):
        return [s.rstrip() for s in f.readlines()]

    if fname == '-':
        return slurp(sys.stdin)

    with open(fname, 'rt') as f:
        return slurp(f)


def array_digest(a):
    return hashlib.sha256(a.tobytes('C')).hexdigest()


def npz_data_hash(fname, varname=None):
    f = np.load(fname)

    if len(f.files) == 1 and varname is None:
        varname = f.files[0]

    if varname is not None:
        if varname not in f:
            return None
        return array_digest(f[varname])

    return {k: array_digest(f[k]) for k in f}


def update_params(pp, **kwargs):
    from copy import copy
    pp = copy(pp)
    for k, v in kwargs.items():
        if hasattr(pp, k):
            setattr(pp, k, v)
        else:
            raise ValueError("No such parameter: '{}'".format(k))
    return pp


def run_main(file_list_file,
             nthreads,
             prefix='RIO',
             mode='rio',
             ssl=False,
             wmore=True,
             block=(7, 7),
             block_shape=(512, 512),
             dtype='uint16',
             npz=False,
             bytes_at_open=None,
             aws_unsigned=False):
    import pickle

    def without(xx, skip):
        return SimpleNamespace(**{k: v for k, v in xx.__dict__.items() if k not in skip})

    files = slurp_lines(file_list_file)

    pp = SimpleNamespace(block=block,
                         block_shape=block_shape,
                         dtype=dtype,
                         nthreads=nthreads,
                         bytes_at_open=bytes_at_open,
                         aws_unsigned=aws_unsigned,
                         mode=mode,
                         ssl=ssl,
                         band=1)

    print('''Files:
{}
 ...
{}
    files   - {:d}
    threads - {:d}
    mode    - {}{}
    '''.format('\n'.join(files[:3]),
               '\n'.join(files[-2:]),
               len(files),
               pp.nthreads,
               mode, ' (no S3 signing)' if aws_unsigned else ''))

    procs = {'rio': pprio_bench.PReadRIO_bench}

    if mode not in procs:
        raise ValueError('Unknown mode: {} only know: rio'.format(mode))
    ProcClass = procs[mode]

    rdr = ProcClass(nthreads=pp.nthreads,
                    region_name=None,  # None -- auto-guess
                    use_ssl=ssl,
                    bytes_at_open=bytes_at_open,
                    aws_unsigned=aws_unsigned)
    rdr.warmup()

    if wmore:
        nwarm = min(len(files), pp.nthreads)
        print('Will read {} files for warmup first'.format(nwarm))

        pix = np.ndarray((nwarm, *pp.block_shape), dtype=pp.dtype)
        _, ww = rdr.read_blocks(files[-nwarm:], pp.block, dst=pix)
        print('Done in {:.3f} seconds'.format(ww.t_total))

    pix = np.ndarray((len(files), *pp.block_shape), dtype=pp.dtype)
    _, xx = rdr.read_blocks(files, pp.block, dst=pix)

    for k, v in pp.__dict__.items():
        if not hasattr(xx.params, k):
            setattr(xx.params, k, v)

    xx.result_hash = array_digest(pix)

    if wmore:
        xx._warmup = ww

    print('Result hash: {}'.format(xx.result_hash))

    fnames = {ext: mk_fname(xx.params, ext=ext, prefix=prefix)
              for ext in ['pickle', 'npz']}

    pickle.dump(xx, open(fnames['pickle'], 'wb'))

    print('''Saved results to:
    - {}'''.format(fnames['pickle']))

    if npz:
        np.savez(fnames['npz'], data=pix)
        print('    - {}'.format(fnames['npz']))

    print(gen_stats_report(xx))

    return 0
