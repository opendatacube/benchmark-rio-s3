import numpy as np
import hashlib
import sys
from types import SimpleNamespace
from . import pprio_bench


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


def files_per_second(t_end):
    """ Compute files per second as a function of completion time

    Return: sorted(t_end), files_per_second
    """
    tt = np.sort(t_end)
    nn = np.arange(1, tt.shape[0] + 1)
    return tt, nn/tt


def unpack_stats(xx, ms=False):
    t_scaler = 1000 if ms else 1

    chunk_size = np.r_[[r.chunk_size for r in xx.stats]]
    t_open = np.r_[[r.t_open for r in xx.stats]]*t_scaler
    t_total = np.r_[[r.t_total for r in xx.stats]]*t_scaler
    t_read = t_total - t_open

    if hasattr(xx.stats[0], 't0'):
        t0 = np.r_[[r.t0 for r in xx.stats]]*t_scaler
        t0 -= t0.min()
        t_end = t0 + t_total
        fps_t, fps = files_per_second(t_end/t_scaler)
    else:
        t0, t_end, fps, fps_t = [None]*4

    return SimpleNamespace(chunk_size=chunk_size,
                           nthreads=xx.params.nthreads,
                           t0=t0,
                           t_end=t_end,
                           t_open=t_open,
                           t_read=t_read,
                           fps=fps,
                           fps_t=fps_t,
                           t_total=t_total)


def join_reports(s1, s2):
    s1 = s1.split('\n')
    s2 = s2.split('\n')

    def trim(ss):
        nmax = max(len(s) for s in ss if not s.startswith('--'))
        ss = [s[:nmax] for s in ss]
        return ss, nmax

    s1, nmax1 = trim(s1)
    s2, nmax2 = trim(s2)

    fmt = "{:%d} | {}" % nmax1
    ll = [fmt.format(a, b) for a, b in zip(s1, s2)]
    ll = ['-'*len(l) if l.startswith('--') else l for l in ll]
    return '\n'.join(ll)


def gen_stats_report(xx, extra_msg=None):

    chunk_size = np.r_[[r.chunk_size for r in xx.stats]]
    t_open = np.r_[[r.t_open for r in xx.stats]]*1000
    t_total = np.r_[[r.t_total for r in xx.stats]]*1000
    t_read = t_total - t_open
    hdr = '''
Tile: {pp.block[0]:d}_{pp.block[1]:d}#{pp.band:d}
   - blocks  : {pp.block_shape[0]:d}x{pp.block_shape[1]:d}@{pp.dtype}
   - nthreads: {pp.nthreads:d}
{extra_msg}
'''.format(pp=xx.params,
           extra_msg='' if extra_msg is None else '   - ' + extra_msg).strip()

    hash = getattr(xx, 'result_hash', None)
    if hash is not None:
        hash = hash[:32]+'..'+hash[-8:]
    else:
        hash = "<no hash recorded>"

    return '''
-------------------------------------------------------------
{}
-------------------------------------------------------------

Files read             : {:d}
Total data bytes       : {:,d}
  (excluding headers)
Bytes per chunk        : {:.0f} [{:d}..{:d}]

Time:
 per tile:
  - total   {:7.3f} [{:.<6.1f}..{:.>7.1f}] ms
  - open    {:7.3f} [{:.<6.1f}..{:.>7.1f}] ms {:4.1f}%
  - read    {:7.3f} [{:.<6.1f}..{:.>7.1f}] ms {:4.1f}%

 {}
 total_cpu: {:.2f} sec
 walltime : {:.2f} sec
-------------------------------------------------------------
'''.format(hdr,
           chunk_size.shape[0],
           chunk_size.sum(),
           chunk_size.mean().round(), chunk_size.min(), chunk_size.max(),
           t_total.mean(), t_total.min(), t_total.max(),

           t_open.mean(), t_open.min(), t_open.max(), (t_open/t_total).mean()*100,
           t_read.mean(), t_read.min(), t_read.max(), (t_read/t_total).mean()*100,

           hash,

           (t_total.sum()*1e-3).round(),
           xx.t_total).strip()


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
             npz=False):
    import pickle

    def without(xx, skip):
        return SimpleNamespace(**{k: v for k, v in xx.__dict__.items() if k not in skip})

    files = slurp_lines(file_list_file)

    pp = SimpleNamespace(block=block,
                         block_shape=block_shape,
                         dtype=dtype,
                         nthreads=nthreads,
                         mode=mode,
                         ssl=ssl,
                         band=1)

    print('''Files:
{}
 ...
{}
    files   - {:d}
    threads - {:d}
    mode    - {}
    '''.format('\n'.join(files[:3]),
               '\n'.join(files[-2:]),
               len(files),
               pp.nthreads,
               mode))

    procs = {'rio': pprio_bench.PReadRIO_bench}

    if mode not in procs:
        raise ValueError('Unknown mode: {} only know: rio'.format(mode))
    ProcClass = procs[mode]

    rdr = ProcClass(nthreads=pp.nthreads,
                    region_name='ap-southeast-2',
                    use_ssl=ssl)
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
