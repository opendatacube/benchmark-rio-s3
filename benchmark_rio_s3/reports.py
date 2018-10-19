import glob
import pickle
import numpy as np
import itertools
from types import SimpleNamespace


def files_per_second(t_end):
    """ Compute files per second as a function of completion time

    Return: sorted(t_end), files_per_second
    """
    tt = np.sort(t_end)
    nn = np.arange(1, tt.shape[0] + 1)
    return tt, nn/tt


def unpack_stats(xx, ms=False):
    t_scaler = 1000 if ms else 1

    stats = [r for r in xx.stats if r is not None]
    n_bad = len(xx.stats) - len(stats)

    chunk_size = np.r_[[r.chunk_size for r in stats]]
    t_open = np.r_[[r.t_open for r in stats]]*t_scaler
    t_total = np.r_[[r.t_total for r in stats]]*t_scaler
    t_read = t_total - t_open

    t0 = np.r_[[r.t0 for r in stats]]*t_scaler
    t0 -= t0.min()
    t_end = t0 + t_total
    fps_t, fps = files_per_second(t_end/t_scaler)

    return SimpleNamespace(chunk_size=chunk_size,
                           nthreads=xx.params.nthreads,
                           params=xx.params,
                           _raw=xx,
                           t0=t0,
                           t_end=t_end,
                           t_open=t_open,
                           t_read=t_read,
                           n_bad=n_bad,
                           duration=xx.t_total,
                           throughput=np.median(fps),
                           throughput_max=fps.max(),
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

    if not isinstance(xx, StatsResult):
        xx = StatsResult(**unpack_stats(xx, ms=True).__dict__)

    chunk_size = xx.chunk_size
    t_open = xx.t_open
    t_total = xx.t_total
    t_read = xx.t_read
    n_bad = xx.n_bad
    hash = getattr(xx._raw, 'result_hash', None)

    hdr = '''
Tile: {pp.block[0]:d}_{pp.block[1]:d}#{pp.band:d}
   - blocks  : {pp.block_shape[0]:d}x{pp.block_shape[1]:d}@{pp.dtype}
   - nthreads: {pp.nthreads:d}
{extra_msg}
'''.format(pp=xx.params,
           extra_msg='' if extra_msg is None else '   - ' + extra_msg).strip()

    if hash is not None:
        hash = hash[:32]+'..'+hash[-8:]
    else:
        hash = "<no hash recorded>"

    if n_bad > 0:
        failures = 'WARNING: {} datasets failed to load\n'.format(n_bad)
    else:
        failures = ''

    return '''
-------------------------------------------------------------
{}
-------------------------------------------------------------
  {}
{}
Files read             : {:,d}
Total data bytes       : {:,d}
  (excluding headers)
Bytes per chunk        : {:,d} [{:,d}..{:,d}]

 Time        Median Min          Max
 per tile  --------------------------
  - total   {:7.3f} [{:.<6.1f}..{:.>7.1f}] ms
  - open    {:7.3f} [{:.<6.1f}..{:.>7.1f}] ms {:4.1f}%
  - read    {:7.3f} [{:.<6.1f}..{:.>7.1f}] ms {:4.1f}%

total_wait: {:7.2f} sec (across all threads)
walltime  : {:7.2f} sec
throughput: {:6.1f} tiles per second
            {:6.1f} tiles per second per thread
-------------------------------------------------------------
'''.format(hdr,
           hash,
           failures,
           chunk_size.shape[0],
           chunk_size.sum(),
           int(np.median(chunk_size)), chunk_size.min(), chunk_size.max(),
           np.median(t_total), t_total.min(), t_total.max(),
           np.median(t_open), t_open.min(), t_open.max(), (t_open/t_total).mean()*100,
           np.median(t_read), t_read.min(), t_read.max(), (t_read/t_total).mean()*100,
           (t_total.sum()*1e-3).round(),
           xx.duration,
           xx.throughput,
           xx.throughput/xx.nthreads).strip()


class StatsResult(object):
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        if not hasattr(self, 'file'):
            self.file = ''

    def __repr__(self):
        return 'x{s.nthreads:d} fps:{s.throughput:.2f} t:{s.duration:.1f}s <{s.file}>'.format(s=self)

    def __str__(self):
        return self.__repr__()


def load_dir(dirname='.', filter='*__*.pickle', ms=True):
    """ Returns a dictionary

    number_of_threads -> [StatsResult]
    """
    def load(fname):
        with open(fname, 'rb') as f:
            x = unpack_stats(pickle.load(f), ms=ms)
            x.file = fname
            return StatsResult(**x.__dict__)

    files = [f for f in glob.glob(dirname + '/' + filter) if not f.startswith('WRM')]

    data_all = sorted([load(f) for f in files], key=lambda s: s.nthreads)
    data_all = dict((k, list(v)) for k, v in itertools.groupby(data_all,
                                                               lambda s: s.nthreads))
    return data_all


def pick_best(d, mode='time'):
    """ Returns a dictionary

    number_of_threads -> BestUnpackedResults
    """
    modes = dict(time=lambda s: s.duration,  # smaller first
                 throughput=lambda s: -s.throughput  # bigger first
                 )

    comparator = modes[mode]
    return {k: sorted(v, key=comparator)[0] for k, v in d.items()}
