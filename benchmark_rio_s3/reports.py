import numpy as np
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

    chunk_size = np.r_[[r.chunk_size for r in xx.stats]]
    t_open = np.r_[[r.t_open for r in xx.stats]]*t_scaler
    t_total = np.r_[[r.t_total for r in xx.stats]]*t_scaler
    t_read = t_total - t_open

    t0 = np.r_[[r.t0 for r in xx.stats]]*t_scaler
    t0 -= t0.min()
    t_end = t0 + t_total
    fps_t, fps = files_per_second(t_end/t_scaler)

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
