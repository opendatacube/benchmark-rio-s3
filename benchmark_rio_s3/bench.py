from matplotlib import pyplot as plt
import numpy as np
import hashlib
from types import SimpleNamespace
from . import s3fetch
from . import pread_rio


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

    fmt = ('{prefix}_{p.tile[0]:d}_{p.tile[1]:d}b{p.block[0]:d}_{p.block[1]:d}B{p.band}'
           '__{p.nthreads:02d}_%03d.{ext}').format(prefix=prefix,
                                                   p=params,
                                                   ext=ext)

    return find_next_available_file(fmt)


def slurp_lines(fname, *args, **kwargs):
    if len(args) > 0 or len(kwargs) > 0:
        fname = fname.format(*args, **kwargs)

    with open(fname, 'rt') as f:
        return [s.rstrip() for s in f.readlines()]


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


def link_x_axis(*axs, start_from_zero=None):
    x_min = min(ax.axis()[0] for ax in axs)
    x_max = max(ax.axis()[1] for ax in axs)

    if start_from_zero and x_min > 0:
        x_min = 0

    for ax in axs:
        ax.axis((x_min, x_max) + ax.axis()[2:])


def add_hist(data, n, ax=None, n_sigma=None, thresh=None, **kwargs):
    if n_sigma is not None and thresh is None:
        thresh = data.mean() + np.sqrt(data.var())

    if thresh is not None:
        data = data[data < thresh]

    if ax is None:
        ax = plt.gca()

    return ax.hist(data, n, **kwargs)


def unpack_stats(xx, ms=False):
    t_scaler = 1000 if ms else 1

    chunk_size = np.r_[[r.chunk_size for r in xx.stats]]
    t_open = np.r_[[r.t_open for r in xx.stats]]*t_scaler
    t_total = np.r_[[r.t_total for r in xx.stats]]*t_scaler
    t_read = t_total - t_open

    return SimpleNamespace(chunk_size=chunk_size,
                           t_open=t_open,
                           t_read=t_read,
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
Tile: ({pp.tile[0]:d},{pp.tile[1]:d})@{pp.block[0]:d}_{pp.block[1]:d}#{pp.band:d}
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


def plot_results(rr, fig=None):
    chunk_size = np.r_[[r.chunk_size for r in rr]]
    t_open = np.r_[[r.t_open for r in rr]]*1000
    t_total = np.r_[[r.t_total for r in rr]]*1000
    t_read = t_total - t_open

    fig = fig or plt.figure(figsize=(12, 8))

    ax = fig.add_subplot(2, 2, 1)
    ax.plot(chunk_size, '.')
    ax.set_title('Chunk size (bytes)')
    ax.xaxis.set_visible(False)

    ax = fig.add_subplot(2, 2, 2)
    ax.hist(chunk_size, 50, linewidth=0, alpha=0.5, color='b')
    ax.yaxis.set_visible(False)
    ax.set_title('Chunk size (bytes)')

    ax = fig.add_subplot(2, 2, 3)
    plt.scatter(chunk_size, t_total, marker='.', s=3)
    ax.set_title('Chunk Size vs Load Time')

    ax = fig.add_subplot(2, 2, 4)
    add_hist(t_open, 30, n_sigma=1.5, ax=ax, alpha=0.4, color='r', linewidth=0)
    add_hist(t_read, 30, n_sigma=1.5, ax=ax, alpha=0.4, color='g', linewidth=0)
    add_hist(t_total, 30, n_sigma=1.5, ax=ax, alpha=0.2, color='b', linewidth=0)
    ax.legend(['Open', 'Read', 'Total'])
    ax.set_title('Time (ms)')
    ax.yaxis.set_visible(False)

    fig.tight_layout()

    return fig


def plot_stats_results(data, fig, cc=None):
    from matplotlib import __version__ as mp_version

    if cc is None:
        if mp_version >= '2.0.0':
            cc = ['C0', 'C1', 'C2', 'C3']
        else:
            cc = ['b', 'g', 'r', 'm']

    n_threads, total_t, total_b, total_f = np.r_[[(s.params.nthreads,
                                                   s.t_total,
                                                   sum([x.chunk_size for x in s.stats]),
                                                   len(s.stats))
                                                  for s in data]].T
    files_per_second = total_f/total_t
    kb_throughput = total_b/total_t/(1 << 10)
    wkpt = files_per_second/n_threads
    wkpt = 100*wkpt/wkpt.max()

    best_idx = total_t.argmin()

    x_ticks = np.array([1] + list(range(4, int(n_threads.max()), 4))
                       + [n_threads.max()])

    ax = fig.add_subplot(2, 2, 1)
    c = cc[0]
    ax.plot(n_threads, total_t, c+'o-', linewidth=3, alpha=0.7)
    ax.set_xlabel('# Worker Threads')
    ax.set_ylabel('Time (secs)')
    ax.xaxis.set_ticks(x_ticks)

    ax.annotate('{s.t_total:.3f} secs using {s.params.nthreads:d} threads'.format(s=data[best_idx]),
                xy=(n_threads[best_idx], total_t[best_idx]),
                xytext=(0.3, 0.9),
                textcoords='axes fraction',
                arrowprops=dict(facecolor=c,
                                alpha=0.4,
                                shrink=0.05))

    ax = fig.add_subplot(2, 2, 2)
    c = cc[1]
    ax.plot(n_threads, files_per_second, c+'s-', linewidth=3, alpha=0.7)
    ax.set_xlabel('# Worker Threads')
    ax.set_ylabel('Files/sec')
    ax.xaxis.set_ticks(x_ticks)

    ax.annotate('{frate:.0f} files/s using {s.params.nthreads:d} threads'.format(
        frate=files_per_second[best_idx],
        s=data[best_idx]),
                xy=(n_threads[best_idx], files_per_second[best_idx]),
                xytext=(0.3, 0.1),
                textcoords='axes fraction',
                arrowprops=dict(facecolor=c,
                                alpha=0.4,
                                shrink=0.05))

    ax = fig.add_subplot(2, 2, 3)
    c = cc[2]
    ax.plot(n_threads, wkpt, c+'o-')
    ax.xaxis.set_ticks(x_ticks)
    ax.set_xlabel('# Worker Threads')
    ax.set_ylabel('Efficiency per thread %')
    ax.axis(ax.axis()[:2] + (0, 105))

    ax = fig.add_subplot(2, 2, 4)
    c = cc[3]
    ax.plot(n_threads, kb_throughput, c+'o-', linewidth=3, alpha=0.7)
    ax.set_xlabel('# Worker Threads')
    ax.set_ylabel('KiB/sec*')
    ax.xaxis.set_ticks(x_ticks)

    ax.annotate('* excluding headers',
                xy=(1, 1),
                xytext=(0.05, 0.9),
                textcoords='axes fraction')

    ax.annotate('{kbps:.0f} KiB/s using {s.params.nthreads:d} threads'.format(
        kbps=kb_throughput[best_idx],
        s=data[best_idx]),
                xy=(n_threads[best_idx], kb_throughput[best_idx]),
                xytext=(0.3, 0.1),
                textcoords='axes fraction',
                arrowprops=dict(facecolor=c,
                                alpha=0.4,
                                shrink=0.05))

    fig.tight_layout()
    return best_idx


def plot_comparison(fig, stats, names=None, threshs=None, colors=None, nochunk=False):
    from matplotlib import __version__ as mp_version

    if names is None:
        names = ['A', 'B']

    def linked_hist(data, axs, colors, msg, nb, thresh):
        for d, ax, cc, name in zip(data, axs, colors, names):
            add_hist(d, nb, ax=ax, thresh=thresh, alpha=0.3, color=cc)
            ax.legend(['{} ({})'.format(msg, name)])
            ax.axvline(d.mean(), color=cc, linewidth=3, alpha=0.7)
            ax.yaxis.set_visible(False)

        link_x_axis(*axs, start_from_zero=True)

    if threshs is None:
        threshs = [18000, 250, 100]

    if colors is None:
        if mp_version >= '2.0.0':
            colors = (('C0', 'C1'),
                      ('C4', 'C3'),
                      ('C9', 'C6'))
        else:
            colors = (('r', 'g'),
                      ('c', 'm'),
                      ('b', 'y'))

    if nochunk:
        linked_hist([s.t_total for s in stats],
                    [fig.add_subplot(2, 3, i) for i in [1, 4]],
                    colors[0],
                    'Total',
                    30, threshs[0])
    else:
        linked_hist([s.chunk_size for s in stats],
                    [fig.add_subplot(2, 3, i) for i in [1, 4]],
                    colors[0],
                    'Chunk Size',
                    30, threshs[0])

    linked_hist([s.t_open for s in stats],
                [fig.add_subplot(2, 3, i) for i in [2, 5]],
                colors[1],
                'Open',
                30, threshs[1])

    linked_hist([s.t_read for s in stats],
                [fig.add_subplot(2, 3, i) for i in [3, 6]],
                colors[2],
                'Read',
                30, threshs[2])

    fig.tight_layout()


def update_params(pp, **kwargs):
    from copy import copy
    pp = copy(pp)
    for k, v in kwargs.items():
        if hasattr(pp, k):
            setattr(pp, k, v)
        else:
            raise ValueError("No such parameter: '{}'".format(k))
    return pp


def string2bool(s):
    return s.lower() in ['y', 'yes', 't', 'true', '1']


def run_main(file_list_file, nthreads,
             prefix='MXL5',
             mode='rio',
             ssl='no',
             wmore='y',
             npz='no'):
    import pickle

    def without(xx, skip):
        return SimpleNamespace(**{k: v for k, v in xx.__dict__.items() if k not in skip})

    ssl = string2bool(ssl)
    npz = string2bool(npz)
    wmore = string2bool(wmore)

    nthreads = int(nthreads)

    files = slurp_lines(file_list_file)

    pp = SimpleNamespace(tile=(-9, -18),  # TODO: extract from file name?
                         block=(8, 2),
                         block_shape=(256, 256),
                         dtype='uint8',
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

    procs = {'rio': pread_rio.PReadRIO_bench,
             's3tif': s3fetch.S3TiffReader}

    if mode not in procs:
        raise ValueError('Unknown mode: {} only know: rio|s3tif'.format(mode))
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


def run_bench_suite(uris_file,
                    thread_counts=None,
                    mode_prefixes=None,
                    ssl='y',
                    wmore='y',
                    mode=None,
                    times=1,
                    warmup_passes=1):
    import sys
    import os
    from datetime import datetime
    from pathlib import Path
    import shutil
    from subprocess import check_call

    bench_app = Path(__file__).resolve().parent.parent/'runbench.py'
    bench_app = bench_app.resolve()

    modes = [mode] if mode is not None else ['rio', 's3tif']

    def external_run_bench(urls, nthreads, prefix, npz='n', ssl='y', mode='rio', wmore='y'):
        def opts(**kwargs):
            return ['{}={}'.format(k, v) for k, v in kwargs.items()]

        args = [sys.executable, str(bench_app), urls, str(nthreads)] + opts(npz=npz,
                                                                            wmore=wmore,
                                                                            ssl=ssl,
                                                                            mode=mode,
                                                                            prefix=prefix)
        return check_call(args)

    if thread_counts is None:
        thread_counts = [1, 2, 4, 8, 16, 20, 24, 28, 32, 38]

    if mode_prefixes is None:
        mode_prefixes = dict(rio='RIO',
                             s3tif='S3T')

    out_dir = Path(''.join(datetime.now().isoformat().split(':')[:2]))

    if out_dir.exists():
        print('Output directory: {} already exists'.format(out_dir))
        return 1

    out_dir.mkdir()
    out_dir = out_dir.resolve()

    copy_file = str(out_dir/'urls.txt')
    shutil.copy(uris_file, copy_file)

    os.chdir(str(out_dir))

    # warmup bucket
    for _ in range(warmup_passes):
        external_run_bench(copy_file, 32, prefix='WMP', wmore=wmore, ssl=ssl)

    for tc in thread_counts:
        for mode in modes:
            prefix = mode_prefixes[mode]
            for _ in range(times):
                external_run_bench(copy_file, tc, prefix=prefix, mode=mode, ssl=ssl, wmore=wmore)

    print('Results saved to: "{}"'.format(out_dir))
    return 0


def main(args=None):
    import sys

    if args is None:
        args = sys.argv[1:]

    aa = []
    kw = {}

    for a in args:
        kv = a.split('=')
        if len(kv) == 1:
            aa.append(a)
        elif len(kv) == 2:
            k, v = kv
            kw[k] = v

    if len(aa) == 1:
        for k in ['times', 'warmup_passes']:
            if k in kw:
                kw[k] = int(kw[k])

        if 'thread_counts' in kw:
            kw['thread_counts'] = [int(x) for x in kw['thread_counts'].split(',')]

        print('Run test suite with file: ' + aa[0])
        return run_bench_suite(aa[0], **kw)

    if len(aa) != 2:
        print('Expect at least 2 args: file_list num_threads <prefix=prefix> <mode=rio|s3tif> <ssl=y|n> <npz=y|n>')
        return 1

    return run_main(*aa, **kw)
