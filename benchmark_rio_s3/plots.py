from matplotlib import pyplot as plt
from matplotlib import __version__ as mp_version
import numpy as np
from .reports import unpack_stats


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


def plot_results(rr, fig=None, cc=None):
    if cc is None:
        if mp_version >= '2.0.0':
            cc = ('C0', 'C9', 'C4', 'C7')
        else:
            cc = ('r', 'g', 'b', 'k')

    chunk_size = np.r_[[r.chunk_size for r in rr]]
    t_open = np.r_[[r.t_open for r in rr]]*1000
    t_total = np.r_[[r.t_total for r in rr]]*1000
    t_read = t_total - t_open

    fig = fig or plt.figure(figsize=(12, 8))

    ax = fig.add_subplot(2, 3, 1)
    ax.plot(chunk_size, cc[3]+'.')
    ax.set_title('Chunk size (bytes)')
    ax.xaxis.set_visible(False)

    ax = fig.add_subplot(2, 3, 2)
    ax.hist(chunk_size, 50, linewidth=0, alpha=0.5, color=cc[3])
    ax.yaxis.set_visible(False)
    ax.set_title('Chunk size (bytes)')

    ax = fig.add_subplot(2, 3, 3)
    plt.scatter(chunk_size, t_total, marker='.', s=3, color=cc[3])
    ax.set_title('Chunk Size vs Load Time')

    ax = fig.add_subplot(2, 1, 2)
    add_hist(t_open, 30, n_sigma=1.5, ax=ax, alpha=0.4, color=cc[0], linewidth=0, label='Open')
    add_hist(t_read, 30, n_sigma=1.5, ax=ax, alpha=0.4, color=cc[1], linewidth=0, label='Read')
    add_hist(t_total, 30, n_sigma=1.5, ax=ax, alpha=0.4, color=cc[2], linewidth=0, label='Total')
    ax.set_xlabel('Time (ms)')
    ax.yaxis.set_visible(False)
    ax.legend()

    fig.tight_layout()

    return fig


def plot_stats_results(data, fig, cc=None):

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

    sts = [unpack_stats(d) for d in data]
    if False and sts[0].fps is not None:
        fps_median = np.array([np.median(st.fps) for st in sts])
    else:
        fps_median = None

    mb_throughput = (total_b/total_t)/(1 << 20)
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
    if fps_median is not None:
        ax.plot(n_threads, fps_median, cc[0]+'o-', linewidth=3, alpha=0.7)

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
    ax.plot(n_threads, wkpt, c+'o-', linewidth=3, alpha=0.7)
    ax.xaxis.set_ticks(x_ticks)
    ax.set_xlabel('# Worker Threads')
    ax.set_ylabel('Efficiency per thread %')
    ax.axis(ax.axis()[:2] + (0, 105))

    ax = fig.add_subplot(2, 2, 4)
    c = cc[3]
    ax.plot(n_threads, mb_throughput, c+'o-', linewidth=3, alpha=0.7)
    ax.set_xlabel('# Worker Threads')
    ax.set_ylabel('MiB/sec*')
    ax.xaxis.set_ticks(x_ticks)

    ax.annotate('* excluding headers',
                xy=(1, 1),
                xytext=(0.05, 0.9),
                textcoords='axes fraction')

    ax.annotate('{mbps:.0f} MiB/s using {s.params.nthreads:d} threads'.format(
        mbps=mb_throughput[best_idx],
        s=data[best_idx]),
                xy=(n_threads[best_idx], mb_throughput[best_idx]),
                xytext=(0.3, 0.1),
                textcoords='axes fraction',
                arrowprops=dict(facecolor=c,
                                alpha=0.4,
                                shrink=0.05))

    fig.tight_layout()
    return best_idx


def plot_comparison(fig, stats, names=None, threshs=None, colors=None, alpha=0.3, nochunk=False):
    if names is None:
        names = ['A', 'B']

    def linked_hist(data, axs, colors, msg, nb, thresh):
        for d, ax, cc, name in zip(data, axs, colors, names):
            add_hist(d, nb, ax=ax, thresh=thresh, alpha=alpha, color=cc)
            ax.legend(['{} ({})'.format(msg, name)])
            ax.axvline(d.mean(), color=cc, linewidth=3, alpha=0.7)
            ax.yaxis.set_visible(False)

        link_x_axis(*axs, start_from_zero=True)

    if threshs is None:
        threshs = [18000, 250, 100]

    if colors is None:
        if mp_version >= '2.0.0':
            colors = (('C4', 'C3'),
                      ('C0', 'C1'),
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
