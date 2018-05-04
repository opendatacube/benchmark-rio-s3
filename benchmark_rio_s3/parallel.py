import concurrent.futures as fut
import queue
from types import SimpleNamespace

__all__ = ['ParallelStreamProc']


def split_it(src, n, qmaxsize=100, sleep=0.05):
    def q2it(q, state, timeout):
        while state.abort is False:
            try:
                idx, item = q.get(block=True, timeout=timeout)
                yield (idx, item)
                q.task_done()
            except queue.Empty:
                if state.completed:
                    break

    def run_src_pump(src, q, state, sleep, on_blocked=None):
        for item in src:
            filled = False

            while filled is False and state.abort is False:
                try:
                    q.put(item, block=True, timeout=sleep)
                    filled = True
                except queue.Full:
                    if on_blocked is not None:
                        on_blocked(state)

            if state.abort:
                return

        state.completed = True  # Mark end of stream to consumers
        q.join()

    q = queue.Queue(maxsize=qmaxsize)

    state = SimpleNamespace(abort=False,
                            completed=False,
                            _queue=q)

    consumers = [q2it(q, state, timeout=sleep) for _ in range(n)]
    state.run = lambda on_blocked=None: run_src_pump(src, q, state, sleep, on_blocked=on_blocked)

    return state, consumers


class ParallelStreamProc(object):
    """Process stream using multiple threads

    It's like `stream_proc(src_stream)` except in parallel across
    multiple threads, each of the threads will see only part of the original
    stream. Each element of the original stream will be observed by one thread
    only.

    ```
    def stream_proc(src, extra_arg, foo=None):
        # setup local thread storage or whatever
        for v in src:
            pass  # do work, taking care of multi-threading issues

    pp_stream_proc = ParalelStreamProc(4).bind(stream_proc)

    pp_stream_proc(src_stream, extra_args, foo=3)
    ```

    Note that the class itself is not thread-safe, expectation is that it is
    used by main thread, you can bind several different stream processors, but
    only one of them can be running at any given time.
    """
    def __init__(self, nthreads):
        self._workers = [fut.ThreadPoolExecutor(max_workers=1)
                         for _ in range(nthreads)]
        self._nthreads = nthreads
        self._state = None

    def _run(self, src, stream_proc, on_blocked=None, max_workers=None, args=(), kwargs=None):
        if max_workers is None:
            max_workers = self._nthreads
        elif max_workers > self._nthreads:
            raise ValueError("Only have {} worker threads, but asked for {}".format(self._nthreads, max_workers))
        elif max_workers < 1:
            raise ValueError("max_workers can not be less than 1")

        if kwargs is None:
            kwargs = {}

        if self._state is not None:
            raise ValueError("Can not run concurrent jobs")

        state, its = split_it(src, max_workers)
        self._state = state

        futures = [worker.submit(stream_proc, it, *args, **kwargs)
                   for worker, it in zip(self._workers[:max_workers], its)]

        state.run(on_blocked=on_blocked)

        rr = fut.wait(futures)
        assert len(rr.done) == len(futures)

        self._state = None

    def abort(self):
        state = self._state
        if state:
            state.abort = True

    def bind(self, stream_proc, on_blocked=None, max_workers=None):
        def run(src, *args, **kwargs):
            return self._run(src,
                             stream_proc,
                             on_blocked=on_blocked,
                             max_workers=max_workers,
                             args=args,
                             kwargs=kwargs)

        run.abort = self.abort
        return run
