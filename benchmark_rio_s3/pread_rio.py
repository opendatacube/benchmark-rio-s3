from timeit import default_timer as t_now
import rasterio
from types import SimpleNamespace
import threading
import boto3
from .parallel import ParallelStreamProc

__all__ = ["PReadRIO"]

_thread_lcl = threading.local()


def get_boto3_session(region_name='ap-southeast-2'):
    session = getattr(_thread_lcl, 'session', None)
    if session is None:
        session = boto3.Session(region_name=region_name)
        setattr(_thread_lcl, 'session', session)
    return session


class PReadRIO(object):
    @staticmethod
    def block_stream_proc(src_stream,
                          block,
                          dst,
                          stats_out,
                          gdal_opts=None,
                          region_name=None,
                          band=1):
        session = get_boto3_session(region_name)

        with rasterio.Env(session=session, **gdal_opts):
            for idx, url in src_stream:
                t0 = t_now()
                dst_slice = dst[idx, :, :]

                with rasterio.open(url, 'r') as f:
                    win = f.block_window(band, *block)
                    t1 = t_now()
                    f.read(band, window=win, out=dst_slice)
                    t2 = t_now()
                    chunk_size = f.block_size(band, *block)

                stats_out[idx] = SimpleNamespace(t_open=t1-t0,
                                                 t_total=t2-t0,
                                                 t0=t0,
                                                 chunk_size=chunk_size)

    def __init__(self, nthreads,
                 region_name='ap-southeast-2',
                 use_ssl=True):
        self._nthreads = nthreads
        self._pstream = ParallelStreamProc(nthreads)
        self._rdr_block = self._pstream.bind(PReadRIO.block_stream_proc)
        self._use_ssl = use_ssl  # At least for now we ignore this param
        self._region_name = region_name

        self._gdal_opts = dict(VSI_CACHE=True,
                               CPL_VSIL_CURL_ALLOWED_EXTENSIONS='tif',
                               GDAL_DISABLE_READDIR_ON_OPEN=True)

    def warmup(self):
        def _warmup():
            session = get_boto3_session(region_name=self._region_name)
            return session.get_credentials()

        return self._pstream.broadcast(_warmup)

    def read_blocks(self,
                    urls,
                    block_idx,
                    dst,
                    band=1):
        t0 = t_now()

        stats = [None for _ in urls]

        self._rdr_block(enumerate(urls), block_idx, dst, stats,
                        gdal_opts=self._gdal_opts,
                        region_name=self._region_name,
                        band=band)

        t_total = t_now()-t0
        params = SimpleNamespace(nthreads=self._nthreads,
                                 band=band,
                                 block_shape=dst.shape[1:],
                                 dtype=dst.dtype.name,
                                 block=block_idx)

        return dst, SimpleNamespace(stats=stats,
                                    params=params,
                                    t0=t0,
                                    t_total=t_total)
