import sys
import click
from .bench import slurp_lines


def parse_shape(s):
    shape = tuple(int(v) for v in s.split('x'))
    if len(shape) == 1:
        shape = shape + shape

    if len(shape) != 2:
        raise ValueError('Expect WxH')

    return shape[::-1]


def parse_tuple(s, expect_n=None, error_msg='Wrong length'):
    if s is None:
        return None

    tt = tuple(int(v) for v in s.split(','))
    if expect_n is not None:
        if len(tt) != expect_n:
            raise ValueError(error_msg)
    return tt


def make_click_parser(func, error_msg):
    def parse(ctx, param, value):
        if value is None:
            return None
        try:
            return func(value)
        except ValueError:
            raise click.BadParameter(error_msg)
    return parse


click_parse_tuple = make_click_parser(parse_tuple, 'Expect comma separated list of integers')
click_parse_xy = make_click_parser(lambda s: parse_tuple(s, 2), 'Expect x,y')
click_parse_shape = make_click_parser(parse_shape, 'Expect WxH')

cli = click.Group(name='bench-rio-s3', help="Bunch of tools for benchmarking rasterio performance in the cloud")


@cli.command(name='run')
@click.option('--prefix', type=str, default='rio', help='Prefix for results file')
@click.option('--block', callback=click_parse_xy,
              default='7,7',
              help='Block to read, default: "7,7"')
@click.option('--dtype', default='uint16', help='Pixel type of the source images, default: uint16')
@click.option('--block-shape', default='512x512',
              callback=click_parse_shape,
              help='Size of GeoTiff blocks (aka tiles), default: 512x512')
@click.option('--warmup-more/--no-warmup-more',
              is_flag=True, default=True,
              help='Fetch one file per thread prior to recording benchmark data, on by default')
@click.option('-n', '--threads', type=int, default=1,
              help='Number of processing threads to use')
@click.option('--save-pixel-data',
              is_flag=True, default=False,
              help='Save fetched pixels using npz format from numpy')
@click.argument('url_file')
def run(prefix, block, dtype, block_shape,
        warmup_more, save_pixel_data,
        threads,
        url_file):
    """Run benchmark

    You will need to supply some information about test data

    \b
    dtype       -- type of pixel data, for example LS8 is uint16
    block-shape -- size of GeoTiff blocks (aka tiles), LS8 on AWS is 512x512
    block       -- which block to read, defaults to 7,7 which is roughly the
                   middle of a landsat tile

    You can use `rio info <url>` to find those

    \b
    URL_FILE    -- File containing urls to fetch, these should be unique, all
                   files should have the same format, i.e. dtype and tiling regime.

    \b
    example:
    s3://landsat-pds/c1/L8/106/070/LC08_L1TP_106070_20180417_20180501_01_T1/LC08_L1TP_106070_20180417_20180501_01_T1_B1.TIF
    s3://landsat-pds/c1/L8/106/070/LC08_L1TP_106070_20180503_20180516_01_T1/LC08_L1TP_106070_20180503_20180516_01_T1_B1.TIF

    You can use `bench-rio-s3 ls s3://mybucket/path/` to generated this file
    """
    from .bench import run_main

    run_main(url_file, threads,
             prefix=prefix,
             mode='rio',
             wmore=warmup_more,
             block=block,
             block_shape=block_shape,
             dtype=dtype,
             npz=save_pixel_data,)
    sys.exit(0)


@cli.command(name='run-suite')
@click.option('--block', callback=click_parse_xy,
              default=None,
              help='Block to read, default: "center" block')
@click.option('--warmup-more/--no-warmup-more',
              is_flag=True, default=True,
              help='Fetch one file per thread prior to recording benchmark data, on by default')
@click.option('-n', '--threads', default=None,
              callback=click_parse_tuple,
              help='Number of processing threads to run benchmark with, comma-separated list of integers, e.g. 1,2,4')
@click.option('--times', type=int, default=1,
              help='How many times to run benchmark for each thread count setting')
@click.argument('url_file')
def run_suite(block, warmup_more, threads, times, url_file):
    """Run benchmark suite

    Run benchmarks with different settings

    Note that this will launch a new process for every run to make sure that
    various once-off costs can be measured.
    """
    from pathlib import Path
    from datetime import datetime
    import os
    import rasterio
    import math

    def setup_output_dir(urls):
        out_dir = Path(''.join(datetime.now().isoformat().split(':')[:2]))

        if out_dir.exists():
            raise click.ClickException('Output directory: {} already exists'.format(out_dir))

        out_dir.mkdir()
        out_dir = out_dir.resolve()

        os.chdir(str(out_dir))

        with open('urls.txt', 'wt') as f:
            f.write('\n'.join(urls) + '\n')

    def external_run_bench(*args):
        from subprocess import check_call
        args = [sys.executable, sys.argv[0], 'run', *args]
        return check_call(args)

    def fetch_file_info(fname):
        idx = 0
        with rasterio.open(fname, 'r') as src:
            bshape = src.block_shapes[idx]
            shape_in_blocks = tuple(math.ceil(N/n) for N, n in zip(src.shape, bshape))
            return dict(dtype=src.dtypes[idx],
                        block_shape=bshape,
                        shape=src.shape,
                        shape_in_blocks=shape_in_blocks)

    def build_args(finfo, block, nthreads, prefix=None):
        args = ['--dtype={}'.format(finfo['dtype']),
                '--block-shape={}x{}'.format(*finfo['block_shape']),
                '--block={},{}'.format(*block),
                '--{}warmup-more'.format('' if warmup_more else 'no-'),
                '--threads={}'.format(nthreads),
                'urls.txt']
        if prefix is not None:
            args.insert(-1, '--prefix={}'.format(prefix))
        return args

    threads = threads or [1, 2, 4, 8, 16, 20, 24, 28, 32, 38]

    urls = slurp_lines(url_file)
    click.echo('Fetching info for {}'.format(urls[0]))
    finfo = fetch_file_info(urls[0])

    if block is None:
        block = tuple(n//2 for n in finfo['shape_in_blocks'])

    setup_output_dir(urls)
    # TODO: bucket warmup

    for nth in threads:
        args = build_args(finfo, block, nth)
        click.echo('Running with args: "{}"'.format(' '.join(args)))
        for _ in range(times):
            external_run_bench(*args)

    sys.exit(0)


@cli.command(name='ls')
@click.option('--filter', type=str, default=None,
              help='Supply filter shell style e.g. "*.TIF"')
@click.option('--regex', type=str, default=None,
              help='Supply filter (regular expression)')
@click.argument('prefix')
def run_s3_ls(filter, regex, prefix):
    """List files in some s3 bucket

    \b
    Example: bench-rio-s3 ls --filter '*_B1.TIF' s3://landsat-pds/c1/L8/106/070/
    """
    from .s3tools import s3_fancy_ls
    from fnmatch import fnmatch

    def glob_predicate(path):
        return fnmatch(path, filter)

    if filter is not None:
        predicate = glob_predicate
    else:
        predicate = regex

    urls = s3_fancy_ls(prefix, absolute=True, predicate=predicate)
    print('\n'.join(urls))
    sys.exit(0)


#######################################
# unit tests below
#######################################

def test_parsers():
    assert parse_shape('512') == (512, 512)
    assert parse_shape('640x480') == (480, 640)
    assert parse_tuple('3,4') == (3, 4)
