import sys
import click


def parse_shape(s):
    shape = tuple(int(v) for v in s.split('x'))
    if len(shape) == 1:
        shape = shape + shape

    if len(shape) != 2:
        raise ValueError('Expect WxH')

    return shape[::-1]


def parse_xy(s):
    xy = tuple(int(v) for v in s.split(','))
    if len(xy) != 2:
        raise ValueError('Expect x,y')
    return xy


def click_parse_shape(ctx, param, value):
    if value is None:
        return None

    try:
        return parse_shape(value)
    except ValueError:
        raise click.BadParameter('Expect WxH')


def click_parse_xy(ctx, param, value):
    if value is None:
        return None

    try:
        return parse_xy(value)
    except ValueError:
        raise click.BadParameter('Expect x,y')


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
def run_suite():
    """Run benchmark suite

    Run benchmarks with different settings

    Note that this will launch a new process for every run to make sure that
    various once-off costs can be measured.
    """
    print(sys.argv)
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
    assert parse_xy('3,4') == (3, 4)
