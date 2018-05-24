# benchmark-rio-s3

Tools for benchmarking multi-threaded performance of Rasterio/GDAL when
accessing files on S3.

Benchmark measures how long it takes to

1. Open GeoTiff file
2. Read single block (also referred as tile in GeoTiff spec)
3. How well this process scales with more processing threads

When you see word "block" in this document/command line options, it refers to
the same thing as "tile" in GeoTiff spec.


## Prerequisites

This has been tested on Ubuntu 16.04 and 18.04 images available in the AWS
marketplace, but any Linux with python 3.5 or better should work, so long as you
have recent enough versions of [GDAL](http://www.gdal.org)
and [rasterio](https://github.com/mapbox/rasterio/) installed.

You machine should have properly configured AWS credentials for accessing data.
To verify try this command:

```
rio info s3://mybucket/myfile.tif
```

or you can use Landsat 8 public archive, for example

```
rio info s3://landsat-pds/c1/L8/106/070/LC08_L1TP_106070_20180417_20180501_01_T1/LC08_L1TP_106070_20180417_20180501_01_T1_B1.TIF
```

## Installation

On Ubuntu

```
sudo pip3 install git+https://github.com/Kirill888/benchmark-rio-s3.git
```

Then to check everything went well

```
bench-rio-s3 --help
bench-rio-s3 run --help
```

## Running Benchmark

First you'll need to generate a list of urls to use for testing. These should
point to GeoTiff files on S3. All files have to have the same `dtype`, they need
to be "tiled", and their tile sizes should be the same. For example Landsat 8
images have `dtype=uint16` and tile shape is `512x512`. You need to pick which
block to read, this block should be present in all the images. If you don't
specify which block to read, "middle" block will be automatically selected. For
example Landsat 8 images are something like 16x16 blocks in size, where each
block is 512x512 pixels. If you specify `--block 1,2` pixels
`im[512:1024,1024:1536]` will be fetched.

To generate the list you can use a helper command

```
bench-rio-s3 ls s3://bucket/path/to/images
```

### Example using Ladsat 8

First generate url list

```
bench-rio-s3 ls --filter '*_B?.TIF' s3://landsat-pds/c1/L8/106/070/ | tee urls.txt
```

Run a quick test to see things are alright

```
head -n 8 urls.txt | bench-rio-s3 run --skip-bucket-warmup --no-warmup-more --threads 4 -
```

Then run a full test

```
bench-rio-s3 run --threads "$(seq -s, 32)" --times 3 urls.txt
```

This will repeat test 3 times with number of worker threads from 1 all the
way to 32.


## Visualising results

TODO
