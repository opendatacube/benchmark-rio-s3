from setuptools import setup, find_packages

setup(
    name='benchmark-rio-s3',
    version='0.1',
    license='Apache License 2.0',
    url='https://github.com/opendatacube/benchmark-rio-s3',
    packages=find_packages(),
    include_package_data=True,
    author='Kirill Kouzoubov',
    author_email='kirill.kouzoubov@ga.gov.au',
    description='Tools for benchmarking multi-threaded performance of Rasterio/GDAL when accessing files on S3',
    python_requires='>=3.5',
    install_requires=['numpy',
                      'rasterio>=1.0.0',
                      'requests',
                      'botocore',
                      'boto3',
                      'click',
                      ],
    tests_require=['pytest'],
    extras_require=dict(
        report=['nbconvert', 'matplotlib']
    ),
    entry_points={
        'console_scripts': [
            'bench-rio-s3 = benchmark_rio_s3.app:cli',
        ],
    }
)
