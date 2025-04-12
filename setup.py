from setuptools import setup, find_packages

setup(
    name='bagfile_reader',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'foxglove-data-platform==0.6.0',
        'mcap-protobuf-support==0.0.10',
        'mcap-ros2-support==0.1.0',
        'mcap-ros1-support==0.4.0',
        'duckdb',
        'opencv-python',
        'numpy',
        'pandas',
        'tqdm'
    ],
    author='William Engel',
    description='A helper class for reading bagfiles using the Foxglove Data Platform',
    python_requires='>=3.7',
)
