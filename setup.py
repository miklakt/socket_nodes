import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

setup(
    name='socket_nodes',
    version='0.0.4',
    description='TCP socket nodes and server',
    author='Laktionov Mikhail',
    author_email = 'miklakt@gmail.com',
    packages=['socket_nodes'],
)