#!/usr/bin/env python

from distutils.core import setup

import bedup.btrfs
import bedup.chattr
import bedup.fiemap
import bedup.ioprio
import bedup.openat

setup(
    name='bedup',
    version='0.0.2',
    author='Gabriel de Perthuis',
    author_email='g2p.code+bedup@gmail.com',
    url='https://github.com/g2p/bedup',
    license='GNU GPL',
    keywords='btrfs deduplication dedup',
    description='Deduplication for Btrfs filesystems',
    install_requires=[
        'argparse',  # only required for Python 2.6
        #'cffi >= 0.4',  # 0.4 is not released and confuses pip
        'pyxdg',
        'sqlalchemy',
    ],
    ext_modules=[
        bedup.btrfs.ffi.verifier.get_extension(),
        bedup.chattr.ffi.verifier.get_extension(),
        bedup.fiemap.ffi.verifier.get_extension(),
        bedup.ioprio.ffi.verifier.get_extension(),
        bedup.openat.ffi.verifier.get_extension(),
    ],
    ext_package='bedup',
    packages=[
        'bedup',
    ],
    classifiers='''
        Programming Language :: Python :: 2
        Programming Language :: Python :: Implementation :: CPython
        Programming Language :: Python :: Implementation :: PyPy
        License :: OSI Approved :: GNU General Public License (GPL)
        Operating System :: POSIX :: Linux
        Topic :: System :: Filesystems
        Topic :: Utilities
        Environment :: Console
    '''.strip().splitlines(),

)

