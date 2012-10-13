#!/usr/bin/env python

from setuptools import setup
from sys import version_info

import bedup.btrfs
import bedup.chattr
import bedup.fiemap
import bedup.ioprio
import bedup.openat
import bedup.syncfs

install_requires = [
    #'cffi >= 0.4',  # 0.4 is not released and confuses pip
    'pyxdg',
    'sqlalchemy',
]

if version_info < (2, 7):
    install_requires.append('argparse')

setup(
    name='bedup',
    version='0.0.3',
    author='Gabriel de Perthuis',
    author_email='g2p.code+bedup@gmail.com',
    url='https://github.com/g2p/bedup',
    license='GNU GPL',
    keywords='btrfs deduplication filesystem dedup',
    description='Deduplication for Btrfs filesystems',
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'bedup = bedup.__main__:script_main']},
    ext_modules=[
        bedup.btrfs.ffi.verifier.get_extension(),
        bedup.chattr.ffi.verifier.get_extension(),
        bedup.fiemap.ffi.verifier.get_extension(),
        bedup.ioprio.ffi.verifier.get_extension(),
        bedup.openat.ffi.verifier.get_extension(),
        bedup.syncfs.ffi.verifier.get_extension(),
    ],
    ext_package='bedup',
    packages=[
        'bedup',
    ],
    use_2to3=True,
    zip_safe=False,  # cargo-culted from the CFFI docs
    classifiers='''
        Programming Language :: Python :: 2
        Programming Language :: Python :: 3
        Programming Language :: Python :: Implementation :: CPython
        Programming Language :: Python :: Implementation :: PyPy
        License :: OSI Approved :: GNU General Public License (GPL)
        Operating System :: POSIX :: Linux
        Topic :: System :: Filesystems
        Topic :: Utilities
        Environment :: Console
    '''.strip().splitlines(),
)

