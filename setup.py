
#!/usr/bin/env python

from distutils.core import setup

import bedup.btrfs
import bedup.chattr
import bedup.fiemap
import bedup.ioprio
import bedup.openat

setup(
    name='bedup',
    version='0.0.1',
    author='Gabriel de Perthuis',
    author_email='g2p.code@gmail.com',
    license='GNU GPL',
    description='Deduplication for Btrfs filesystems',
    install_requires=[
        'argparse',  # only required for Python 2.6
        #'cffi >= 0.4',  # 0.4 is not released and confuses pip
        'pyxdg',
        'sqlalchemy',
        'ttystatus',  # XXX Not uploaded to PyPI
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
)

