
#!/usr/bin/env python

from distutils.core import setup

setup(
    name='bedup',
    version='0.0.1',
    author='Gabriel de Perthuis',
    author_email='g2p.code@gmail.com',
    description='Deduplication for Btrfs filesystems',
    install_requires=[
        'argparse',  # only required for Python 2.6
        #'cffi',  # 0.4 is not released and confuses pip
        'pyxdg',
        'sqlalchemy',
    ],
    packages=[
        'bedup',
    ],
)

