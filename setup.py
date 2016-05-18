#!/usr/bin/env python3

from setuptools import setup
from shutil import copystat
from setuptools.command.build_py import build_py

import os

from bedup.platform.cffi_support import get_ext_modules


def replace_and_check(dat, pat, subst):
    # Only assert after, because build_py may run multiple times
    rv = dat.replace(pat, subst)
    assert subst in rv
    return rv


# http://www.digip.org/blog/2011/01/generating-data-files-in-setup.py.html
# build_py runs before build_ext
class build_py_with_cffi_marker(build_py):
    def run(self):
        build_py.run(self)
        if self.dry_run:
            return

        marker_path = os.path.join(
            self.build_lib, 'bedup/platform/cffi_support.py')
        with open(marker_path) as marker_file:
            marker_data = marker_file.read()
        marker_data = replace_and_check(marker_data,
            'BTRFS_INCLUDE_DIR = getcwd()\n',
            'BTRFS_INCLUDE_DIR = %r\n' % os.getcwd())
        marker_data = replace_and_check(marker_data,
            'CFFI_INSTALLED_MODE = False\n',
            'CFFI_INSTALLED_MODE = True\n')
        assert 'CFFI_INSTALLED_MODE = True\n' in marker_data
        marker_path2 = marker_path + '.processed'
        with open(marker_path2, 'w') as marker_file:
            marker_file.write(marker_data)
        copystat(marker_path, marker_path2)
        os.rename(marker_path2, marker_path)


install_requires = [
    'alembic',  # XXX I need Alembic, but not Mako or MarkupSafe.
    'cffi >= 0.4.2',
    # Requiring this version to get rid of lextab/yacctab
    # files dumped in random places
    'pycparser >= 2.9.1',
    'pyxdg',
    'SQLAlchemy >= 0.8.2', # needs Query.select_entity_from()
]

setup(
    name='bedup',
    version='0.10.1',
    author='Gabriel de Perthuis',
    author_email='g2p.code+bedup@gmail.com',
    url='https://github.com/g2p/bedup',
    license='GNU GPL',
    keywords='btrfs deduplication filesystem dedup',
    description='Deduplication for Btrfs filesystems',
    install_requires=install_requires,
    extras_require={
        'interactive': ['ipdb']},
    entry_points={
        'console_scripts': [
            'bedup = bedup.__main__:script_main']},
    cmdclass=dict(build_py=build_py_with_cffi_marker),
    ext_modules=get_ext_modules(),
    ext_package='bedup.platform',
    packages=[
        'bedup',
        'bedup.platform',
    ],
    zip_safe=False,  # cargo-culted from the CFFI docs
    classifiers='''
        Programming Language :: Python :: 3
        Programming Language :: Python :: Implementation :: CPython
        Programming Language :: Python :: Implementation :: PyPy
        License :: OSI Approved :: GNU General Public License (GPL)
        Operating System :: POSIX :: Linux
        Intended Audience :: System Administrators
        Intended Audience :: End Users/Desktop
        Topic :: System :: Filesystems
        Topic :: Utilities
        Environment :: Console
    '''.strip().splitlines(),
    long_description='''
    Deduplication for Btrfs.

    bedup looks for new and changed files, making sure that multiple copies of
    identical files share space on disk. It integrates deeply with btrfs so
    that scans are incremental and low-impact.

    See `github.com/g2p/bedup <https://github.com/g2p/bedup#readme>`_
    for usage instructions.''')

