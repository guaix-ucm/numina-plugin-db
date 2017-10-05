#!/usr/bin/env python

from setuptools import setup, Extension
from setuptools import find_packages

import sys

setup(name='numinadb',
      version='0.1.dev0',
      author='Sergio Pascual',
      author_email='sergiopr@fis.ucm.es',
      url='http://guaix.fis.ucm.es/projects/numina',
      license='GPLv3',
      description='Numina Database Plugin',
      packages=find_packages('.'),
      install_requires=[
          "six",
          "sqlalchemy",
          "numina"
      ],
      entry_points={
        'numina_plugins.1': [
            'rundb = numinadb.rundb:register',
            ],
      },
      classifiers=[
                   "Programming Language :: C",
                   "Programming Language :: Cython",
                   "Programming Language :: Python :: 2.7",
                   "Programming Language :: Python :: 3.3",
                   "Programming Language :: Python :: 3.4",
                   "Programming Language :: Python :: 3.5",
                   'Development Status :: 3 - Alpha',
                   "Environment :: Other Environment",
                   "Intended Audience :: Science/Research",
                   "License :: OSI Approved :: GNU General Public License (GPL)",
                   "Operating System :: OS Independent",
                   "Topic :: Scientific/Engineering :: Astronomy",
                   ],
      long_description=open('README.rst').read()
      )
