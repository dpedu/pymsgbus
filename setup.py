#!/usr/bin/env python3
from setuptools import setup

from msgbus import __version__

setup(name='msgbus',
      version=__version__,
      description='pubsub communication tools',
      url='http://gitlab.xmopx.net/dave/pymsgbus',
      author='dpedu',
      author_email='dave@davepedu.com',
      packages=['msgbus'],
      entry_points={
          "console_scripts": [
              "mbusd = msgbus.server:main",
              "mbuspub = msgbus.pub:main",
              "mbussub = msgbus.sub:main",
          ]
      })
