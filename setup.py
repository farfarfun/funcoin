import sys
from os import path

from darkbuild.tool import read_version
from setuptools import find_packages, setup
import time

install_requires = ['darkbuild', 'darksecret']

setup(name='darkcoins',
      version=time.strftime("%Y%m%d%H%M", time.localtime()),
      description='darkcoin',
      author='niult',
      author_email='1007530194@qq.com',
      url='https://github.com/1007530194',

      packages=find_packages(),
      package_data={"": ["*.js", "*.*"]},
      install_requires=install_requires,
      entry_points={'console_scripts': [
          'darkcoin_server = darkcoin.server.script:darkcoin_server',
          'darkcoin_worker = darkcoin.server.script:darkcoin_worker']},
      )
