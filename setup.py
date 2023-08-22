import time
from setuptools import find_packages, setup


install_requires = ["funbuild", "funsecret", "funfile", "pymysql", "ccxt", "pymysql"]


setup(
    name="funcoin",
    version=time.strftime("%Y%m%d%H%M", time.localtime()),
    description="funcoin",
    author="bingtao",
    author_email="1007530194@qq.com",
    url="https://github.com/1007530194",
    packages=find_packages(),
    package_data={"": ["*.js", "*.*"]},
    install_requires=install_requires,
    entry_points={
        "console_scripts": [
            "funcoin_server = funcoin.server.script:funcoin_server",
            "funcoin_worker = funcoin.server.script:funcoin_worker",
        ]
    },
)
