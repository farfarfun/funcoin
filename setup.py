from funpypi import setup


install_requires = ["funbuild", "funsecret", "funfile", "pymysql", "ccxt", "pymysql", "fastapi", "uvicorn"]


setup(
    name="funcoin",
    install_requires=install_requires,
    entry_points={
        "console_scripts": [
            "funcoin = funcoin.server.script:funcoin",
            #"funcoin_server = funcoin.server.script:funcoin_server",
            #"funcoin_worker = funcoin.server.script:funcoin_worker",
        ]
    },
)
