#!python
# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name='redash2dqsql',
    version='0.1',

    install_requires=[
        'redash_toolbelt @ git+https://github.com/tusharm/redash-toolbelt',
        'sqlglot',
        'databricks-sdk',
        'click',
        'python-dotenv',
        'databricks-sql-connector',
    ],
    entry_points={
        'console_scripts': [
            'redash2dqsql=redash2dqsql.cli:main',
        ],
    }
)
