#!/usr/bin/env python
# encoding: utf-8


"""
@version: 0.1
@author: yangchao
@license: Apache Licence 
@contact: emailofyc@gmail.com
@site:  
@software: PyCharm
@file: orm.py
@time: 17-2-19 下午8:08
"""

import asyncio
import logging
import aiomysql


async def create_plll(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


if __name__ == '__main__':
    pass
