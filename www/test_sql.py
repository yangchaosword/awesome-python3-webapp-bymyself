#!/usr/bin/env python
# encoding: utf-8


"""
@version: 0.1
@author: yangchao
@license: Apache Licence 
@contact: emailofyc@gmail.com
@site:  
@software: PyCharm
@file: test_sql.py
@time: 17-2-20 下午12:29
"""

import orm, asyncio
import logging;

logging.basicConfig(level=logging.INFO)
from models import User, Blog, Comment


async def test(loop):
    await orm.create_pool(loop=loop, user='www-data', password='www-data', db='awesome')

    u = User(name='Test', email='test@example.com', passwd='1234567890', image='about:blank')
    await u.save()
    logging.info('tesk ok')


loop = asyncio.get_event_loop()
loop.run_until_complete(test(loop))
loop.close()
