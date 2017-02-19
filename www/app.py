#!/usr/bin/env python
# encoding: utf-8


"""
@version: 0.1
@author: yangchao
@license: Apache Licence 
@contact: emailofyc@gmail.com
@site:  
@software: PyCharm
@file: app.py
@time: 17-2-19 下午6:31
"""

import logging;

logging.basicConfig(level=logging.INFO)
import asyncio, os, json, time
from datetime import datetime
from aiohttp import web


def index(request):
    return web.Response(body=b'<h1>Awesome</h1>', content_type='text/html')


async def init(loop):
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', index)
    srv = await loop.create_server(app.make_handler(), '127.0.0.1', 9000)
    logging.info('server started at http://127.0.0.1:9000...')
    return srv


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init(loop))
    loop.run_forever()
