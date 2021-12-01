import asyncio
import os
from contextvars import ContextVar
from typing import Optional

import aiohttp
import aiosqlite
import asyncpg

import config
from modules import utils

request_sync: ContextVar[asyncio.Semaphore] = ContextVar('request_sync')
request_session: ContextVar[aiohttp.ClientSession] = ContextVar('request_session')
db_task_cache: ContextVar[aiosqlite.Connection] = ContextVar('db_task_cache')
db_pg_pool: ContextVar[Optional[asyncpg.Pool]] = ContextVar('db_pg_pool', default=None)


async def context_init() -> None:
    request_sync.set(asyncio.Semaphore(config.MAX_THREADS))
    request_session.set(aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT_S),
    ))

    db_task_cache.set(await aiosqlite.connect(config.TASK_CACHE_DB))
    await utils.init_task_cache_db()

    if config.OUTPUT_FORMAT == 'postgres':
        db_pg_pool.set(await asyncpg.create_pool(config.POSTGRES_DSN))
    elif config.OUTPUT_FORMAT == 'csv':
        os.makedirs(config.CSV_PATH, exist_ok=True)


async def cleanup_context() -> None:
    await request_session.get().close()
    await db_task_cache.get().close()
    if db_pg_pool.get():
        await db_pg_pool.get().close()
