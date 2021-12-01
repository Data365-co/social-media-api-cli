import asyncio
import csv
import dataclasses
import random
from itertools import count
from typing import Literal, Dict, NewType, Optional, AsyncGenerator, List, Iterable, Set, Coroutine, Tuple

import aiohttp
from async_timeout import timeout
from loguru import logger
from more_itertools import unique_everseen, ichunked

import config
from modules import context, exceptions

logger.remove()

Url = NewType('Url', str)


@dataclasses.dataclass
class ApiResponse:
    data: Optional[Dict]
    error: Optional[Dict]
    status: str


async def init_task_cache_db() -> None:
    sql = (
        'CREATE TABLE IF NOT EXISTS tasks (item_id text PRIMARY KEY, status text);'
    )
    db_conn = context.db_task_cache.get()
    try:
        await db_conn.execute(sql)
    finally:
        await db_conn.commit()


async def set_task_status(item_id: str, status: str) -> None:
    sql = (
        'INSERT OR REPLACE INTO tasks VALUES (?, ?);'
    )
    db_conn = context.db_task_cache.get()
    try:
        await db_conn.execute(sql, (item_id, status))
    finally:
        await db_conn.commit()


async def get_task_status(item_id: str) -> Optional[str]:
    sql = (
        'SELECT status FROM tasks WHERE item_id = ?'
    )
    db_conn = context.db_task_cache.get()
    try:
        async with db_conn.execute(sql, (item_id,)) as db_cursor:
            row = await db_cursor.fetchone()
            if row:
                return row[0]
            return None
    finally:
        await db_conn.commit()


async def save_items_to_csv(
        items: Iterable[Dict],
        *,
        table: str,
        pk_fields: Iterable[str] = ('id',),
) -> None:
    items = tuple(
        unique_everseen(
            iterable=items,
            key=lambda row: tuple(row[field] for field in pk_fields),
        )
    )
    if not items:
        return

    fields = sorted(items[0].keys())
    csv_file_path = config.CSV_PATH / f"{table}.csv"
    fp = None
    try:
        try:
            fp = csv_file_path.open(mode='x', encoding='utf8', newline='')
        except FileExistsError:
            fp = csv_file_path.open(mode='a', encoding='utf8', newline='')
            csv_writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
        else:
            fp.write('\ufeff')
            csv_writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
            csv_writer.writerow(fields)
        csv_writer.writerows(
            (item[field] for field in fields) for item in items
        )
    finally:
        if fp is not None:
            fp.close()


async def save_items_to_postgres(
        items: Iterable[Dict],
        *,
        table: str,
        pk_fields: Iterable[str] = ('id',),
) -> None:
    items = tuple(
        unique_everseen(
            iterable=items,
            key=lambda row: tuple(row[field] for field in pk_fields),
        )
    )
    if not items:
        return

    pk_fields = tuple(pk_fields)
    fields = tuple(items[0].keys())

    fields_list = ', '.join(fields)
    pk_fields_list = ', '.join(pk_fields)
    values_template = ', '.join(f"${n+1}" for n in range(len(fields)))

    fields_to_update = [field for field in fields if field not in pk_fields]
    if fields_to_update:
        update_cmd = 'UPDATE SET ' + ', '.join(
            f"{field} = COALESCE(EXCLUDED.{field}, {table}.{field})" for field in fields_to_update
        )
    else:
        update_cmd = 'NOTHING'

    sql = (
        f" INSERT INTO {config.POSTGRES_SCHEMA}.{table} ({fields_list})"
        f" VALUES ({values_template})"
        f" ON CONFLICT ({pk_fields_list}) DO {update_cmd}"
    )

    db_pool = context.db_pg_pool.get()
    async with db_pool.acquire() as db_conn:
        for items_batch in ichunked(items, 15):
            values = [
                [item[field] for field in fields]
                for item in items_batch
            ]
            await db_conn.executemany(sql, values)


async def make_request(
        method: Literal['get', 'post'],
        url: Url,
        params: Dict,
) -> ApiResponse:
    clean_params = {k: v for k, v in params.items() if v is not None}
    params = clean_params.copy()
    params['access_token'] = config.ACCESS_TOKEN

    async with context.request_sync.get():
        for n in count():
            try:
                async with timeout(config.REQUEST_TIMEOUT_S):
                    async with context.request_session.get().request(method, url, params=params) as resp:
                        resp_data = await resp.json()
                        status_code = resp.status
            except (
                    asyncio.TimeoutError,
                    aiohttp.ClientConnectionError,
                    aiohttp.ServerTimeoutError,
                    aiohttp.ServerDisconnectedError,
                    aiohttp.ServerConnectionError,
            ):
                logger.debug(f"{method.upper():6} [{'TO':3}] {'-':10} {url} {clean_params}")
                await asyncio.sleep(min(n, 5) * random.uniform(0.75, 1.25))
            else:
                logger.debug(f"{method.upper():6} [{status_code:3}] {resp_data['status']:10} {url} {clean_params}")
                if status_code == 429:
                    await asyncio.sleep(min(n, 5) * random.uniform(0.75, 1.25))
                else:
                    return ApiResponse(**resp_data)


async def request_update(url: str, params: Dict) -> Optional[str]:
    url = Url(f"{config.API_URL}/{url}/update")
    resp = await make_request('post', url, params)
    if resp.status != 'accepted':
        raise exceptions.ApiRequestError(
            f"Failed to request update: status={resp.status}, url={url}, error={resp.error}"
        )
    return resp.data['task_id']


async def get_update_status(url: str, params: Dict) -> Literal['finished', 'failed', 'unknown', 'created', 'pending']:
    url = Url(f"{config.API_URL}/{url}/update")
    resp = await make_request('get', url, params)
    if resp.status != 'ok':
        raise exceptions.ApiRequestError(
            f"Failed to get update status: status={resp.status}, url={url}, error={resp.error}"
        )
    return resp.data['status']


async def get_item(url: str, params: Dict) -> Optional[Dict]:
    url = Url(f"{config.API_URL}/{url}")
    resp = await make_request('get', url, params)
    if resp.status == 'fail' and resp.error['code'] == 'NotFoundError':
        return None
    if resp.status != 'ok':
        raise exceptions.ApiRequestError(
            f"Failed to get item: status={resp.status}, url={url}, error={resp.error}"
        )
    return resp.data


async def get_collection(url: str, params: Dict) -> AsyncGenerator[List[Dict], None]:
    url = Url(f"{config.API_URL}/{url}")

    params = params.copy()
    params['cursor'] = None
    params['max_page_size'] = 100

    while True:
        resp = await make_request('get', url, params)
        if resp.status != 'ok':
            raise exceptions.ApiRequestError(
                f"Failed to get collection: status={resp.status}, url={url}, error={resp.error}"
            )
        if not resp.data['items']:
            return
        yield resp.data['items']
        if not resp.data['page_info']['has_next_page']:
            break
        params['cursor'] = resp.data['page_info']['cursor']


async def schedule(coroutines: Iterable[Tuple[Coroutine, str]]) -> None:
    coroutines = iter(coroutines)
    has_more_coroutines = True
    tasks: Set[asyncio.Task] = set()
    done: Set[asyncio.Task]
    finished_count = 0
    total_count = 0
    while True:
        while has_more_coroutines and len(tasks) < config.MAX_QUEUE_SIZE:
            try:
                coro, name = next(coroutines)
            except StopIteration:
                has_more_coroutines = False
            else:
                total_count += 1
                tasks.add(asyncio.create_task(coro, name=name))
        if not tasks and not has_more_coroutines:
            break
        done, tasks = await asyncio.wait(tasks, timeout=5, return_when=asyncio.FIRST_EXCEPTION)
        try:
            if done:
                await asyncio.gather(*done)
                finished_count += len(done)
                logger.info(f"Finished {finished_count}/{total_count}")
        except Exception:
            for task in tasks:
                task.cancel()
            raise


def validate_config() -> List[str]:
    errors: List[str] = []

    for var_name in (
            'ACCESS_TOKEN',
            'TASK_CACHE_DB',
            'OUTPUT_FORMAT',

            'API_BASE_URL',
            'API_VERSION',
            'API_URL',

            'REQUEST_TIMEOUT_S',
            'UPDATE_CHECK_PERIOD_S',
            'MAX_THREADS',
            'MAX_QUEUE_SIZE',
    ):
        if not hasattr(config, var_name) or not getattr(config, var_name):
            errors.append(
                f"You must set value for the {var_name} variable in the config file before using the script."
            )
    if errors:
        return errors

    if config.OUTPUT_FORMAT == 'postgres':
        if not hasattr(config, 'POSTGRES_DSN') or not config.POSTGRES_DSN:
            errors.append(
                'You must set value for the POSTGRES_DSN variable in the config file before using the script.'
            )
        if not hasattr(config, 'POSTGRES_SCHEMA') or not config.POSTGRES_SCHEMA:
            errors.append(
                'You must set value for the POSTGRES_SCHEMA variable in the config file before using the script.'
            )
    elif config.OUTPUT_FORMAT == 'csv':
        if not hasattr(config, 'CSV_PATH') or not config.CSV_PATH:
            errors.append(
                'You must set value for the CSV_PATH variable in the config before using the script.'
            )
    else:
        errors.append(
            f"Value '{config.OUTPUT_FORMAT}' for the variable OUTPUT_FORMAT is invalid."
        )

    return errors
