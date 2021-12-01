import csv
import datetime
import os
import re
from typing import List, Dict, Union, Iterable, Set, Tuple

import config
from modules import context, utils

saved_posts: Set[int] = set()
saved_comments: Set[int] = set()
saved_profiles: Set[int] = set()
saved_searches_for_posts: Set[str] = set()
saved_connections: Set[Tuple[int, str, str]] = set()


async def init() -> None:
    if config.OUTPUT_FORMAT == 'postgres':
        sql_file_path = os.path.join(os.path.dirname(__file__), 'init_postgres_db.sql')
        with open(sql_file_path, mode='r', encoding='utf8') as fp:
            sql = fp.read().format(schema_name=config.POSTGRES_SCHEMA)
        async with context.db_pg_pool.get().acquire() as db_conn:
            await db_conn.execute(sql)
    elif config.OUTPUT_FORMAT == 'csv':
        try:
            with open(config.CSV_PATH / 'facebook_posts.csv', mode='r', encoding='utf8', newline='') as fp:
                fp.read(1)
                saved_posts.update(int(item['id']) for item in csv.DictReader(fp))
        except FileNotFoundError:
            pass
        try:
            with open(config.CSV_PATH / 'facebook_comments.csv', mode='r', encoding='utf8', newline='') as fp:
                fp.read(1)
                saved_comments.update(int(item['id']) for item in csv.DictReader(fp))
        except FileNotFoundError:
            pass
        try:
            with open(config.CSV_PATH / 'facebook_profiles.csv', mode='r', encoding='utf8', newline='') as fp:
                fp.read(1)
                saved_profiles.update(int(item['id']) for item in csv.DictReader(fp))
        except FileNotFoundError:
            pass
        try:
            with open(config.CSV_PATH / 'facebook_searches_for_posts.csv', mode='r', encoding='utf8', newline='') as fp:
                fp.read(1)
                saved_searches_for_posts.update(item['id'] for item in csv.DictReader(fp))
        except FileNotFoundError:
            pass
        try:
            with open(config.CSV_PATH / 'facebook_connections.csv', mode='r', encoding='utf8', newline='') as fp:
                fp.read(1)
                saved_connections.update(
                    (int(item['id']), str(item['parent_id']), item['collection']) for item in csv.DictReader(fp)
                )
        except FileNotFoundError:
            pass


def normalize_post_pg(item: Dict) -> Dict:
    item['id'] = int(item['id'])
    item['created_time'] = (datetime.datetime.fromisoformat(item['created_time'])
                            if item['created_time'] else None)
    item['text_tagged_users'] = ([int(el) for el in item['text_tagged_users']]
                                 if item['text_tagged_users'] else None)
    item['attached_medias_id'] = ([int(el) for el in item['attached_medias_id']]
                                  if item['attached_medias_id'] else None)
    item['attached_post_id'] = int(item['attached_post_id']) if item['attached_post_id'] else None
    item['owner_id'] = int(item['owner_id']) if item['owner_id'] else None
    item['group_id'] = int(item['group_id']) if item['group_id'] else None
    return item


def normalize_post_csv(item: Dict) -> Dict:
    item['attached_medias_id'] = ';'.join(item['attached_medias_id'] or [])
    item['attached_medias_preview_content'] = ';'.join(item['attached_medias_preview_content'] or [])
    item['attached_medias_preview_url'] = ';'.join(item['attached_medias_preview_url'] or [])
    item['text_tagged_users'] = ';'.join(item['text_tagged_users'] or [])
    item['text_tags'] = ';'.join(item['text_tags'] or [])
    item['text'] = ' '.join(re.split(r"\s+", item['text'] or ''))
    return item


async def save_posts(items: List[Dict]) -> None:
    if config.OUTPUT_FORMAT == 'postgres':
        await utils.save_items_to_postgres(
            items=(normalize_post_pg(el) for el in items),
            table='facebook_posts',
        )
    elif config.OUTPUT_FORMAT == 'csv':
        items_to_save = (item for item in items if int(item['id']) not in saved_posts)
        await utils.save_items_to_csv(
            items=(normalize_post_csv(item) for item in items_to_save),
            table='facebook_posts',
        )
        saved_posts.update(int(item['id']) for item in items)
    else:
        raise ValueError(f"Unknown output format: {config.OUTPUT_FORMAT}")


def normalize_comment_pg(item: Dict) -> Dict:
    item['id'] = int(item['id'])
    item['parent_id'] = int(item['parent_id'])
    item['created_time'] = datetime.datetime.fromisoformat(item['created_time']) if item['created_time'] else None
    item['text_tagged_users'] = ([int(el) for el in item['text_tagged_users']]
                                 if item['text_tagged_users'] else None)
    item['owner_id'] = int(item['owner_id']) if item['owner_id'] else None
    return item


def normalize_comment_csv(item: Dict) -> Dict:
    item['text_tagged_users'] = ';'.join(item['text_tagged_users'] or [])
    item['text_tags'] = ';'.join(item['text_tags'] or [])
    item['text'] = ' '.join(re.split(r"\s+", item['text'] or ''))
    return item


async def save_comments(items: List[Dict]) -> None:
    if config.OUTPUT_FORMAT == 'postgres':
        await utils.save_items_to_postgres(
            items=(normalize_comment_pg(el) for el in items),
            table='facebook_comments',
        )
    elif config.OUTPUT_FORMAT == 'csv':
        items_to_save = (item for item in items if int(item['id']) not in saved_comments)
        await utils.save_items_to_csv(
            items=(normalize_comment_csv(item) for item in items_to_save),
            table='facebook_comments',
        )
        saved_comments.update(int(item['id']) for item in items)
    else:
        raise ValueError(f"Unknown output format: {config.OUTPUT_FORMAT}")


def normalize_profile_pg(item: Dict) -> Dict:
    item['id'] = int(item['id'])
    item['last_post_created_time'] = (datetime.datetime.fromisoformat(item['last_post_created_time'])
                                      if item['last_post_created_time'] else None)
    return item


def normalize_profile_csv(item: Dict) -> Dict:
    item['categories'] = ';'.join(item['categories'] or [])
    item['current_city'] = ';'.join(item['current_city'] or [])
    item['education'] = ';'.join(item['education'] or [])
    item['hometown'] = ';'.join(item['hometown'] or [])
    item['workplace'] = ';'.join(item['workplace'] or [])
    item['langs'] = ';'.join(item['langs'] or [])
    item['biography'] = ' '.join(re.split(r"\s+", item['biography'] or ''))
    return item


async def save_profiles(items: List[Dict]) -> None:
    if config.OUTPUT_FORMAT == 'postgres':
        await utils.save_items_to_postgres(
            items=(normalize_profile_pg(el) for el in items),
            table='facebook_profiles',
        )
    elif config.OUTPUT_FORMAT == 'csv':
        items_to_save = (item for item in items if int(item['id']) not in saved_profiles)
        await utils.save_items_to_csv(
            items=(normalize_profile_csv(item) for item in items_to_save),
            table='facebook_profiles',
        )
        saved_profiles.update(int(item['id']) for item in items)
    else:
        raise ValueError(f"Unknown output format: {config.OUTPUT_FORMAT}")


def normalize_searches_for_posts_pg(item: Dict) -> Dict:
    item['location_id'] = int(item['location_id']) if item['location_id'] else None
    item['author_id'] = int(item['author_id']) if item['author_id'] else None
    item['from_date'] = datetime.datetime.fromisoformat(item['from_date']) if item['from_date'] else None
    item['to_date'] = datetime.datetime.fromisoformat(item['to_date']) if item['to_date'] else None
    del item['post_count']
    return item


def normalize_searches_for_posts_csv(item: Dict) -> Dict:
    del item['post_count']
    return item


async def save_searches_for_posts(items: List[Dict]) -> None:
    if config.OUTPUT_FORMAT == 'postgres':
        await utils.save_items_to_postgres(
            items=(normalize_searches_for_posts_pg(el) for el in items),
            table='facebook_searches_for_posts',
        )
    elif config.OUTPUT_FORMAT == 'csv':
        items_to_save = (item for item in items if item['id'] not in saved_searches_for_posts)
        await utils.save_items_to_csv(
            items=(normalize_searches_for_posts_csv(item) for item in items_to_save),
            table='facebook_searches_for_posts',
        )
        saved_searches_for_posts.update(item['id'] for item in items)
    else:
        raise ValueError(f"Unknown output format: {config.OUTPUT_FORMAT}")


async def save_connections(
        parent_item_id: Union[str, int],
        item_ids: Iterable[Union[str, int]],
        collection: str,
) -> None:
    items = [
        {
            'id': int(item_id),
            'parent_id': str(parent_item_id),
            'collection': collection,
        }
        for item_id in item_ids
    ]
    if config.OUTPUT_FORMAT == 'postgres':
        await utils.save_items_to_postgres(
            items=items,
            table='facebook_connections',
            pk_fields=('id', 'parent_id', 'collection'),
        )
    elif config.OUTPUT_FORMAT == 'csv':
        items_to_save = (
            item for item in items
            if (item['id'], item['parent_id'], item['collection']) not in saved_connections
        )
        await utils.save_items_to_csv(
            items=items_to_save,
            table='facebook_connections',
            pk_fields=('id', 'parent_id', 'collection'),
        )
        saved_connections.update((item['id'], item['parent_id'], item['collection']) for item in items)
    else:
        raise ValueError(f"Unknown output format: {config.OUTPUT_FORMAT}")
