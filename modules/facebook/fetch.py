import asyncio
import datetime
import random
from typing import Optional, Dict, List, Literal, Coroutine

from more_itertools import ichunked

import config
from modules import utils
from modules.facebook.save import save_comments, save_posts, save_profiles, save_searches_for_posts, save_connections


async def fetch_comment(
        item_id: str,
        *,
        this_item: Optional[Dict] = None,
        fetch_comments: bool = False,
        max_comments: Optional[int] = None,
) -> None:
    task_status = await utils.get_task_status(item_id)
    if task_status == 'finished':
        return

    params = {
        'load_comments': int(fetch_comments),
        'max_comments': max_comments,
    }

    tasks: List[Coroutine] = []

    if this_item:
        comment = this_item
    else:
        comment = await utils.get_item(f"facebook/comment/{item_id}", params)
        if comment is not None:
            tasks.append(
                save_comments([comment])
            )

    if comment is not None:
        if comment['owner_id']:
            tasks.append(
                fetch_profile(comment['owner_id'])
            )

        if fetch_comments:
            if comment['comments_count']:
                replies_fetched_count = 0
                async for replies_batch in utils.get_collection(
                        f"facebook/comment/{item_id}/replies", {**params, 'order_by': 'date_desc'}
                ):
                    tasks.extend(
                        fetch_comment(
                            item_id=reply['id'],
                            this_item=reply,
                            fetch_comments=fetch_comments,
                            max_comments=max_comments,
                        ) for reply in replies_batch
                    )
                    tasks.append(
                        save_comments(replies_batch)
                    )
                    tasks.append(
                        save_connections(
                            parent_item_id=comment['id'],
                            item_ids=(reply['id'] for reply in replies_batch),
                            collection='facebook/comment/replies',
                        )
                    )

                    replies_fetched_count += len(replies_batch)
                    if max_comments is not None and replies_fetched_count >= max_comments:
                        break

                    for tasks_batch in ichunked(tasks, config.TASKS_BATCH_SIZE):
                        await asyncio.gather(*tasks_batch)
                    tasks.clear()

    for tasks_batch in ichunked(tasks, config.TASKS_BATCH_SIZE):
        await asyncio.gather(*tasks_batch)
    tasks.clear()

    await utils.set_task_status(item_id, 'finished')


async def fetch_post(
        item_id: str,
        *,
        update: bool = False,
        this_item: Optional[Dict] = None,
        fetch_comments: bool = False,
        max_comments: Optional[int] = None,
) -> None:
    task_status = await utils.get_task_status(item_id)
    if task_status == 'finished':
        return

    params = {
        'load_comments': int(fetch_comments),
        'max_comments': max_comments,
    }

    if update:
        if task_status is None:
            await utils.request_update(f"facebook/post/{item_id}", params)
            await utils.set_task_status(item_id, 'created')
        while True:
            await asyncio.sleep(config.UPDATE_CHECK_PERIOD_S * random.uniform(0.75, 1.25))
            task_status = await utils.get_update_status(f"facebook/post/{item_id}", params)
            if task_status in ('finished', 'fail', 'unknown'):
                await utils.set_task_status(item_id, 'collecting')
                break

    tasks: List[Coroutine] = []

    if this_item:
        post = this_item
    else:
        post = await utils.get_item(f"facebook/post/{item_id}", params)
        if post is not None:
            tasks.append(
                save_posts([post])
            )

    if post is not None:
        if post['owner_id']:
            tasks.append(
                fetch_profile(post['owner_id'])
            )

        if post['group_id']:
            tasks.append(
                fetch_profile(post['group_id'])
            )

        if fetch_comments:
            if post['comments_count']:
                comments_fetched_count = 0
                async for comments_batch in utils.get_collection(
                        f"facebook/post/{item_id}/comments", {**params, 'order_by': 'date_desc'}
                ):
                    tasks.extend(
                        fetch_comment(
                            item_id=comment['id'],
                            this_item=comment,
                            fetch_comments=fetch_comments,
                            max_comments=max_comments,
                        ) for comment in comments_batch
                    )
                    tasks.append(
                        save_comments(comments_batch)
                    )
                    tasks.append(
                        save_connections(
                            parent_item_id=post['id'],
                            item_ids=(comment['id'] for comment in comments_batch),
                            collection='facebook/post/comments',
                        )
                    )

                    comments_fetched_count += len(comments_batch)
                    if max_comments is not None and comments_fetched_count >= max_comments:
                        break

                    for tasks_batch in ichunked(tasks, config.TASKS_BATCH_SIZE):
                        await asyncio.gather(*tasks_batch)
                    tasks.clear()

    for tasks_batch in ichunked(tasks, config.TASKS_BATCH_SIZE):
        await asyncio.gather(*tasks_batch)
    tasks.clear()

    await utils.set_task_status(item_id, 'finished')


async def fetch_profile(
        item_id: str,
        *,
        update: bool = False,
        this_item: Optional[Dict] = None,
        fetch_feed_posts: bool = False,
        fetch_community_posts: bool = False,
        fetch_comments: bool = False,
        max_posts: Optional[int] = None,
        max_comments: Optional[int] = None,
        from_date: Optional[datetime.datetime] = None,
        to_date: Optional[datetime.datetime] = None,
) -> None:
    task_status = await utils.get_task_status(item_id)
    if task_status == 'finished':
        return

    params = {
        'from_date': from_date.isoformat() if from_date is not None else None,
        'to_date': to_date.isoformat() if to_date is not None else None,
        'load_feed_posts': int(fetch_feed_posts),
        'load_community_posts': int(fetch_community_posts),
        'load_comments': int(fetch_comments),
        'max_posts': max_posts,
        'max_comments': max_comments,
    }

    if update:
        task_status = await utils.get_task_status(item_id)
        if task_status is None:
            await utils.request_update(f"facebook/profile/{item_id}", params)
            await utils.set_task_status(item_id, 'created')
        while True:
            await asyncio.sleep(config.UPDATE_CHECK_PERIOD_S * random.uniform(0.75, 1.25))
            task_status = await utils.get_update_status(f"facebook/profile/{item_id}", params)
            if task_status in ('finished', 'fail', 'unknown'):
                await utils.set_task_status(item_id, 'collecting')
                break

    tasks: List[Coroutine] = []

    if this_item:
        profile = this_item
    else:
        profile = await utils.get_item(f"facebook/profile/{item_id}", params)
        if profile is not None:
            tasks.append(
                save_profiles([profile])
            )

    if profile is not None:
        if fetch_feed_posts:
            feed_posts_fetched_count = 0
            async for posts_batch in utils.get_collection(
                    f"facebook/profile/{item_id}/feed/posts", {**params, 'order_by': 'date_desc'}
            ):
                tasks.extend(
                    fetch_post(
                        item_id=post['id'],
                        this_item=post,
                        fetch_comments=fetch_comments,
                        max_comments=max_comments,
                    )
                    for post in posts_batch
                )
                tasks.append(
                    save_posts(posts_batch)
                )
                tasks.append(
                    save_connections(
                        parent_item_id=profile['id'],
                        item_ids=(post['id'] for post in posts_batch),
                        collection='facebook/profile/feed/posts',
                    )
                )

                feed_posts_fetched_count += len(posts_batch)
                if max_posts is not None and feed_posts_fetched_count >= max_posts:
                    break

                for tasks_batch in ichunked(tasks, config.TASKS_BATCH_SIZE):
                    await asyncio.gather(*tasks_batch)
                tasks.clear()

        if fetch_community_posts:
            community_posts_fetched_count = 0
            async for posts_batch in utils.get_collection(
                    f"facebook/profile/{item_id}/community/posts", {**params, 'order_by': 'date_desc'}
            ):
                tasks.extend(
                    fetch_post(
                        item_id=post['id'],
                        this_item=post,
                        fetch_comments=fetch_comments,
                        max_comments=max_comments,
                    )
                    for post in posts_batch
                )
                tasks.append(
                    save_posts(posts_batch)
                )
                tasks.append(
                    save_connections(
                        parent_item_id=profile['id'],
                        item_ids=(post['id'] for post in posts_batch),
                        collection='facebook/profile/community/posts',
                    )
                )

                community_posts_fetched_count += len(posts_batch)
                if max_posts is not None and community_posts_fetched_count >= max_posts:
                    break

                for tasks_batch in ichunked(tasks, config.TASKS_BATCH_SIZE):
                    await asyncio.gather(*tasks_batch)
                tasks.clear()

    for tasks_batch in ichunked(tasks, config.TASKS_BATCH_SIZE):
        await asyncio.gather(*tasks_batch)
    tasks.clear()

    await utils.set_task_status(item_id, 'finished')


async def fetch_search_for_posts(
        request: str,
        *,
        update: bool = False,
        this_item: Optional[Dict] = None,
        fetch_comments: bool = False,
        max_posts: Optional[int] = None,
        max_comments: Optional[int] = None,
        from_date: Optional[datetime.datetime] = None,
        to_date: Optional[datetime.datetime] = None,
        search_type: Literal['top', 'latest', 'hashtag'] = 'top',
) -> None:
    item_id = '/'.join((
        request.casefold(),
        (from_date.isoformat() if from_date is not None else ''),
        (to_date.isoformat() if to_date is not None else ''),
        search_type,
    ))

    task_status = await utils.get_task_status(item_id)
    if task_status == 'finished':
        return

    params = {
        'from_date': from_date.isoformat() if from_date is not None else None,
        'to_date': to_date.isoformat() if to_date is not None else None,
        'load_comments': int(fetch_comments),
        'max_posts': max_posts,
        'max_comments': max_comments,
    }

    if update:
        task_status = await utils.get_task_status(item_id)
        if task_status is None:
            await utils.request_update(f"facebook/search/{request}/posts/{search_type}", params)
            await utils.set_task_status(item_id, 'created')
        while True:
            await asyncio.sleep(config.UPDATE_CHECK_PERIOD_S * random.uniform(0.75, 1.25))
            task_status = await utils.get_update_status(f"facebook/search/{request}/posts/{search_type}", params)
            if task_status in ('finished', 'fail', 'unknown'):
                await utils.set_task_status(item_id, 'collecting')
                break

    tasks: List[Coroutine] = []

    if this_item:
        search = this_item
    else:
        search = await utils.get_item(f"facebook/search/{request}/posts/{search_type}", params)
        if search is not None:
            tasks.append(
                save_searches_for_posts([search])
            )

    if search is not None:
        posts_fetched_count = 0
        async for posts_batch in utils.get_collection(
                f"facebook/search/{request}/posts/{search_type}/posts", {**params, 'order_by': 'date_desc'}
        ):
            tasks.extend(
                fetch_post(
                    item_id=post['id'],
                    this_item=post,
                    fetch_comments=fetch_comments,
                    max_comments=max_comments,
                )
                for post in posts_batch
            )
            tasks.append(
                save_posts(posts_batch)
            )
            tasks.append(
                save_connections(
                    parent_item_id=search['id'],
                    item_ids=(post['id'] for post in posts_batch),
                    collection='facebook/search/posts',
                )
            )

            posts_fetched_count += len(posts_batch)
            if max_posts is not None and posts_fetched_count >= max_posts:
                break

            for tasks_batch in ichunked(tasks, config.TASKS_BATCH_SIZE):
                await asyncio.gather(*tasks_batch)
            tasks.clear()

    for tasks_batch in ichunked(tasks, config.TASKS_BATCH_SIZE):
        await asyncio.gather(*tasks_batch)
    tasks.clear()

    await utils.set_task_status(item_id, 'finished')
