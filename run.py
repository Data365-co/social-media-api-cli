import asyncio
import os
import sys
import time
import traceback
from typing import Optional, Iterable, Literal

import click
import iso8601
from loguru import logger

import config
import modules.facebook.fetch
import modules.facebook.save
from modules import utils, context


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())
        sys.exit(2)

    config_errors = utils.validate_config()
    if config_errors:
        for error in config_errors:
            click.echo(error)
        sys.exit(2)


@cli.command('facebook-post')
@click.argument('target_items_ids', type=click.File(encoding='utf8'), default='-')
@click.option('--verbose/-v', is_flag=True, help='Enable debug messages')
@click.option('--restart', is_flag=True, help='Start fetching items from the beginning')
@click.option('--fetch-comments', is_flag=True, help='Enable comments fetching')
@click.option('--max-comments', type=int, default=None, help='Max number of comments to be fetched')
def facebook_post(
        target_items_ids: Iterable[str],
        *,
        verbose: bool,
        restart: bool,
        fetch_comments: bool,
        max_comments: Optional[int],
) -> None:
    """Fetch facebook posts.

    You must provide the list of IDs (not URLs) for the target posts as the first parameter of this script.

    \b
    Example:
        1745230722290442
        861864591213144
        3686330981405690
        575839316173883
        343706246413407

    Script will fetch posts + related comments (if flag --fetch-comments is set) and profiles using Social Media API,
    then store them in the PostgreSQL database or CSV files.

    You must set values for ACCESS_TOKEN variable before using the script.
    Configuration is stored in the config.py file.

    You can stop the script at any moment, next time it will start from where it left (if flag --restart is not set).

    If you want to change any parameters (--max-comments, --fetch-comments, etc), restart the script
    with --restart flag, otherwise new parameters will not be applied for already fetched items.
    """

    if verbose:
        log_level = 'DEBUG'
    else:
        log_level = 'INFO'
    logger.add(sys.stderr, filter=utils.__name__, level=log_level)

    if restart:
        if os.path.exists(config.TASK_CACHE_DB):
            os.remove(config.TASK_CACHE_DB)

    async def main() -> None:
        await context.context_init()
        try:
            await modules.facebook.save.init()
            coroutines = (
                (
                    modules.facebook.fetch.fetch_post(
                        item_id=item_id.strip(),
                        fetch_comments=fetch_comments,
                        max_comments=max_comments,
                        update=True,
                    ),
                    item_id.strip(),
                )
                for item_id in target_items_ids
            )
            await utils.schedule(coroutines)
        finally:
            await context.cleanup_context()

    try:
        asyncio.run(main())
    except Exception as exc:
        click.echo(f"Error: {type(exc).__name__}: {str(exc)}")
        etype, value, tb = sys.exc_info()
        with open(f"traceback_{int(time.time())}.txt", mode='w+', encoding='utf8') as fp:
            fp.write(''.join(traceback.format_exception(etype, value, tb)))
        sys.exit(1)


@cli.command('facebook-profile')
@click.argument('target_items_ids', type=click.File(encoding='utf8'), default='-')
@click.option('--verbose/-v', is_flag=True, help='Enable debug messages')
@click.option('--restart', is_flag=True, help='Start fetching items from the beginning')
@click.option('--fetch-feed-posts', is_flag=True, help='Enable posts fetching (from timeline section of the profile)')
@click.option('--fetch-community-posts', is_flag=True,
              help='Enable posts fetching (from community section of the profile)')
@click.option('--fetch-comments', is_flag=True, help='Enable comments fetching')
@click.option('--max-posts', type=int, default=None, help='Max number of posts to be fetched')
@click.option('--max-comments', type=int, default=None, help='Max number of comments to be fetched')
@click.option('--from-date', type=str, default=None,
              help='Fetch posts from this date. Date must be in iso8601 format (2021-01-01T02:16:32)')
@click.option('--to-date', type=str, default=None,
              help='Fetch posts to this date. Date must be in iso8601 format (2021-01-01T02:16:32)')
def facebook_profile(
        target_items_ids: Iterable[str],
        *,
        verbose: bool,
        restart: bool,
        fetch_feed_posts: bool,
        fetch_community_posts: bool,
        fetch_comments: bool,
        max_posts: Optional[int],
        max_comments: Optional[int],
        from_date: Optional[str],
        to_date: Optional[str],
) -> None:
    """Fetch facebook profiles.

    You must provide the list of IDs or usernames (not URLs) for the target profiles
    as the first parameter of this script.

    \b
    Example:
        1745230722290442
        4
        zuck
        andrey.smirnov
        343706246413407

    Script will fetch profiles + posts (if flag --fetch-feed-posts or --fetch-community-posts is set),
    related comments (if flag --fetch-comments is set) and profiles using Social Media API,
    then store them in the PostgreSQL database or CSV files.

    You must set values for ACCESS_TOKEN variable before using the script.
    Configuration is stored in the config.py file.

    You can stop the script at any moment, next time it will start from where it left (if flag --restart is not set).

    If you want to change any parameters (--max-comments, --fetch-comments, etc), or fetch new set of items,
    restart the script with --restart flag, otherwise new parameters will not be applied for already fetched items.
    """

    if verbose:
        log_level = 'DEBUG'
    else:
        log_level = 'INFO'
    logger.add(sys.stderr, filter=utils.__name__, level=log_level)

    if from_date:
        from_date = iso8601.parse_date(from_date)
    if to_date:
        to_date = iso8601.parse_date(to_date)

    if restart:
        if os.path.exists(config.TASK_CACHE_DB):
            os.remove(config.TASK_CACHE_DB)

    async def main() -> None:
        await context.context_init()
        try:
            await modules.facebook.save.init()
            coroutines = (
                (
                    modules.facebook.fetch.fetch_profile(
                        item_id=item_id.strip(),
                        fetch_feed_posts=fetch_feed_posts,
                        fetch_community_posts=fetch_community_posts,
                        fetch_comments=fetch_comments,
                        max_posts=max_posts,
                        max_comments=max_comments,
                        from_date=from_date,
                        to_date=to_date,
                        update=True,
                    ),
                    item_id.strip(),
                )
                for item_id in target_items_ids
            )
            await utils.schedule(coroutines)
        finally:
            await context.cleanup_context()

    try:
        asyncio.run(main())
    except Exception as exc:
        click.echo(f"Error: {type(exc).__name__}: {str(exc)}")
        etype, value, tb = sys.exc_info()
        with open(f"traceback_{int(time.time())}.txt", mode='w+', encoding='utf8') as fp:
            fp.write(''.join(traceback.format_exception(etype, value, tb)))
        sys.exit(1)


@cli.command('facebook-search-posts')
@click.argument('target_requests', type=click.File(encoding='utf8'), default='-')
@click.option('--verbose/-v', is_flag=True, help='Enable debug messages')
@click.option('--restart', is_flag=True, help='Start fetching items from the beginning')
@click.option('--fetch-comments', is_flag=True, help='Enable comments fetching')
@click.option('--max-posts', type=int, default=None, help='Max number of posts to be fetched')
@click.option('--max-comments', type=int, default=None, help='Max number of comments to be fetched')
@click.option('--from-date', type=str, default=None,
              help='Fetch posts from this date. Date must be in iso8601 format (2021-01-01T02:16:32)')
@click.option('--to-date', type=str, default=None,
              help='Fetch posts to this date. Date must be in iso8601 format (2021-01-01T02:16:32)')
@click.option('--search-type', type=click.Choice(('top', 'latest', 'hashtag')), default='top',
              help='Fetch posts to this date. Date must be in iso8601 format (2021-01-01T02:16:32)')
def facebook_search_for_posts(
        target_requests: Iterable[str],
        *,
        verbose: bool,
        restart: bool,
        fetch_comments: bool,
        max_posts: Optional[int],
        max_comments: Optional[int],
        from_date: Optional[str],
        to_date: Optional[str],
        search_type: Literal['top', 'latest', 'hashtag'],
) -> None:
    """Fetch facebook searches for posts.

    You must provide the list of requests for the target searches
    as the first parameter of this script.

    \b
    Example:
        Google
        Facebook
        Microsoft
        IBM

    Script will fetch posts, related comments (if flag --fetch-comments is set) and profiles using Social Media API,
    then store them in the PostgreSQL database or CSV files.

    You must set values for ACCESS_TOKEN variable before using the script.
    Configuration is stored in the config.py file.

    You can stop the script at any moment, next time it will start from where it left (if flag --restart is not set).

    If you want to change any parameters (--max-comments, --fetch-comments, etc), or fetch new set of items,
    restart the script with --restart flag, otherwise new parameters will not be applied for already fetched items.
    """

    if verbose:
        log_level = 'DEBUG'
    else:
        log_level = 'INFO'
    logger.add(sys.stderr, filter=utils.__name__, level=log_level)

    if from_date:
        from_date = iso8601.parse_date(from_date)
    if to_date:
        to_date = iso8601.parse_date(to_date)

    if restart:
        if os.path.exists(config.TASK_CACHE_DB):
            os.remove(config.TASK_CACHE_DB)

    async def main() -> None:
        await context.context_init()
        try:
            await modules.facebook.save.init()
            coroutines = (
                (
                    modules.facebook.fetch.fetch_search_for_posts(
                        request=request.strip().casefold(),
                        fetch_comments=fetch_comments,
                        max_posts=max_posts,
                        max_comments=max_comments,
                        from_date=from_date,
                        to_date=to_date,
                        search_type=search_type,
                        update=True,
                    ),
                    request.strip().casefold(),
                )
                for request in target_requests
            )
            await utils.schedule(coroutines)
        finally:
            await context.cleanup_context()

    try:
        asyncio.run(main())
    except Exception as exc:
        click.echo(f"Error: {type(exc).__name__}: {str(exc)}")
        etype, value, tb = sys.exc_info()
        with open(f"traceback_{int(time.time())}.txt", mode='w+', encoding='utf8') as fp:
            fp.write(''.join(traceback.format_exception(etype, value, tb)))
        sys.exit(1)


if __name__ == '__main__':
    cli()
