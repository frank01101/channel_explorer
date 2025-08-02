#!/usr/bin/env python

"""chupdate.py: Messaging Database Updater

This program fetches data from messaging services (Telegram, WhatsApp,
Signal, Messenger), including information about channels, users and
messages. It updates a local SQLite database asynchronously with the
fetched data, ensuring proper deduplication, upsert logic, and conflict
resolution. The script supports configurable fetch depth and session
handling.

Usage:
    Run the script from the command line with appropriate arguments for
    desired fetch depth.

Classes:
    DatabaseUpdater:
        Establishes connection to the database and performs
        asynchronous updates of channel, user and message tables.

Requirements:
    - pandas
    - numpy
    - aiosqlite
    - JSON configuration file with session and API credentials
    - data handling modules for appropriate messaging services
"""

__author__ = 'Franciszek Humieja'
__copyright__ = 'Copyright (c) 2025 Franciszek Humieja'
__license__ = 'MIT'
__version__ = '1.0.1'

import asyncio
import aiosqlite
from sqlite3 import (OperationalError, DatabaseError, ProgrammingError,
                     IntegrityError)
from sqlite3 import Cursor
import pandas as pd
from numpy import dtype as np_dtype
import logging
import traceback
import json
import base64
import datetime
from typing import get_args
import argparse

from tgdata import TelegramDataHandler
from wadata import WhatsAppDataHandler
from sgdata import SignalDataHandler
from msdata import MessengerDataHandler

# Local logger
logger = logging.getLogger(__name__)

# Handler type variable
DataHandler = (TelegramDataHandler | WhatsAppDataHandler | SignalDataHandler |
               MessengerDataHandler)


class DatabaseUpdater:
    """Asynchronous handler for safely updating an SQLite database with
    structured data from messaging services. Supports table creation,
    schema evolution and upserts.
    ----------
    The updater ensures data consistency by deactivating outdated rows
    (with `active = FALSE`) and inserting new rows with updated
    information. It uses async locks to maintain concurrency safety and
    supports structured table versioning logic based on session and
    service identifiers.
    ----------
    Public attributes:
        db (aiosqlite.Connection):
            The active database connection object.
        max_sql_variables (int):
            SQLite maximum number of variables in a single query.
    ----------
    Public methods:
        start(): Starts the connection to the database.
        stop(): Stops the connection to the database.
        update_channels(): Updates tables 'channel' and
            'channel_session' with retrieved channel info.
        update_users(): Updates tables 'user', 'user_session' and
            'user_channel' with retrieved user info.
        update_messages(): Updates table 'message' with retrieved
            message info.
    ----------
    Example:
        import aiosqlite
        from chupdate import DatabaseUpdater
        from wadata import WhatsAppDataHandler
        ----------
        async with WhatsAppDataHandler('sess', 123, 'ab12') as handler:
            channels = handler.get_all_channels_frame()
            async with DatabaseUpdater('database.db') as updater:
                await updater.update_channels(handler, channels)
                await updater.update_users(handler, channels)
                await updater.update_messages(
                        handler,
                        channels,
                        all_messages=True,
                        all_messages_limit=3000)
                async with updater.db.cursor() as cursor:
                    await cursor.execute('''
                            SELECT COUNT(*)
                            FROM message
                            WHERE peer_id = 78473
                            ''')
                    result = await cursor.fetchone()
                n_messages = result[0]
    """

    # The SQLite limit of variables in a query.
    max_sql_variables = 250000

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._lock = asyncio.Lock()
        self.db = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def start(self):
        """Opens an asynchronous connection to the SQLite database and
        sets it to WAL (Write-Ahead Logging) mode for better concurrency
        performance.
        """
        try:
            self.db = await aiosqlite.connect(database=self._db_path)
        except (OperationalError, DatabaseError) as e:
            logger.error(
                    f'{type(e).__name__}: Error while connecting to the '
                    f'database: {e}.')
        else:
            async with self.db.cursor() as cur:
                await cur.execute('PRAGMA journal_mode=WAL')
            logger.info(
                    f'Opened the connection to the database {self._db_path!r}')

    async def stop(self):
        """Closes the database connection and logs the total number of
        changes made during the session.
        """
        total_changes = self.db.total_changes
        try:
            await self.db.close()
        except (OperationalError, DatabaseError) as e:
            logger.error(
                    f'{type(e).__name__}: Error while closing connection '
                    f'to the database: {e}.')
        else:
            logger.info(
                    'Closed the connection to the database '
                    f'{self._db_path!r}. Performed {total_changes} '
                    'operations on database rows in total.')

    async def update_channels(
            self, handler: DataHandler, channels_df: pd.DataFrame) -> None:
        """Upserts channel and session-channel data into the database.
        ----------
        If full channel information is available (this may bring more
        information -- for example comment discussions in Telegram
        broadcast channels), outdated rows are deactivated (with
        active = FALSE) for the sake of preserving historical state.
        ----------
        Arguments:
        handler: tgdata.TelegramDataHandler |
                 wadata.WhatsAppDataHandler |
                 sgdata.SignalDataHandler |
                 msdata.MessengerDataHandler
            The data handler instance.
        channels_df: pandas.DataFrame
            The dataframe containing channel metadata to be upserted.
        """
        # Tuple of key database columns; each change of a value in one
        # of these columns will be preserved -- a row with the new value
        # will be inserted in a new position, leaving the row with the
        # old value at the place.
        key_cols_all = (
                'id', '_', 'title', 'creator', 'broadcast', 'megagroup',
                'restricted', 'scam', 'fake', 'gigagroup', 'username',
                'deactivated', '__full', 'about', 'can_view_participants',
                'participants_hidden', 'linked_chat_id', 'service', 'active')
        full_info = True if '__full' in channels_df.columns else False
        df = self._encode_for_sqlite(df=channels_df)
        key_cols = tuple(col for col in df.columns if col in key_cols_all)
        async with self._lock:
            logger.info(
                    f'{handler.service_name}/{handler.session_name}: '
                    f"Started upserting table 'channel'.")
            async with self.db.cursor() as cur:
                await self._upsert_table(
                        table='channel',
                        df=df.drop(columns='date_joined'),
                        key_cols=key_cols,
                        cursor=cur)
                if full_info:
                    await self._disactivate_old_rows(
                            table='channel',
                            service=handler.service_name,
                            session=handler.session_name,
                            cursor=cur)
                await self._upsert_table(
                        table='channel_session',
                        df=df[[
                            'id', 'date_joined', 'session', 'service',
                            'active', 'date_saved']],
                        key_cols=(
                            'id', 'date_joined', 'session', 'service',
                            'active'),
                        cursor=cur)
                if full_info:
                    await self._disactivate_old_rows(
                            table='channel_session',
                            service=handler.service_name,
                            session=handler.session_name,
                            cursor=cur)
            await self.db.commit()
            logger.info(
                    f'{handler.service_name}/{handler.session_name}: '
                    f"Committed changes to table 'channel'.")

    async def update_users(
            self,
            handler: DataHandler,
            channels_df: pd.DataFrame,
            *,
            full_info: bool = False) -> None:
        """Upserts user, user-session and user-channel data into the
        database.
        ----------
        If full channel information is available (this may bring more
        information -- for example comment discussions in Telegram
        broadcast channels), outdated rows are deactivated (with
        active = FALSE) for the sake of preserving historical state.
        ----------
        Arguments:
        handler: tgdata.TelegramDataHandler |
                 wadata.WhatsAppDataHandler |
                 sgdata.SignalDataHandler |
                 msdata.MessengerDataHandler
            The service handler providing access to user data.
        channels_df: pandas.DataFrame
            Dataframe of channel data used to identify scope of users.
        full_info: bool = False (optional)
            Whether to fetch and store full user info.
        """
        # Tuple of key database columns; each change of a value in one
        # of these columns will be preserved -- a row with the new value
        # will be inserted in a new position, leaving the row with the
        # old value at the place.
        key_cols_all = (
                '_', 'id', 'deleted', 'bot', 'verified', 'restricted', 'scam',
                'fake', 'premium', 'bot_business', 'first_name', 'last_name',
                'username', 'phone', '__full', 'about',
                'business_greeting_message', 'business_away_message',
                'birthday', 'personal_channel_id', 'service', 'active')
        # Tuple of database columns that will not be stored in 'user'
        # table because the information stored in them can be session-
        # or channel-dependend. These columns will be saved in
        # 'user_session' or 'user_channel' tables instead.
        cols_exclude_all = (
                'is_self', 'contact', 'mutual_contact', 'close_friend',
                'blocked', 'channels')
        # Tuple of columns that will be saved in 'user_session' table.
        cols_session_all = (
                'id', 'is_self', 'contact', 'mutual_contact', 'close_friend',
                'blocked', 'session', 'service', 'active', 'date_saved',
                'date_saved_full')
        channel_full_info = True if '__full' in channels_df.columns else False
        df_orig = await handler.get_all_users_frame(
                channels_df, full_info=full_info)
        df_users_channels_orig = (
                df_orig[[
                    'id', 'channels', 'session', 'service', 'active',
                    'date_saved']]
                .explode(column='channels', ignore_index=True)
                .rename(columns={'channels': 'channel'})
                .convert_dtypes())
        df = self._encode_for_sqlite(df=df_orig)
        df_users_channels = self._encode_for_sqlite(df=df_users_channels_orig)
        key_cols = tuple(col for col in df.columns if col in key_cols_all)
        cols_exclude = [
                col for col in df.columns if col in cols_exclude_all]
        cols_session = [
                col for col in df.columns if col in cols_session_all]
        async with self._lock:
            logger.info(
                    f'{handler.service_name}/{handler.session_name}: '
                    f"Started upserting table 'user'.")
            async with self.db.cursor() as cur:
                await self._upsert_table(
                        table='user',
                        df=df.drop(columns=cols_exclude),
                        key_cols=key_cols,
                        cursor=cur)
                if channel_full_info:
                    await self._disactivate_old_rows(
                            table='user',
                            service=handler.service_name,
                            session=handler.session_name,
                            cursor=cur)
                await self._upsert_table(
                        table='user_session',
                        df=df[cols_session],
                        key_cols=tuple(
                            col for col in cols_session if col not in
                            ('blocked', 'date_saved', 'date_saved_full')),
                        cursor=cur)
                if channel_full_info:
                    await self._disactivate_old_rows(
                            table='user_session',
                            service=handler.service_name,
                            session=handler.session_name,
                            cursor=cur)
                await self._upsert_table(
                        table='user_channel',
                        df=df_users_channels,
                        key_cols=('id', 'channel', 'service', 'active'),
                        cursor=cur)
                if channel_full_info:
                    await self._disactivate_old_rows(
                            table='user_channel',
                            service=handler.service_name,
                            session=handler.session_name,
                            cursor=cur)
            await self.db.commit()
            logger.info(
                    f'{handler.service_name}/{handler.session_name}: '
                    f"Committed changes to table 'user'.")

    async def update_messages(
            self,
            handler: DataHandler,
            channels_df: pd.DataFrame,
            *,
            all_messages: bool = False,
            all_messages_limit: int = None) -> None:
        """Upserts message data into the database, either incrementally
        (new + edited messages only) or by fetching the full message
        history (with possible limit of fetched messages per channel).
        ----------
        If all messages are fetched (regardless of limit per channel)
        and full channel information is available (this may bring more
        information -- for example comment discussions in Telegram
        broadcast channels), outdated rows are deactivated (with
        active = FALSE) for the sake of preserving historical state.
        ----------
        Arguments:
        handler: tgdata.TelegramDataHandler |
                 wadata.WhatsAppDataHandler |
                 sgdata.SignalDataHandler |
                 msdata.MessengerDataHandler
            Data handler capable of providing message history.
        channels_df: pandas.DataFrame
            Dataframe of channels for which messages are updated.
        all_messages: bool = False (optional)
            Whether to fetch and update all messages (or all messages
            within the limit per channel, if all_messages_limit is
            given) instead of only new and edited ones.
        all_messages_limit: int = None (optional)
            If set, limits the number of messages per channel when
            fetching all messages. If all_messages = False, this
            argument has no effect.
        """
        # Tuple of key database columns; each change of a value in one
        # of these columns will be preserved -- a row with the new value
        # will be inserted in a new position, leaving the row with the
        # old value at the place.
        key_cols_all = (
                '_', 'id', 'peer_id', 'date', 'message', 'mentioned', 'post',
                'pinned', 'from_id', 'saved_peer_id', 'fwd_from',
                'via_bot_id', 'via_business_bot_id', 'edit_date',
                'post_author', 'service', 'active')
        channel_full_info = True if '__full' in channels_df.columns else False
        if all_messages:
            df_orig = await handler.get_all_messages_frame(
                    channels_df, limit=all_messages_limit)
        else:
            messages_stored_df = await self._get_stored_messages_frame()
            df_orig = await handler.get_new_messages_frame(
                    channels_df=channels_df,
                    messages_df=messages_stored_df)
        df = self._encode_for_sqlite(df=df_orig)
        key_cols = tuple(col for col in df.columns if col in key_cols_all)
        async with self._lock:
            logger.info(
                    f'{handler.service_name}/{handler.session_name}: '
                    f"Started upserting table 'message'.")
            async with self.db.cursor() as cur:
                await self._upsert_table(
                        table='message',
                        df=df,
                        key_cols=key_cols,
                        cursor=cur)
                if all_messages and channel_full_info:
                    await self._disactivate_old_rows(
                            table='message',
                            service=handler.service_name,
                            session=handler.session_name,
                            cursor=cur,
                            limit=all_messages_limit)
            await self.db.commit()
            logger.info(
                    f'{handler.service_name}/{handler.session_name}: '
                    f"Committed changes to table 'message'.")

    async def _create_table(
            self,
            name: str,
            *,
            dtypes: pd.Series = None,
            cols: pd.Index = None,
            key_cols: list | tuple = None,
            cursor: Cursor = None) -> None:
        """Creates a new table in the database.
        ----------
        The schema is inferred from either 'dtypes' or 'cols' and a
        unique index is created based on provided key columns, excluding
        selected "full info" keys (because they can be skipped while
        fetching data).
        ----------
        Arguments:
        name: str
            Name of the table to create.
        dtypes: pd.Series = None (optional)
            Series mapping column names to pandas dtypes (used to
            determine SQLite types).
        cols: pd.Index = None (optional)
            Alternative to 'dtypes' -- used when only column names are
            known, without the dtypes.
        key_cols: list | tuple = None (optional)
            Columns used to define a uniqueness constraint.
        cursor: sqlite3.Cursor = None (optional)
            Cursor to use; if not provided, a new one is created
            internally.
        """
        virtual_cursor = True if not cursor else False
        key_cols_full = self._find_key_cols_full(key_cols=key_cols)
        columns = []
        unique_columns = []
        try:
            for col, dtype in dtypes.items():
                sqlite_type = self._map_dtype(dtype=dtype)
                if col == 'id':
                    sqlite_type = f'{sqlite_type} NOT NULL'
                columns.append(f'{col} {sqlite_type}')
                if col in key_cols and col not in key_cols_full:
                    if col == 'active':
                        unique_columns.append(col)
                    else:
                        null_value = self._map_null_value(dtype=dtype)
                        unique_columns.append(f'COALESCE({col}, {null_value})')
        except AttributeError:
            columns = [*cols]
            columns[columns.index('id')] = 'id NOT NULL'
            unique_columns = tuple(
                    f"COALESCE({col}, '')" for col in key_cols
                    if col not in key_cols_full and col != 'active')
            unique_columns.append('active')
        create_sql = f'''
        CREATE TABLE IF NOT EXISTS {name} ({', '.join(columns)})
        '''
        unique_sql = f'''
        CREATE UNIQUE INDEX IF NOT EXISTS {name}_unique
        ON {name} ({', '.join(unique_columns)})
        '''
        if virtual_cursor:
            cursor = await self.db.cursor()
        try:
            await cursor.execute(sql=create_sql)
            if key_cols is not None:
                await cursor.execute(sql=unique_sql)
        finally:
            if virtual_cursor:
                await cursor.close()
        logger.info(f'Created table {name!r} with {len(columns)} columns.')

    async def _add_column(
            self,
            table: str,
            name: str,
            *,
            dtype: np_dtype | pd.api.extensions.ExtensionDtype | str = None,
            cursor: Cursor = None) -> None:
        """Adds a column to an existing table in the database.
        ----------
        Arguments:
        table: str
            Name of the table to alter.
        name: str
            Name of the new column.
        dtype: numpy.dtype |
               pandas.api.extensions.ExtensionDtype |
               str
               = None (optional)
            Pandas or NumPy dtype to determine the corresponding SQLite
            column type.
        cursor: sqlite3.Cursor = None (optional)
            Cursor to use; if not provided, a new one is created
            internally.
        """
        virtual_cursor = True if not cursor else False
        if dtype:
            sqlite_type = self._map_dtype(dtype=dtype)
            column = f'{name} {sqlite_type}'
        else:
            column = name
        alter_sql = f'''
        ALTER TABLE {table}
        ADD COLUMN {column}
        '''
        if virtual_cursor:
            cursor = await self.db.cursor()
        try:
            await cursor.execute(sql=alter_sql)
        finally:
            if virtual_cursor:
                await cursor.close()
        logger.info(f'Added column {column!r} to table {table!r}.')

    async def _upsert_table(
            self,
            table: str,
            df: pd.DataFrame,
            key_cols: list | tuple = None,
            *,
            cursor: Cursor = None) -> None:
        """Upserts data from a DataFrame into the given SQLite table.
        ----------
        Handles:
        - Table creation, if it doesn't exist.
        - Column addition, if schema mismatch is encountered.
        - Premarking outdated rows as inactive (active = FALSE) based
          on "full info" diffs -- the "full info" key columns cannot be
          added to table's unique index because they may be skipped
          while fetching data, so the 'ON CONFLICT' clause of upsert
          does not fully prevents overwriting changes in these columns.
          This premarking as inactive enables the following upsert
          operation to preserve historical data with respect to changes
          in the "full info" columns in the case when the "basic info"
          columns remain unchanged.
        - Safe insertion with deduplication using 'ON CONFLICT' clause
          for "basic info" columns.
        - Assigning to the cursor rowids of the table where
          update/insert took place -- which can be utilized further.
        ----------
        Arguments:
        table: str
            Name of the target database table.
        df: pandas.DataFrame
            DataFrame with data to be upserted.
        key_cols: list | tuple = None (optional)
            Columns used to determine uniqueness (for upserts).
        cursor: sqlite3.Cursor = None (optional)
            Cursor to use; if not provided, a new one is created internally.
        """
        virtual_cursor = True if not cursor else False
        key_cols_full = tuple(self._find_key_cols_full(key_cols=key_cols))
        key_cols_basic = tuple(
                col for col in key_cols if col not in key_cols_full)
        non_key_cols = tuple(col for col in df.columns if col not in key_cols)
        upserted_rows = []
        # The following SQL prevents from overwriting rows for which
        # basic info was intact but full info columns were modified.
        # This case is an exception, not preserved by the unique index
        # contraint of the tables. By setting active=FALSE, it makes
        # the new row being inserted, instead of firing an unwanted
        # conflict in the following upsert.
        def pre_disactivate_sql(active_val: str = 'FALSE') -> str:
            return f"""
            UPDATE {table}
            SET active = {active_val}
            WHERE __full IS NOT NULL
                AND {' AND '.join(f"COALESCE({col}, '') = COALESCE(?, '')"
                     for col in key_cols_basic)}
                AND ({' OR '.join(f"COALESCE({col}, '') != COALESCE(?, '')"
                      for col in key_cols_full)})
            """
        upsert_sql = f"""
        INSERT INTO {table} ({', '.join(df.columns)})
        VALUES ({', '.join('?'*df.shape[1])})
        ON CONFLICT DO {f'''UPDATE
        SET {', '.join(f'{col} = excluded.{col}'
             for col in key_cols_full + non_key_cols)}'''
        if key_cols_full or non_key_cols else 'NOTHING'}
        RETURNING rowid
        """
        cur_count = await self.db.cursor()
        if virtual_cursor:
            cursor = await self.db.cursor()
        try:
            await cur_count.execute(f'SELECT COUNT(*) FROM {table}')
        except OperationalError as e:
            if 'no such table' in str(e):
                logger.warning(
                        f'Table {table!r} does not exist in the database. '
                        'A new table is being created...')
                await self._create_table(
                        name=table,
                        dtypes=df.convert_dtypes().dtypes,
                        key_cols=key_cols,
                        cursor=cursor)
                await self._upsert_table(
                        table=table, df=df, key_cols=key_cols, cursor=cursor)
            else:
                raise
        else:
            pre_count_result = await cur_count.fetchone()
            try:
                if key_cols_full:
                    rows_key_cols = (
                            df
                            .loc[:, key_cols_basic + key_cols_full]
                            .values
                            .tolist())
                    await cursor.executemany(
                            sql=pre_disactivate_sql(),
                            parameters=rows_key_cols)
                    logger.info(
                            f'Table {table!r}: Premarked {cursor.rowcount} '
                            'rows as inactive.')
            except IntegrityError:
                await cursor.executemany(
                        sql=pre_disactivate_sql(active_val='NULL'),
                        parameters=rows_key_cols)
                logger.info(
                        f'Table {table!r}: Premarked {cursor.rowcount} '
                        'rows as inactive (with NULL value for '
                        'uniqueness integrity).')
            except OperationalError as e:
                if 'no such column' in str(e):
                    missing_col = str(e).split(' ')[-1]
                    logger.warning(
                            f'Table {table!r} does not have key column '
                            f'named {missing_col!r}. The new column is '
                            'being added...')
                    await self._add_column(
                            table=table,
                            name=missing_col,
                            dtype=df.convert_dtypes().dtypes[missing_col],
                            cursor=cursor)
                    await self._upsert_table(
                            table=table,
                            df=df,
                            key_cols=key_cols,
                            cursor=cursor)
                else:
                    raise
            else:
                try:
                    # SQLite does not provide support for RETURNING in the
                    # Cursor.executemany() method. This is why a loop is used
                    # here to gather upserted row ids in a list.
                    for row in df.values.tolist():
                        await cursor.execute(sql=upsert_sql, parameters=row)
                        upsert_result = await cursor.fetchone()
                        if upsert_result:
                            upserted_rows.append(upsert_result[0])
                except OperationalError as e:
                    if 'has no column' in str(e):
                        missing_col = str(e).split(' ')[-1]
                        logger.warning(
                                f'Table {table!r} does not have column named '
                                f'{missing_col!r}. The new column is being '
                                'added...')
                        await self._add_column(
                                table=table,
                                name=missing_col,
                                dtype=df.convert_dtypes().dtypes[missing_col],
                                cursor=cursor)
                        await self._upsert_table(
                                table=table,
                                df=df,
                                key_cols=key_cols,
                                cursor=cursor)
                    else:
                        raise
                else:
                    await cur_count.execute(f'SELECT COUNT(*) FROM {table}')
                    post_count_result = await cur_count.fetchone()
                    n_inserted_rows = (
                            post_count_result[0] - pre_count_result[0])
                    logger.info(
                            f'Table {table!r}: Inserted {n_inserted_rows} '
                            'rows and updated '
                            f'{len(upserted_rows)-n_inserted_rows} rows.')
                    if (not virtual_cursor
                        and len(upserted_rows) <= self.max_sql_variables - 2):
                        # Since the aim is to store upserted row ids as
                        # cursor parameters (as it would have been, if
                        # RETURNING could be used in Cursor.executemany()),
                        # we save it there using SELECT.
                        select_sql = f'''
                        SELECT rowid
                        FROM {table}
                        WHERE rowid IN ({', '.join('?'*len(upserted_rows))})
                        '''
                        await cursor.execute(
                                sql=select_sql, parameters=upserted_rows)
        finally:
            await cur_count.close()
            if virtual_cursor:
                await cursor.close()

    async def _disactivate_old_rows(
            self,
            table: str,
            service: str,
            session: str,
            *,
            cursor: Cursor = None,
            limit: int = None) -> None:
        """Marks old rows as inactive (active = FALSE) for the given
        session/service that are not among the recently upserted rows.
        ----------
        Arguments:
        table: str
            Name of the table to update.
        service: str
            Messaging service name (e.g., 'telegram').
        session: str
            Session identifier for the account.
        cursor: sqlite3.Cursor = None (optional)
            Cursor to use; if not provided, a new one is created
            internally.
        limit: int = None (optional)
            If provided, restricts disactivation to a limited number of
            rows per channel.
        """
        virtual_cursor = True if not cursor else False
        if virtual_cursor:
            cursor = await self.db.cursor()
        rows_result = await cursor.fetchall()
        upserted_rows = tuple(row[0] for row in rows_result)
        if limit:
            rows_within_limit = await self._get_rows_within_limit(limit=limit)
            n_limit_rows_allowed = (
                    self.max_sql_variables - len(upserted_rows) - 2)
            if len(rows_within_limit) > n_limit_rows_allowed:
                # Prevent the following SQL to exceed the built-in limit
                # of variable number.
                rows_within_limit = rows_within_limit[:n_limit_rows_allowed]
        else:
            rows_within_limit = tuple()
        def update_sql(active_val: str = 'FALSE') -> str:
            return f'''
            UPDATE {table}
            SET active = {active_val}
            WHERE active = TRUE
                AND rowid NOT IN ({', '.join('?'*len(upserted_rows))})
                AND service = ?
                AND session = ?
                {f"AND rowid IN ({', '.join('?'*len(rows_within_limit))})"
                 if limit else ''}
            '''
        try:
            if upserted_rows:
                await cursor.execute(
                        sql=update_sql(),
                        parameters=(*upserted_rows, service, session,
                                    *rows_within_limit))
                logger.info(
                        f'Table {table!r}: Marked {cursor.rowcount} rows '
                        'as inactive.')
        except IntegrityError:
            await cursor.execute(
                    sql=update_sql(active_val='NULL'),
                    parameters=(*upserted_rows, service, session,
                                *rows_within_limit))
            logger.info(
                    f'Table {table!r}: Marked {cursor.rowcount} rows '
                    'as inactive (with NULL value for uniqueness integrity).')
        finally:
            if virtual_cursor:
                await cursor.close()

    async def _get_stored_messages_frame(self) -> pd.DataFrame:
        """Retrieves a DataFrame of stored messages (active only),
        including their 'id', 'peer_id', 'active' and 'date_saved'.
        ----------
        Such a DataFrame is needed in data handlers to fetch new (not
        stored) messages.
        ----------
        Returns:
        pandas.DataFrame
            A DataFrame of existing messages with datetime conversion.
        """
        select_sql = f'''
        SELECT id, peer_id, active, date_saved
        FROM message
        WHERE active = TRUE
        '''
        async with self.db.cursor() as cur:
            try:
                await cur.execute(sql=select_sql)
            except OperationalError as e:
                if 'no such table' in str(e):
                    return None
                else:
                    raise
            else:
                result_messages = await cur.fetchall()
                df = pd.DataFrame(
                        data=result_messages,
                        columns=[desc[0] for desc in cur.description])
                df['date_saved'] = (
                        df['date_saved']
                        .apply(datetime.datetime.fromisoformat))
                return df.convert_dtypes()

    async def _get_rows_within_limit(self, limit: int) -> tuple[int]:
        """For each channel, selects the most recent 'limit' messages
        by ID.
        ----------
        Used for pruning purposes in message disactivation logic.
        ----------
        Arguments:
        limit: int
            Number of newest messages to retrieve per channel.
        ----------
        Returns:
        tuple[int]
            Combined rowids of recent messages across all channels.
        """
        async with self.db.cursor() as cur:
            await cur.execute('SELECT id FROM channel')
            channels_result = await cur.fetchall()
            channels = tuple(res[0] for res in channels_result)
            rowids = []
            for channel in channels:
                select_messages_sql = f'''
                SELECT rowid
                FROM message
                WHERE peer_id = {channel}
                ORDER BY id DESC
                LIMIT {limit}
                '''
                await cur.execute(select_messages_sql)
                messages_result = await cur.fetchall()
                for res in messages_result:
                    rowids.append(res[0])
        return rowids

    @staticmethod
    def _find_key_cols_full(key_cols: list | tuple) -> list | tuple:
        """Extracts the slice of 'key_cols' corresponding to the "full
        info" columns.
        ----------
        The full info columns are expected to be between '__full' and
        'service'.
        ----------
        Arguments:
        key_cols: list | tuple
            The sequence of all key column names.
        ----------
        Returns:
        list | tuple
            Slice of key columns considered as "full info" keys.
        """
        try:
            full_start = key_cols.index('__full')
            full_end = key_cols.index('service')
            return key_cols[full_start:full_end]
        except ValueError:
            return tuple()

    @staticmethod
    def _encode_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
        """Transforms a DataFrame to a SQLite-friendly format:
        - Serializes dicts/lists to JSON
        - Encodes bytes to base64
        - Converts dates and datetimes to ISO strings
        ----------
        Arguments:
        df: pd.DataFrame
            Input dataframe to be encoded.
        ----------
        Returns:
        pd.DataFrame
            Encoded dataframe ready for SQLite insertion.
        """
        def data_encoder(obj):
            def json_encoder(inner_obj):
                if isinstance(inner_obj, bytes):
                    return base64.b64encode(inner_obj).decode('ascii')
                if isinstance(inner_obj, (datetime.datetime, datetime.date)):
                    return inner_obj.isoformat()
                raise TypeError(
                        f'Object of type {type(inner_obj).__name__} is not '
                        'JSON serializable')
            if isinstance(obj, (dict, list)):
                return json.dumps(obj, default=json_encoder)
            elif isinstance(obj, (datetime.datetime, datetime.date)):
                return obj.isoformat()
            elif pd.isna(obj):
                return None
            else:
                return obj
        return df.map(data_encoder)

    @staticmethod
    def _map_dtype(
            dtype: np_dtype | pd.api.extensions.ExtensionDtype | str) -> str:
        """Maps a pandas or NumPy dtype to the corresponding SQLite
        column type.
        ----------
        Parameters:
        dtype: numpy.dtype | pandas.api.extensions.ExtensionDtype | str
            The pandas or NumPy dtype to convert.
        ----------
        Returns:
        str
            SQLite type string (e.g., 'INTEGER', 'TEXT').
        """
        if pd.api.types.is_integer_dtype(dtype):
            return 'INTEGER'
        elif pd.api.types.is_bool_dtype(dtype):
            return 'INTEGER'
        elif pd.api.types.is_float_dtype(dtype):
            return 'REAL'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TEXT'
        else:
            return 'TEXT'

    @staticmethod
    def _map_null_value(
            dtype: np_dtype | pd.api.extensions.ExtensionDtype | str
    ) -> int | float | str:
        """Provides a default 'null-equivalent' value for the given
        dtype, used in COALESCE expressions for uniqueness.
        ----------
        Parameters:
        dtype: numpy.dtype | pandas.api.extensions.ExtensionDtype | str
            The pandas or NumPy dtype to inspect.
        ----------
        Returns:
        int | float | str
            Fallback value representing "null" for this column type.
        """
        if pd.api.types.is_integer_dtype(dtype):
            return -1
        elif pd.api.types.is_bool_dtype(dtype):
            return -1
        elif pd.api.types.is_float_dtype(dtype):
            return -1.0
        else:
            return "''"

def map_data_handler(
        service_name: str,
        session_name: str,
        api_id: int,
        api_hash: str) -> DataHandler | None:
    """Maps a service name to the corresponding DataHandler class and
    instantiates it.
    ----------
    Parameters:
    service_name: str
        The name of the messaging service (e.g., 'telegram').
    session_name: str
        Session identifier for the account.
    api_id: int
        The API ID used to authenticate with the service.
    api_hash: str
        The API hash used to authenticate with the service.
    ----------
    Returns:
    DataHandler
        An instance of the appropriate handler subclass.
    ----------
    Raises:
    ValueError
        If the service name is not recognized.
    """
    for HandlerCls in get_args(DataHandler):
        if service_name == HandlerCls.service_name:
            return HandlerCls(session_name, api_id, api_hash)
    raise ValueError(f'Unrecognized service name: {service_name}')

async def process_session(
        updater: DatabaseUpdater,
        service_name: str,
        session_data: dict,
        cmdline_args: argparse.Namespace) -> None:
    """Orchestrates concurrent update processes for a single session of
    a messaging service.
    ----------
    This includes:
    - Initializing the appropriate data handler
    - Fetching channel/user/message data (with options for depth and scope)
    - Dispatching concurrent update tasks for database insertion
    ----------
    Parameters:
    updater: DatabaseUpdater
        Active database updater managing the SQLite operations.
    service_name: str
        Name of the service for which this session applies.
    session_data: dict
        Dictionary with keys: 'session', 'api_id', 'api_hash'.
    cmdline_args: argparse.Namespace
        Parsed command-line arguments with flags controlling update
        behavior.
    """
    update_channels = cmdline_args.channels != 'none'
    full_channels_info = cmdline_args.channels == 'full'
    update_users = cmdline_args.users != 'none'
    full_users_info = cmdline_args.users == 'full'
    update_messages = cmdline_args.messages != 'none'
    all_messages = cmdline_args.messages == 'all'
    try:
        session_name = session_data['session']
    except KeyError as e:
        logger.error(
                f'{service_name}: KeyError: Cannot read session name '
                'for one of the sessions because the configuration file '
                f'does not contain necessary key: {e}.')
    else:
        try:
            api_id = session_data['api_id']
            api_hash = session_data['api_hash']
        except KeyError as e:
            logger.error(
                    f'{service_name}/{session_name}: KeyError: Cannot read '
                    'session credentials because the configuration file does '
                    f'not contain necessary key: {e}.')
        else:
            try:
                handler = map_data_handler(
                        service_name, session_name, api_id, api_hash)
            except (ValueError, NotImplementedError, AttributeError) as e:
                logger.error(
                        f'{service_name}/{session_name}: {type(e).__name__}: '
                        f'Cannot initialize data handler: {e}.')
            else:
                async with handler:
                    channels_df = await handler.get_all_channels_frame(
                            full_info=full_channels_info)
                    try:
                        async with asyncio.TaskGroup() as tg:
                            if update_channels:
                                tg.create_task(updater.update_channels(
                                        handler=handler,
                                        channels_df=channels_df))
                            if update_users:
                                tg.create_task(updater.update_users(
                                        handler=handler,
                                        channels_df=channels_df,
                                        full_info=full_users_info))
                            if update_messages:
                                tg.create_task(updater.update_messages(
                                        handler=handler,
                                        channels_df=channels_df,
                                        all_messages=all_messages,
                                        all_messages_limit=(
                                            cmdline_args.limit_messages)))
                    except* aiosqlite.Error as e_group:
                        for e in e_group.exceptions:
                            tb = e.__traceback__
                            first_tb_frame = traceback.extract_tb(tb)[0]
                            if first_tb_frame.name == 'update_channels':
                                table = 'channel'
                            elif first_tb_frame.name == 'update_users':
                                table = 'user'
                            elif first_tb_frame.name == 'update_messages':
                                table = 'message'
                            else:
                                table = None
                            logger.error(
                                    f'{service_name}/{session_name}: '
                                    f'{type(e).__name__}: Cannot update '
                                    f"table {table!r} in the database: {e}.")

async def main(cmdline_args: argparse.Namespace) -> None:
    """Entrypoint coroutine that coordinates reading configuration,
    initializing the database connection and processing concurrently all
    declared sessions across services.
    ----------
    Parameters:
    cmdline_args: argparse.Namespace
        Parsed command-line arguments controlling fetch modes and limits.
    """
    config_fpath = 'config.json'
    database_fpath = 'chexplore.db'
    try:
        with open(config_fpath, 'r') as f:
            config = json.load(f)
    except FileNotFoundError as e:
        logger.error(
                f'FileNotFoundError: Cannot find configuration file: {e}.')
    else:
        async with DatabaseUpdater(db_path=database_fpath) as db_updater:
            async with asyncio.TaskGroup() as tg:
                try:
                    for service in config['services']:
                        try:
                            for session in config['sessions'][service]:
                                tg.create_task(
                                        process_session(
                                            updater=db_updater,
                                            service_name=service,
                                            session_data=session,
                                            cmdline_args=cmdline_args))
                        except KeyError as e:
                            logger.error(
                                    f'{service}: KeyError: Configuration '
                                    f'file {config_fpath!r} does not contain '
                                    f"necessary key: {e} in 'sessions'. "
                                    'Cannot obtain session information.')
                except KeyError as e:
                    logger.error(
                            f'KeyError: Configuration file {config_fpath!r} '
                            f'does not contain necessary key: {e}. Cannot '
                            'obtain service information.')

if __name__ == '__main__':
    from logging_config import root_logger
    parser = argparse.ArgumentParser(
            prog='chupdate.py',
            description=
                'Updater for database of channel/group info of messaging '
                'services. Fetches and upserts info about channels/groups, '
                'their participants and messages.')
    parser.add_argument(
            '-c', '--channels',
            choices=['none', 'basic', 'full'],
            default='full',
            help=
                'Determine the kind of channel data to fetch: '
                'none -- do not update the channel table, '
                'basic -- update the channel table with data from the basic '
                'columns only, '
                'full -- update the channel table with data from all the '
                'columns. '
                "The default option value is 'full'.")
    parser.add_argument(
            '-u', '--users',
            choices=['none', 'basic', 'full'],
            default='basic',
            help=
                'Determine the kind of user data to fetch: '
                'none -- do not update the user table, '
                'basic -- update the user table with data from the basic '
                'columns only, '
                'full -- update the user table with data from all the '
                'columns. '
                "The default option value is 'basic'.")
    parser.add_argument(
            '-m', '--messages',
            choices=['none', 'new', 'all'],
            default='new',
            help=
                'Determine the kind of message data to fetch: '
                'none -- do not update the message table, '
                'new -- update the message table by fetching only new '
                '(unsaved so far) messages and those of the saved messages '
                'which have been edited, '
                'all -- update the message table by fetching all messages '
                'or the number of messages from each channel determined by '
                'the option -l, --limit_messages if present. '
                "The default option value is 'new'.")
    parser.add_argument(
            '-l', '--limit_messages',
            type=int,
            help=
                'Determine the limit of message number to be fetched for '
                "each channel if -m, --messages option value is 'all'.")
    args = parser.parse_args()
    asyncio.run(main(cmdline_args=args))
