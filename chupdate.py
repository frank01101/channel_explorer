#!/usr/bin/env python

"""chupdate.py"""

__author__ = 'Franciszek Humieja'
__copyright__ = 'Copyright (c) 2025 Franciszek Humieja'
__license__ = 'MIT'
__version__ = '0.4.0'

import asyncio
import aiosqlite
from sqlite3 import (OperationalError, DatabaseError, ProgrammingError,
                     IntegrityError)
from sqlite3 import Cursor
import pandas as pd
from numpy import dtype as np_dtype
import logging
import json
import base64
import datetime
from typing import get_args

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
    def __init__(self, db_path: str):
        self._db_path = db_path
        self.db = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def start(self):
        """Connects to the database asynchronously."""
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
        """Closes connection to the database asynchronously."""
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
                        'id', 'date_joined', 'session', 'service', 'active',
                        'date_saved']],
                    key_cols=(
                        'id', 'date_joined', 'session', 'service', 'active'),
                    cursor=cur)
            if full_info:
                await self._disactivate_old_rows(
                        table='channel_session',
                        service=handler.service_name,
                        session=handler.session_name,
                        cursor=cur)
        await self.db.commit()
        logger.info(
                f'{handler.service_name}/{handler.session_name}: Finished '
                f"upserting table 'channel'.")

    async def update_users(
            self,
            handler: DataHandler,
            channels_df: pd.DataFrame,
            *,
            full_info: bool = False) -> None:
        # Tuple of key database columns; each change of a value in one
        # of these columns will be preserved -- a row with the new value
        # will be inserted in a new position, leaving the row with the
        # old value at the place.
        key_cols_all = (
                '_', 'id', 'deleted', 'bot', 'verified', 'restricted', 'scam',
                'fake', 'premium', 'bot_business', 'first_name', 'last_name',
                'username', 'phone', '__full', 'about',
                'business_greeting_message', 'business_away_message',
                'business_intro', 'birthday', 'personal_channel_id',
                'service', 'active')
        cols_exclude_all = (
                'is_self', 'contact', 'mutual_contact', 'close_friend',
                'blocked', 'channels')
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
                f'{handler.service_name}/{handler.session_name}: Finished '
                f"upserting table 'user'.")

    async def update_messages(
            self,
            handler: DataHandler,
            channels_df: pd.DataFrame,
            *,
            all_messages: bool = False) -> None:
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
            df_orig = await handler.get_all_messages_frame(channels_df)
        else:
            messages_stored_df = await self._get_stored_messages_frame()
            df_orig = await handler.get_new_messages_frame(
                    channels_df=channels_df,
                    messages_df=messages_stored_df)
        df = self._encode_for_sqlite(df=df_orig)
        key_cols = tuple(col for col in df.columns if col in key_cols_all)
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
                        cursor=cur)
        await self.db.commit()
        logger.info(
                f'{handler.service_name}/{handler.session_name}: Finished '
                f"upserting table 'message'.")

    async def _create_table(
            self,
            name: str,
            *,
            dtypes: pd.Series = None,
            cols: pd.Index = None,
            key_cols: list | tuple = None,
            cursor: Cursor = None) -> None:
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
            logger.debug(create_sql)
            logger.debug(unique_sql)
            await cursor.execute(sql=create_sql)
            if key_cols is not None:
                await cursor.execute(sql=unique_sql)
        finally:
            if virtual_cursor:
                await cursor.close()
        logger.info(f'Created table {name!r} with {len(columns)} columns.')

    async def _upsert_table(
            self,
            table: str,
            df: pd.DataFrame,
            key_cols: list | tuple = None,
            *,
            cursor: Cursor = None) -> None:
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
            if key_cols_full:
                rows_key_cols = (
                        df
                        .loc[:, key_cols_basic + key_cols_full]
                        .values
                        .tolist())
                try:
                    logger.debug(pre_disactivate_sql())
                    await cursor.executemany(
                            sql=pre_disactivate_sql(),
                            parameters=rows_key_cols)
                    logger.info(
                            f'Table {table!r}: Premarked {cursor.rowcount} '
                            'rows as inactive.')
                except IntegrityError:
                    logger.debug(pre_disactivate_sql('NULL'))
                    await cursor.executemany(
                            sql=pre_disactivate_sql(active_val='NULL'),
                            parameters=rows_key_cols)
                    logger.info(
                            f'Table {table!r}: Premarked {cursor.rowcount} '
                            'rows as inactive (with NULL value for '
                            'uniqueness integrity).')
            # SQLite does not provide support for RETURNING in the
            # Cursor.executemany() method. This is why a loop is used
            # here to gather upserted row ids in a list.
            logger.debug(upsert_sql)
            for row in df.values.tolist():
                await cursor.execute(sql=upsert_sql, parameters=row)
                upsert_result = await cursor.fetchone()
                if upsert_result:
                    upserted_rows.append(upsert_result[0])
            await cur_count.execute(f'SELECT COUNT(*) FROM {table}')
            post_count_result = await cur_count.fetchone()
            n_inserted_rows = post_count_result[0] - pre_count_result[0]
            logger.info(
                    f'Table {table!r}: Inserted {n_inserted_rows} rows and '
                    f'updated {len(upserted_rows)-n_inserted_rows} rows.')
            if not virtual_cursor:
                # Since the aim is to store upserted row ids as cursor
                # parameters (as it would have been, if RETURNING could
                # be used in Cursor.executemany()), we save it there
                # using SELECT.
                select_sql = f'''
                SELECT rowid
                FROM {table}
                WHERE rowid IN ({', '.join('?'*len(upserted_rows))})
                '''
                await cursor.execute(sql=select_sql, parameters=upserted_rows)
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
            cursor: Cursor = None) -> None:
        virtual_cursor = True if not cursor else False
        if virtual_cursor:
            cursor = await self.db.cursor()
        rows_result = await cursor.fetchall()
        upserted_rows = tuple(row[0] for row in rows_result)
        def update_sql(active_val: str = 'FALSE') -> str:
            return f'''
            UPDATE {table}
            SET active = {active_val}
            WHERE active = TRUE
                AND rowid NOT IN ({', '.join('?'*len(upserted_rows))})
                AND service = ?
                AND session = ?
            '''
        try:
            await cursor.execute(
                    sql=update_sql(),
                    parameters=(*upserted_rows, service, session))
            logger.info(
                    f'Table {table!r}: Marked {cursor.rowcount} rows '
                    'as inactive.')
        except IntegrityError:
            await cursor.execute(
                    sql=update_sql(active_val='NULL'),
                    parameters=(*upserted_rows, service, session))
            logger.info(
                    f'Table {table!r}: Marked {cursor.rowcount} rows '
                    'as inactive (with NULL value for uniqueness integrity).')
        finally:
            if virtual_cursor:
                await cursor.close()

    async def _get_stored_messages_frame(self) -> pd.DataFrame:
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

    @staticmethod
    def _find_key_cols_full(key_cols: list | tuple) -> list | tuple:
        try:
            full_start = key_cols.index('__full')
            full_end = key_cols.index('service')
            return key_cols[full_start:full_end]
        except ValueError:
            return tuple()

    @staticmethod
    def _encode_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
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
    for HandlerCls in get_args(DataHandler):
        if service_name == HandlerCls.service_name:
            return HandlerCls(session_name, api_id, api_hash)
    raise ValueError(f'Unrecognized service name: {service_name}')

async def process_session(
        updater: DatabaseUpdater,
        service_name: str,
        session_data: dict) -> None:
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
                            full_info=True)
                    try:
                        await updater.update_channels(
                                handler=handler, channels_df=channels_df)
                    except aiosqlite.Error as e:
                        logger.error(
                                f'{service_name}/{session_name}: '
                                f'{type(e).__name__}: Cannot update '
                                f"table 'channel' in the database: {e}.")
                    try:
                        await updater.update_users(
                                handler=handler,
                                channels_df=channels_df,
                                full_info=False)
                    except aiosqlite.Error as e:
                        logger.error(
                                f'{service_name}/{session_name}: '
                                f'{type(e).__name__}: Cannot update '
                                f"table 'user' in the database: {e}.")
                    try:
                        await updater.update_messages(
                                handler=handler,
                                channels_df=channels_df,
                                all_messages=False)
                    except aiosqlite.Error as e:
                        logger.error(
                                f'{service_name}/{session_name}: '
                                f'{type(e).__name__}: Cannot update '
                                f"table 'message' in the database: {e}.")

async def main() -> None:
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
                                            session_data=session))
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
    asyncio.run(main())
