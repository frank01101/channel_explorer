#!/usr/bin/env python

"""chupdate.py"""

__author__ = 'Franciszek Humieja'
__copyright__ = 'Copyright (c) 2025 Franciszek Humieja'
__license__ = 'MIT'
__version__ = '0.1.0'

import asyncio
import aiosqlite
from sqlite3 import OperationalError, DatabaseError, ProgrammingError
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

    async def update_channels(self, handler: DataHandler) -> None:
        # Tuple of key database columns; each change of a value in one
        # of these columns will be recorded.
        key_cols_all = (
                'id', '_', 'title', 'creator', 'broadcast', 'megagroup',
                'restricted', 'scam', 'fake', 'gigagroup', 'username',
                'deactivated', '__full', 'about', 'can_view_participants',
                'participants_hidden', 'linked_chat_id', 'service')
        # DataFrame with current channels, downloaded from
        # the messaging service
        df_orig = await handler.get_all_channels_frame()
        df = self._encode_for_sqlite(df=df_orig)
        key_cols = tuple(col for col in df.columns if col in key_cols_all)
        async with self.db.cursor() as cur:
            await self._upsert_table(
                    table='channel',
                    df=df.drop(columns='date_joined'),
                    key_cols=key_cols,
                    cursor=cur)
            await self._disactivate_old_rows(
                    table='channel',
                    service=handler.service_name,
                    session=handler.session_name,
                    cursor=cur)
            await self._upsert_table(
                    table='channel_session',
                    df=df[[
                        'id', 'date_joined', 'session', 'service', 'active']],
                    key_cols=('id', 'date_joined', 'session', 'service'),
                    cursor=cur)
            await self._disactivate_old_rows(
                    table='channel_session',
                    service=handler.service_name,
                    session=handler.session_name,
                    cursor=cur)
        await self.db.commit()
        logger.info(
                f'{handler.service_name}/{handler.session_name}: Finished '
                f"upserting table 'channel'.")

    async def _create_table(
            self,
            name: str,
            *,
            dtypes: pd.Series = None,
            cols: pd.Index = None,
            key_cols: list | tuple = None,
            cursor: Cursor = None) -> None:
        virtual_cursor = True if not cursor else False
        columns = []
        unique_columns = []
        try:
            for col, dtype in dtypes.items():
                sqlite_type = self._map_dtype(dtype=dtype)
                null_value = self._map_null_value(dtype=dtype)
                if col == 'id':
                    sqlite_type = f'{sqlite_type} NOT NULL'
                columns.append(f'{col} {sqlite_type}')
                if col in key_cols:
                    unique_columns.append(f'COALESCE({col}, {null_value})')
        except AttributeError:
            columns = [*cols]
            columns[columns.index('id')] = 'id NOT NULL'
            unique_columns = key_cols
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
        non_key_cols = tuple(col for col in df.columns if col not in key_cols)
        upserted_rows = []
        upsert_sql = f"""
        INSERT INTO {table} ({', '.join(df.columns)})
        VALUES ({', '.join('?'*df.shape[1])})
        ON CONFLICT DO {f'''UPDATE
        SET {', '.join(f'{col}=excluded.{col}' for col in non_key_cols)}'''
        if non_key_cols else 'NOTHING'}
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
                    f'Table {table!r}: inserted {n_inserted_rows} rows and '
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
        update_sql = f'''
        UPDATE {table}
        SET active = FALSE
        WHERE active = TRUE
            AND rowid NOT IN ({', '.join('?'*len(upserted_rows))})
            AND service = ?
            AND session = ?
        '''
        try:
            logger.debug(update_sql)
            await cursor.execute(
                    sql=update_sql,
                    parameters=(*upserted_rows, service, session))
            logger.info(
                    f'Table {table!r}: Marked {cursor.rowcount} rows '
                    'as inactive.')
        finally:
            if virtual_cursor:
                await cursor.close()

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
                    try:
                        await updater.update_channels(handler=handler)
                    except aiosqlite.Error as e:
                        logger.error(
                                f'{service_name}/{session_name}: '
                                f'{type(e).__name__}: Cannot update '
                                f'channel table in the database: {e}.')

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
