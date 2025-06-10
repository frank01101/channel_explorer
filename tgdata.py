"""tgdata.py: A utility module for extracting data from Telegram groups
and channels, and processing it into pandas' data frames.

This module provides a set of tools for retrieving data of Telegram's
groups/channels, as well as of their users and messages, and storing it
in pandas DataFrames. The functionalities include:
    - Getting dialogs, channels, and groups the user participates in, as
    well as detailed information about channels and their participants.
    - Managing asynchronous fetching of data.
    - Converting data into pandas DataFrames and performing necessary
    data wrangling and cleaning.

Classes:
    TelegramDataHandler:
        Handles data from Telegram groups/channels.

Requirements:
    - pandas library
    - telethon library
    - Telegram API credentials (API_ID, API_HASH) from my.telegram.org
    - tgread.py module
"""

__author__ = 'Franciszek Humieja'
__copyright__ = 'Copyright (c) 2025 Franciszek Humieja'
__license__ = 'MIT'
__version__ = '1.1.2'

import pandas as pd
import asyncio
import logging

from telethon.tl.types import (
        Chat, Channel, ChatFull, ChannelFull, Message, MessageService, User,
        UserFull)
from telethon.tl.types.messages import ChatFull as MetaChatFull
from telethon.tl.types.users import UserFull as MetaUserFull
from telethon.tl.custom.dialog import Dialog
from telethon.hints import EntityLike

from tgread import TelegramReader

# Local logger
logger = logging.getLogger(__name__)


class TelegramDataHandler(TelegramReader):
    """A handler class for processing Telegram data using Telethon
    objects, converting them into pandas DataFrames.
    ----------
    This class extends TelegramReader to provide additional
    functionalities for transforming Telegram's Telethon objects (such
    as channels, messages and users) into unified DataFrame
    representations, conducting necessary data wrangling and cleaning.
    ----------
    Public methods:
        get_channels_dicts(*channels):
            Converts chat/channel data into dictionaries.
        get_channels_frame(*channels):
            Converts chat/channel data into a pandas DataFrame.
        get_all_channels_frame(*, meta_info=True, full_info=True):
            Asynchronously retrieves all Telegram chat/channel data
            and converts it into a DataFrame.
        ----------
        get_messages_dicts(*message_lists):
            Converts message data into dictionaries.
        get_messages_frame(*message_lists):
            Converts message data into a pandas DataFrame.
        get_all_messages_frame(*channels, limit=None):
            Asynchronously retrieves all message data from the given
            Telegram channels and converts it into a DataFrame.
        get_new_messages_frame(
                channels_df, messages_df, *,
                full_check=True, limit=None):
            Asynchronously retrieves new message data (not stored yet)
            from the given Telegram channels and converts it into a
            DataFrame.
        ----------
        get_users_dicts(*user_lists):
            Converts user data into dictionaries.
        get_users_frame(*user_lists):
            Converts user data into a pandas DataFrame.
        get_all_users_frame(*channels, full_info=False):
            Asynchronously retrieves all user data from the given
            Telegram channels and converts it into a DataFrame.
    ----------
    Example:
        from tgdata import TelegramDataHandler
        handler = TelegramDataHandler(
                session_name='mysession', api_id=1234, api_hash='ab123')
        ----------
        # Case 1: connecting and disconnecting manually
        # by calling start() and stop():
        await handler.start()  # inherited method
        channels_df = await handler.get_all_channels_frame()
        messages_df = await handler.get_all_messages_frame(channels_df)
        await handler.stop()  # inherited method
        ----------
        # Case 2: connecting and disconnecting automatically
        # by using ”with“ block:
        async with handler:
            channels = await handler.get_channels()  # inherited method
            channels_df = await handler.get_all_channels_frame()
            new_messages_df = await handler.get_new_messages_frame(
                    channels_df.loc[4:8, :], known_messages_df)
            users_df = await handler.get_all_users_frame(
                    channels[1], channels_df)
    """

    def get_channels_dicts(
            self,
            *channels: Dialog|Chat|Channel|ChatFull|ChannelFull|MetaChatFull
    ) -> (list[dict], list[dict]):
        """Converts Telegram channel objects into lists of dictionaries.
        ----------
        Processes a variable number of Telegram objects representing
        chats or channels and separates their information into two
        lists: one for basic channel information and one for extended
        (full) channel information. Any objects that do not match the
        expected types are skipped with a warning.
        ----------
        Args:
        *channels: telethon.tl.custom.dialog.Dialog |
                   telethon.tl.types.Chat |
                   telethon.tl.types.Channel |
                   telethon.tl.types.ChatFull |
                   telethon.tl.types.ChannelFull |
                   telethon.tl.types.messages.ChatFull
            telethon chat/channel objects;
        ----------
        Returns:
        (list[dict], list[dict])
            a tuple of two lists:
              - the first contains dictionaries with basic channel data;
              - the second contains dictionaries with extended channel
              data;
        """
        # 'channels_dicts' and 'channels_full_dicts': lists of
        # dictionaries describing Channel/Chat (former) or
        # ChannelFull/ChatFull (latter) instances.
        # Scheme: [{dictionary_channel_1}, ..., {dictionary_channel_n}].
        channels_dicts = []
        channels_full_dicts = []
        wrong_channel_types = []
        for channel in channels:
            if isinstance(channel, Dialog):
                channels_dicts.append(channel.entity.to_dict())
                channels_dicts[-1]['last_active'] = channel.date
            elif isinstance(channel, Chat|Channel):
                channels_dicts.append(channel.to_dict())
            elif isinstance(channel, ChatFull|ChannelFull):
                channels_full_dicts.append(channel.to_dict())
            elif isinstance(channel, MetaChatFull):
                channels_full_dicts.append(channel.full_chat.to_dict())
                for chat in channel.chats:
                    channels_dicts.append(chat.to_dict())
            else:
                wrong_channel_types.append(type(channel).__name__)
        if wrong_channel_types:
            types = ', '.join(
                    self._get_types_count(types_list=wrong_channel_types))
            logger.warning(
                    f'{self.session_name}: Skipping objects of the '
                    'following types while creating the DataFrame of '
                    f'channels: {types}.')
        return (channels_dicts, channels_full_dicts)

    def get_channels_frame(
            self,
            *channels: Dialog|Chat|Channel|ChatFull|ChannelFull|MetaChatFull
    ) -> pd.DataFrame:
        """Merges Telegram channel objects into a pandas DataFrame.
        ----------
        Takes a variable number of Telegram objects representing chats
        or channels processes them into dictionaries using 
        TelegramDataHandler.get_channels_dicts(), converts these
        dictionaries into DataFrames, and then merges the resulting
        DataFrames after performing necessary data cleaning.
        ----------
        Args:
        *channels: telethon.tl.custom.dialog.Dialog |
                   telethon.tl.types.Chat |
                   telethon.tl.types.Channel |
                   telethon.tl.types.ChatFull |
                   telethon.tl.types.ChannelFull |
                   telethon.tl.types.messages.ChatFull
            telethon chat/channel objects;
        ----------
        Returns:
        pandas.DataFrame
            a pandas DataFrame containing merged information from the
            input channel objects.
        """
        # 'channels_df': Data of the entity of dialogs---classes
        # Chat or Channel from telethon.tl.types.
        # 'channels_full_df': Data of the classes ChatFull or
        # ChannelFull from telethon.tl.types containing extended
        # information about a Chat/Channel entity.
        # The class telethon.tl.types.messages.ChannelFull contains
        # information of both Chat/Channel and ChatFull/ChannelFull
        # classes which is separated among these two dataframes
        # accordingly; it is preferred to use instances of this class
        # because it also contains information of an additional chat
        # having all the discussion under the channel's posts.
        channels_dicts, channels_full_dicts = (
                self.get_channels_dicts(*channels))
        channels_df = pd.DataFrame(channels_dicts)
        channels_full_df = pd.DataFrame(channels_full_dicts)
        # Below, merging of 'channels_df' and 'channels_full_df' is
        # performed accompanied by necessary data cleaning.
        try:
            # Change the name of one of the columns for clarity.
            channels_df.rename(columns={'date': 'date_joined'}, inplace=True)
            # Drop possible duplicates by grouping by 'id' and taking
            # the first non-null value in each column. This method is
            # used, instead of drop_duplicates(), because some values in
            # columns 'last_active' and 'participants_count' may be null
            # depending on the class the data comes from:
            # - 'last_active' is stored only in Dialog instances,
            # - 'participants_count' can be null when taken from
            #   MetaChatFull.chats.
            channels_df = channels_df.groupby(by='id', as_index=False).first()
        except KeyError as e:
            if channels_df.empty:
                channels_merged_df = channels_full_df
            else:
                channels_merged_df = pd.DataFrame()
                logger.error(
                        f'{self.session_name}: KeyError: Merging Channel '
                        'and ChannelFull data is impossible because the '
                        'non-empty Channel data does not contain necessary '
                        f'features: {e}')
        else:
            try:
                channels_full_df.drop_duplicates(subset='id', inplace=True)
                channels_merged_df = pd.merge(
                        left=channels_df,
                        right=channels_full_df,
                        how='outer',
                        on='id',
                        suffixes=[None, '_full'])
            except KeyError as e:
                channels_merged_df = channels_df
                if not channels_full_df.empty:
                    logger.error(
                            f'{self.session_name}: KeyError: Merging '
                            'Channel and ChannelFull data is impossible '
                            'because the non-empty ChannelFull data does '
                            f'not contain necessary features: {e}')
            else:
                try:
                    # Resolve discrepancies in participants count value
                    # from both merged dataframes---in case when
                    # 'channels_full_df' contains more information than
                    # 'channels_df'.
                    no_participants_count = (
                            channels_merged_df['participants_count'].isna())
                    channels_merged_df.loc[
                            no_participants_count, 'participants_count'] = (
                                channels_merged_df.loc[
                                    no_participants_count,
                                    'participants_count_full'])
                    channels_merged_df.drop(
                            columns='participants_count_full', inplace=True)
                except KeyError as e:
                    logger.warning(
                            f'{self.session_name}: Cannot resolve '
                            'discrepancies in participants count because '
                            'the non-empty merged DataFrame does not '
                            f'contain necessary features: {e}')
        if not channels_merged_df.empty:
            # Add some columns for unification with other messaging
            # services and data updates management.
            channels_merged_df['session'] = self.session_name
            channels_merged_df['service'] = self.service_name
            channels_merged_df['active'] = True
            channels_merged_df['date_saved'] = pd.Timestamp.now(tz='UTC')
            channels_merged_df = channels_merged_df.convert_dtypes()
            logger.info(
                    f'{self.session_name}: Created a DataFrame for '
                    f'{channels_merged_df.shape[0]} channels with '
                    f'{channels_merged_df.shape[1]} features.')
        else:
            logger.info(
                    f'{self.session_name}: Created an empty DataFrame '
                    '(no channels)')
        return channels_merged_df

    async def get_all_channels_frame(
            self,
            *,
            meta_info: bool = True,
            full_info: bool = True) -> pd.DataFrame:
        """Asynchronously retrieves all Telegram chat and channel
        objects the user participates in and compiles them into a
        unified DataFrame.
        ----------
        Args:
        meta_info: bool = True (optional)
            if True, retrieves a list of Dialog objects;
            if False, retrieves a list of Chat/Channel objects;
        full_info: bool = True (optional)
            if True, retrieves additionally the extended (full) chat or
            channel information;
        ----------
        Returns:
        pandas.DataFrame
            a pandas DataFrame containing merged channel data;
        """
        channels = await self.get_channels(meta_info=meta_info)
        channels_full = (
                await self.get_full_channels(*channels) if full_info else [])
        return self.get_channels_frame(*channels, *channels_full)

    def get_messages_dicts(
            self,
            *message_lists: list[Message|MessageService]) -> list[dict]:
        """Converts lists of Telegram message objects into a list of
        dictionaries.
        ----------
        Iterates over the given lists of Telegram objects representing
        messages (types Message or MessageService) and converts each
        message into a dictionary. Objects that are not of the expected
        type are skipped with a warning.
        ----------
        Args:
        *message_lists: list[
                telethon.tl.types.Message |
                telethon.tl.types.MessageService]
            lists of Telegram message objects;
        ----------
        Returns:
        list[dict]
            a list of dictionaries where each dictionary represents a
            message;
        """
        # List of dictionaries, each representing a single message.
        # Scheme: [{message_1 of chat_1}, ..., {message_k1 of chat_1},
        #          {message_1 of chat_2}, ..., {message_k2 of chat_2},
        #          ..., {message_kn of chat_n}]
        messages_dict = []
        wrong_iter_types = []
        wrong_message_types = []
        for single_list in message_lists:
            try:
                for message in single_list:
                    if isinstance(message, Message|MessageService):
                        messages_dict.append(message.to_dict())
                    else:
                        wrong_message_types.append(type(message).__name__)
            except TypeError:
                wrong_iter_types.append(type(single_list).__name__)
        if wrong_iter_types:
            types = ', '.join(
                    self._get_types_count(types_list=wrong_iter_types))
            logger.warning(
                    f'{self.session_name}: Objects of the following types '
                    f'passed where iterable objects expected: {types} -- '
                    'skipping while creating the DataFrame of messages.')
        if wrong_message_types:
            types = ', '.join(
                    self._get_types_count(types_list=wrong_message_types))
            logger.warning(
                    f'{self.session_name}: Skipping objects of the '
                    'following types while creating the DataFrame of '
                    f'messages: {types}.')
        return messages_dict

    def get_messages_frame(
            self,
            *message_lists: list[Message|MessageService]) -> pd.DataFrame:
        """Converts lists of Telegram message objects into a unified
        pandas DataFrame.
        ----------
        Takes lists of Telegram message objects and converts
        them into a list of dictionaries using
        TelegramDataHandler.get_messages_dicts(), then creates a unified
        DataFrame of the input messages.
        ----------
        Args:
        *message_lists: list[
                telethon.tl.types.Message |
                telethon.tl.types.MessageService]
            lists of Telegram message objects;
        ----------
        Returns:
        pandas.DataFrame
            a pandas DataFrame containing merged information from the
            input message objects;
        """
        # Data frame of all the messages from all the chats/channels.
        messages_dict = self.get_messages_dicts(*message_lists)
        messages_df = pd.DataFrame(messages_dict)
        try:
            # Resolve the column 'peer_id' to make it have only the
            # corresponding channel/chat id number instead of the original
            # dictionary.
            peer_id = pd.DataFrame(data=messages_df['peer_id'].to_list())
            peer_id_series = (
                    peer_id
                    .drop(columns='_')
                    .sum(axis=1)
                    .astype('int64'))
            messages_df['peer_id'] = peer_id_series
            # Add some columns for unification with other messaging
            # services data and data updates management.
            messages_df['session'] = self.session_name
            messages_df['service'] = self.service_name
            messages_df['active'] = True
            messages_df['date_saved'] = pd.Timestamp.now(tz='UTC')
            messages_df = messages_df.convert_dtypes()
            logger.info(
                    f'{self.session_name}: Created a DataFrame for '
                    f'{messages_df.shape[0]} messages with '
                    f'{messages_df.shape[1]} features.')
        except KeyError as e:
            if messages_df.empty:
                logger.info(
                        f'{self.session_name}: Created an empty DataFrame '
                        '(no messages).')
            else:
                logger.error(
                        f'{self.session_name}: KeyError: The non-empty '
                        'Messages data does not contain necessary '
                        f'features: {e}')
        return messages_df

    async def get_all_messages_frame(
            self,
            *channels: pd.DataFrame|EntityLike,
            limit: int = None) -> pd.DataFrame:
        """Asynchronously retrieves all Telegram message objects from
        the specified Telegram channels and compiles them into a unified
        DataFrame.
        ----------
        Iterates over the provided channel representations (either as
        DataFrames or EntityLike objects), asynchronously fetching
        messages from each channel, and then merges the results into
        a DataFrame using TelegramDataHandler.get_messages_frame().
        ----------
        Args:
        *channels: pandas.DataFrame | telethon.hints.EntityLike
            a variable number of channel representations (pandas
            DataFrame or EntityLike);
        limit: int = None (optional)
            maximum number of messages to retrieve per channel;
        ----------
        Returns:
        pandas.DataFrame
            a pandas DataFrame containing all fetched message data;
        """
        fetch_messages_tasks = []
        async with asyncio.TaskGroup() as tg:
            def append_fetch_task(entity: EntityLike, limit: int) -> None:
                messages_task = tg.create_task(
                        self.get_all_messages(dialog=entity, limit=limit))
                fetch_messages_tasks.append(messages_task)
            for channel in channels:
                try:
                    channels_df = channel
                    active = channels_df['active']
                    for channel_id in channels_df.loc[active, 'id']:
                        append_fetch_task(entity=int(channel_id), limit=limit)
                except (TypeError, IndexError, AttributeError, KeyError,
                        pd.errors.IndexingError):
                    append_fetch_task(entity=channel, limit=limit)
        all_messages = [task.result() for task in fetch_messages_tasks]
        return self.get_messages_frame(*all_messages)

    async def get_new_messages_frame(
            self,
            channels_df: pd.DataFrame,
            messages_df: pd.DataFrame = None,
            *,
            check_updated: bool = True,
            search_offset: int = 500,
            limit: int = None) -> pd.DataFrame:
        """Asynchronously retrieves new and updated messages from given
        Telegram channels and compiles them into a unified DataFrame.
        ----------
        Compares messages already saved in messages_df with those
        fetched from the channels in channels_df. Only messages that are
        new or have been updated (edited) since the last save are
        retrieved.  If messages_df is empty or lacks necessary columns,
        all messages will be fetched.
        ----------
        Args:
        channels_df: pd.DataFrame
            DataFrame containing channel information;
        messages_df: pd.DataFrame = None (optional)
            previously saved messages DataFrame; if not specified, all
            messages from given chat/channels will be fetched;
        check_updated: bool = True (optional)
            - if True, fetches both new and edited messages; starts
            checking messages from the last known message in messages_df
            for each channel minus the offset given by search_offset;
            note that it is also an opportunity to fetch missing
            messages (that failed to be fetched earlier) within the
            range of the given offset;
            - if False, fetches new messages only, starting from the
            last messages for each chat/channel from messages_df;
        search_offset: int = 500 (optional)
            an offset marking where the search for new and updated
            messages starts; if check_updated=False, it has no effect;
        limit: int = None (optional)
            maximum number of messages to check per channel;
        ----------
        Returns:
        pandas.DataFrame
            a pandas DataFrame containing the new and updated messages;
        """
        try:
            # A mask excluding past channels that we no longer
            # participate in.
            active = channels_df['active']
            # A mask excluding older versions of edited messages.
            up_to_date = messages_df['active']
            fetch_messages_tasks = []
            try:
                async with asyncio.TaskGroup() as tg:
                    for channel_id in channels_df.loc[active, 'id']:
                        in_current_channel = (
                                messages_df['peer_id'] == channel_id)
                        max_id = messages_df.loc[
                                in_current_channel & up_to_date, 'id'].max()
                        if check_updated:
                            ids = messages_df.loc[
                                    in_current_channel & up_to_date,
                                    'id'].to_list()
                            dates = messages_df.loc[
                                    in_current_channel & up_to_date,
                                    'date_saved'].to_list()
                            # Check N=<search_offset> messages before the
                            # last message saved in order to seek for
                            # updated (or missing) messages.
                            start_id = max_id - search_offset
                        else:
                            ids = None
                            dates = None
                            start_id = max_id
                        fetch_messages_tasks.append(
                                tg.create_task(
                                    self.get_new_messages(
                                        dialog=int(channel_id),
                                        known_ids=ids,
                                        save_dates=dates,
                                        min_id=start_id,
                                        limit=limit)))
                new_messages = [
                        task.result() for task in fetch_messages_tasks]
                return self.get_messages_frame(*new_messages)
            except* (KeyError, AttributeError) as e_group:
                for e in e_group.exceptions:
                    raise e
        except (KeyError, TypeError, AttributeError) as e:
            if messages_df is None:
                messages_df = pd.DataFrame()
            try:
                if not messages_df.empty:
                    logger.error(
                            f'{self.session_name}: {type(e).__name__}: '
                            'Passed non-empty messages_df DataFrame that '
                            f'does not contain necessary columns: {e}. '
                            'All messages will be fetched instead of the '
                            'new ones.')
            except AttributeError:
                logger.error(
                        f'{self.session_name}: {type(e).__name__}: Passed '
                        f'a {type(messages_df).__name__} object as '
                        'messages_df where pandas.DataFrame type expected. '
                        'All messages will be fetched instead of the new '
                        'ones.')
            return await self.get_all_messages_frame(channels_df, limit=limit)

    def get_users_dicts(
            self,
            *user_lists: list[User|UserFull|MetaUserFull]
    ) -> (list[dict], list[dict]):
        """Converts Telegram user objects into lists of dictionaries.
        ----------
        Processes a variable number of lists of Telegram objects
        representing users and extracts their information into two
        lists: one for basic user data and one for extended (full) user
        data. Any objects that do not match the expected types are
        skipped with a warning.
        ----------
        Args:
        *user_lists: list[
                telethon.tl.types.User |
                telethon.tl.types.UserFull |
                telethon.tl.types.users.UserFull]
            lists containing user objects;
        ----------
        Returns:
        (list[dict], list[dict])
            a tuple of two lists:
              - the first contains dictionaries with basic user data;
              - the second contains dictionaries with extended user
              data;
        """
        # Lists of dictionaries, each representing a single user.
        # users_dict contains basic information about a user (class
        # telethon.tl.types.User), while user_full_dict contains
        # extended information about a user (class
        # telethon.tl.types.UserFull).
        # Scheme: [{user_1 of chat_1}, ..., {user_k1 of chat_1},
        #          {user_1 of chat_2}, ..., {user_k2 of chat_2},
        #          ..., {user_kn of chat_n}]
        users_dict = []
        users_full_dict = []
        wrong_iter_types = []
        wrong_user_types = []
        for single_list in user_lists:
            try:
                for user in single_list:
                    try:
                        channel_id = user.channel
                    except AttributeError:
                        channel_id = pd.NA
                    if isinstance(user, User):
                        users_dict.append(user.to_dict())
                        users_dict[-1]['channel'] = channel_id
                    elif isinstance(user, UserFull):
                        users_full_dict.append(user.to_dict())
                        users_full_dict[-1]['channel'] = channel_id
                    elif isinstance(user, MetaUserFull):
                        users_full_dict.append(user.full_user.to_dict())
                        users_full_dict[-1]['channel'] = channel_id
                        for inside_user in user.users:
                            users_dict.append(inside_user.to_dict())
                            users_dict[-1]['channel'] = channel_id
                    else:
                        wrong_user_types.append(type(user).__name__)
            except TypeError:
                wrong_iter_types.append(type(single_list).__name__)
        if wrong_iter_types:
            types = ', '.join(
                    self._get_types_count(types_list=wrong_iter_types))
            logger.warning(
                    f'{self.session_name}: Objects of the following types '
                    f'passed where iterable objects expected: {types} -- '
                    'skipping while creating the DataFrame of users.')
        if wrong_user_types:
            types = ', '.join(
                    self._get_types_count(types_list=wrong_user_types))
            logger.warning(
                    f'{self.session_name}: Skipping objects of the '
                    'following types while creating the DataFrame of '
                    f'users: {types}.')
        return (users_dict, users_full_dict)

    def get_users_frame(
            self,
            *user_lists: list[User|UserFull|MetaUserFull]) -> pd.DataFrame:
        """Converts Telegram user objects into a pandas DataFrame.
        ----------
        Takes multiple lists of Telegram user objects and converts them
        into dictionaries using TelegramDataHandler.get_users_dicts(),
        then creates separate DataFrames for basic and extended user
        data. These DataFrames are then merged and data aggregated such
        that each row represents a single user with a list of the
        channels this user participates in stored at 'channels' column.
        ----------
        Args:
        *user_lists: list[
                telethon.tl.types.User |
                telethon.tl.types.UserFull |
                telethon.tl.types.users.UserFull]
            lists containing user objects;
        ----------
        Returns:
        pandas.DataFrame
            a pandas DataFrame containing merged and processed data from
            the input user objects;
        """
        # Data frames of all the users from all the chats/channels.
        # 'users_df': Data of the users entities as saved in a class
        # telethon.tl.types.User.
        # 'users_full_df': Data of the class telethon.tl.types.UserFull,
        # containing extended information about a User entity.
        # The class telethon.tl.types.users.UserFull is an abstract
        # gathering information of both above User and UserFull classes
        # and this information is spread between these two dataframes
        # accordingly.
        users_dict, users_full_dict = self.get_users_dicts(*user_lists)
        users_df = pd.DataFrame(users_dict)
        users_full_df = pd.DataFrame(users_full_dict)
        try:
            users_df['channel'] = users_df['channel'].astype('Int64')
            users_df.drop_duplicates(subset=['id', 'channel'], inplace=True)
        except KeyError as e:
            if users_df.empty:
                users_merged_df = users_full_df
            else:
                users_merged_df = pd.DataFrame()
                logger.error(
                        f'{self.session_name}: KeyError: Merging User '
                        'and UserFull data is impossible because the '
                        'non-empty User data does not contain necessary '
                        f'features: {e}')
        else:
            try:
                users_full_df['channel'] = (
                        users_full_df['channel'].astype('Int64'))
                users_full_df.drop_duplicates(
                        subset=['id', 'channel'], inplace=True)
                users_merged_df = pd.merge(
                        left=users_df,
                        right=users_full_df,
                        how='outer',
                        on=['id', 'channel'],
                        suffixes=[None, '_full'])
            except KeyError as e:
                users_merged_df = users_df
                if not users_full_df.empty:
                    logger.error(
                            f'{self.session_name}: KeyError: Merging User '
                            'and UserFull data is impossible because the '
                            'non-empty UserFull data does not contain '
                            f'necessary features: {e}')
        try:
            # Group examples by 'id' in order to merge data from within
            # each group (i.e. examples with the same 'id') for all the
            # columns except 'channel'. This merging takes the first
            # non-NA value in a given column for each group as a result.
            # For the 'channel' column, the result is a list of all
            # unique and non-NA values from this column in each group.
            # If typical groups become bigger than a few dozens,
            # consider using pd.unique(x.dropna()) instead of
            # set(x.dropna()) in 'channel' column, because the former
            # can work faster for bigger groups.
            users_grouped_df = (
                    users_merged_df
                    .groupby(by='id', as_index=False)
                    .agg({
                        **{col: 'first' for col in users_merged_df.columns
                           if col != 'channel'},
                        'channel': (
                            lambda x: list(set(x.dropna().astype('int64'))))}))
            users_grouped_df.rename(
                    columns={'channel': 'channels'}, inplace=True)
        except KeyError:
            users_grouped_df = pd.DataFrame()
            logger.info(
                    f'{self.session_name}: Created an empty DataFrame '
                    '(no users).')
        else:
            # Add some columns for unification with other messaging
            # services and data updates management.
            users_grouped_df['session'] = self.session_name
            users_grouped_df['service'] = self.service_name
            users_grouped_df['active'] = True
            users_grouped_df['date_saved'] = pd.Timestamp.now(tz='UTC')
            users_grouped_df = users_grouped_df.convert_dtypes()
            logger.info(
                    f'{self.session_name}: Created a DataFrame for '
                    f'{users_grouped_df.shape[0]} users with '
                    f'{users_grouped_df.shape[1]} features.')
        return users_grouped_df

    async def get_all_users_frame(
            self,
            *channels: pd.DataFrame|EntityLike,
            full_info: bool = False) -> pd.DataFrame:
        """Asynchronously retrieves all Telegram users objects from the
        specified Telegram channels and compiles them into a unified
        DataFrame.
        ----------
        Iterates over the provided channel representations (either as
        DataFrames or EntityLike objects), asynchronously fetching
        users from each channel, and then merges the results into a
        DataFrame using TelegramDataHandler.get_users_frame(). If
        full_info is True, also retrieves extended user information
        which can take much time (roughly, one second per one user).
        ----------
        Args:
        *channels: pandas.DataFrame | telethon.hints.EntityLike
            a variable number of channel representations (pandas
            DataFrame or EntityLike);
        full_info: bool = False (optional)
            if True, retrieves extended user information (time costly,
            1 second per user);
        ----------
        Returns:
        pandas.DataFrame
            a pandas DataFrame containing all fetched user data from the
            specified channels;
        """
        fetch_users_tasks = []
        fetch_full_users_tasks = []
        seen_user_ids = set()
        async with asyncio.TaskGroup() as tg:
            async def append_fetch_task(
                    entity: EntityLike, full_info: bool) -> None:
                nonlocal seen_user_ids
                users_task = tg.create_task(self.get_users(dialog=entity))
                fetch_users_tasks.append(users_task)
                if full_info:
                    # Fetch full users without repetitions.
                    user_ids = {user.id for user in await users_task}
                    user_ids -= seen_user_ids
                    seen_user_ids |= user_ids
                    full_users_task = (
                            tg.create_task(self.get_full_users(*user_ids)))
                    fetch_full_users_tasks.append(full_users_task)
            for channel in channels:
                try:
                    channels_df = channel
                    active = channels_df['active']
                    for channel_id in channels_df.loc[active, 'id']:
                        await append_fetch_task(
                                entity=int(channel_id), full_info=full_info)
                except (TypeError, IndexError, AttributeError, KeyError,
                        pd.errors.IndexingError):
                    await append_fetch_task(
                            entity=channel, full_info=full_info)
        users = [task.result() for task in fetch_users_tasks]
        full_users = [task.result() for task in fetch_full_users_tasks]
        return self.get_users_frame(*users, *full_users)

    @staticmethod
    def _get_types_count(types_list: list[str]) -> set[str]:
            types_unique = set(types_list)
            types_count = {
                    f'{single_type} ({types_list.count(single_type)} '
                    + ('time' if types_list.count(single_type) == 1
                       else 'times')
                    + ')'
                    for single_type in types_unique}
            return types_count
