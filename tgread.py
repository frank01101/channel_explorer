#!/usr/bin/env python

"""tgread.py: A utility module for exploring and managing Telegram
groups and channels.

This module provides a comprehensive set of tools for interacting with
Telegram's API using the Telethon library. The functionalities include:
    - Fetching dialogs, channels, and groups the user participates in.
    - Retrieving detailed information about channels and their users.
    - Handling messages, including fetching and filtering by content.

Classes:
    TelegramReader:
        Manages the Telegram client and provides high-level operations
        for interacting with Telegram data.

Functions:
    print_channel_info():
        Prints information about the Telegram's type of given channels.

Requirements:
    - Telethon library
    - Telegram API credentials (API_ID, API_HASH) from my.telegram.org
"""

__author__ = 'Franciszek Humieja'
__copyright__ = 'Copyright (c) 2025 Franciszek Humieja'
__license__ = 'MIT'
__version__ = '1.1.0'

import asyncio
import logging

from datetime import datetime

from telethon import TelegramClient
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import User, Chat, Channel, Message, MessageService
from telethon.tl.types.messages import ChatFull
from telethon.tl.types.users import UserFull
from telethon.tl.custom.dialog import Dialog
from telethon.hints import EntityLike
from telethon.helpers import TotalList
from telethon.errors import RPCError

# Local logger
logger = logging.getLogger(__name__)


class TelegramReader:
    """A class for exploring and managing Telegram dialogs using
    the Telethon library.
    ----------
    This class abstracts the underlying Telethon client to simplify
    fetching channels, messages, and users from Telegram. It also
    handles common tasks like batching requests, managing rate limits,
    and logging.
    ----------
    Public attributes:
    client (TelegramClient):
        The Telethon client instance used for API access.
    ----------
    Public methods:
        start(): Starts the Telegram client asynchronously.
        stop(): Stops the Telegram client and performs cleanup.
        get_channels(): Gets channels/groups the user participates in.
        get_full_channels(): Gets detailed information about dialogs.
        get_new_messages(): Gets new or updated messages from a dialog.
        get_all_messages(): Fetches all messages from a dialog.
        get_users(): Fetches participants of a group or channel.
        get_full_users(): Retrieves detailed information about users.
        get_invite_links(): Fetches messages that contain invite links.
    ----------
    Example:
        from tgread import TelegramReader
        reader = TelegramReader(
                session_name='mysession', api_id=1234, api_hash='ab123')
        ----------
        # Case 1: connecting and disconnecting manually
        # by calling start() and stop():
        await reader.start()
        channels = await reader.get_channels()
        some_users = await reader.get_users(channels[3])
        await reader.stop()
        ----------
        # Case 2: connecting and disconnecting automatically
        # by using ”with“ block:
        async with reader:
            channels = await reader.get_channels()
            full_channels = await reader.get_full_channels(*channels)
            some_messages = await reader.get_all_messages(channels[0])
    """

    service_name = 'telegram'

    def __init__(self, session_name: str, api_id: int, api_hash: str):
        self.session_name = session_name
        self.client = TelegramClient(
                session=session_name, api_id=api_id, api_hash=api_hash)
        # Telethon's automatic flood wait (24*60*60 means always).
        self.client.flood_sleep_threshold = 24*60*60
        # Number of requests to be passed by the semaphores at once
        # (batch sizes) and time to wait (in seconds) after the semaphores
        # get locked in order to prevent the occurrence of FloodWaitError.
        self._full_channels_batch = 24
        self._full_users_batch = 30
        self._full_channels_delay = 30  # [seconds]
        self._full_users_delay = 30  # [seconds]
        self._channels_unreleased = 0
        self._users_unreleased = 0
        self._full_channels_semaphore = asyncio.Semaphore(
                value=self._full_channels_batch)
        self._full_users_semaphore = asyncio.Semaphore(
                value=self._full_users_batch)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def start(self):
        """Starts the Telegram client asynchronously."""
        try:
            await self.client.start()
        except Exception as e:
            await self._log_error(error=e, context='starting Telegram client')

    async def stop(self):
        """Stops the Telegram client asynchronously."""
        try:
            await self.client.disconnect()
        except Exception as e:
            await self._log_error(error=e, context='stopping Telegram client')

    async def get_channels(
            self, *, meta_info: bool = True) -> list[Dialog|Channel|Chat]:
        """Gets all groups (small group chats) and channels (broadcasts,
        megagroups, gigagroups) the user is in. 
        ----------
        Argument:
        meta_info: bool = True (optional)
            if True, the function returns a list of Dialog objects;
            if False, the function returns a list of Chat/Channel
            objects;
        ----------
        Returns:
        list[telethon.tl.custom.dialog.Dialog |
             telethon.tl.types.Channel |
             telethon.tl.types.Chat]
            list of all groups and channels the user is in;
        """
        channels = []
        try:
            async for dialog in self.client.iter_dialogs():
                # IDs of groups and channels are always negative
                # -- contrary to private chats
                if dialog.id < 0:
                    if meta_info:
                        # append the Dialog class object (an abstract of
                        # Chat/Channel class)
                        channels.append(dialog)
                    else:
                        # append the Chat/Channel class object
                        channels.append(dialog.entity)
        except Exception as e:
            await self._log_error(error=e, context='fetching channels')
        logger.info(
                f'{self.session_name}: Retrieved {len(channels)} '
                'channels/groups.')
        return channels

    async def get_full_channels(self, *dialogs: EntityLike) -> list[ChatFull]:
        """Gets a list of ChatFull objects containing full information
        about the given dialogs (private chats, group chats, channels).
        ----------
        Args:
        *dialogs: telethon.hints.EntityLike
            anything that can be associated by Telethon with given
            dialogs;
        ----------
        Returns:
        list[telethon.tl.types.messages.ChatFull]
            a list of ChatFull objects of the given dialogs;
        """
        async def get_single_full_channel(dialog: EntityLike) -> ChatFull:
            """A single fetch task for a full chat/channel utilizing
            a custom semaphore behavior: 
            - it does not get released after a task passes, until it
            gets locked;
            - when it gets locked, it performs all the outstanding
            releases after the past tasks.
            This manual semaphore control is very efficient and
            compatible with Telegram's flood wait limitations for cases
            of multiple asynchronous calls of self.get_full_channels().
            """
            await self._full_channels_semaphore.acquire()
            try:
                try:
                    full_channel = await self.client(
                            GetFullChannelRequest(channel=dialog))
                    return full_channel
                except TypeError:
                    dialog_id = await self.client.get_peer_id(peer=dialog)
                    full_chat = await self.client(
                            GetFullChatRequest(chat_id=abs(dialog_id)))
                    return full_chat
            except Exception as e:
                await self._log_error(
                        error=e,
                        context='fetching full channel',
                        dialog=dialog)
                return None
            finally:
                if self._full_channels_semaphore.locked():
                    # Sleep to prevent GetFullChannelRequest flood wait.
                    await asyncio.sleep(delay=self._full_channels_delay)
                    # Release the semaphore after the current task.
                    self._full_channels_semaphore.release()
                    while self._channels_unreleased > 0:
                        # Release the semaphore after all the past
                        # unreleased tasks.
                        self._full_channels_semaphore.release()
                        self._channels_unreleased -= 1
                else:
                    self._channels_unreleased += 1
        if len(dialogs) >= self._full_channels_batch:
            wait_time = (
                    len(dialogs)//self._full_channels_batch
                    *self._full_channels_delay)
            logger.info(
                    f'{self.session_name}: Started fetching full '
                    f'information for {len(dialogs)} chats/channels. It '
                    f'will take more than {wait_time}s for this batch to '
                    'be fetched, due to flood wait...')
        async with asyncio.TaskGroup() as tg:
            fetch_tasks = [
                    tg.create_task(get_single_full_channel(dialog))
                    for dialog in dialogs]
        full_channels = [task.result() for task in fetch_tasks]
        failed_count = sum(channel is None for channel in full_channels)
        logger.info(
                f'{self.session_name}: Retrieved full information for '
                f'{len(full_channels)-failed_count} '
                f'and failed for {failed_count} channels/groups.')
        return full_channels

    async def get_new_messages(
            self,
            dialog: EntityLike,
            known_ids: list[int] = None,
            save_dates: list[datetime] = None,
            *,
            min_id: int = 0,
            limit: int = None,
            search: str = None) -> list[Message|MessageService]:
        """Gets new messages in a given group or channel. New messages
        can be obtained in either way:
        1) As messages with ids different than ids of known messages
        passed as a list in known_ids. Moreover, it gives the
        opportunity to retrieve also messages that are already listed in
        known_ids but were edited later than corresponding save dates in
        the save_dates list. If save_dates is passed, then known_ids and
        save_dates must have the same length and must have the
        corresponding elements (related to the same message) at the same
        index.
        2) As messages with id greater than or equal to the value passed
        as min_id. Note that without passing known_ids and save_dates
        there is no way to get messages that are already known but were
        edited afterwards.
        ----------
        Args:
        dialog: telethon.hints.EntityLike
            anything that can be associated by Telethon with a given
            dialog;
        known_ids: list[int] = None (optional)
            a list of ids of already known messages;
        save_dates: list[datetime.datetime] = None (optional)
            a list of dates of save for already known messages;
            optional, even when known_ids is passed;
        min_id: int = 0 (optional)
            an id of the oldest message to be retrieved;
        limit: int = None (optional)
            maximal number of messages to be retrieved in each
            channel/group;
        search: string = None (optional)
            a string that will be searched in messages; only messages
            containing this string will be returned.
        ----------
        Returns:
        list[telethon.tl.types.Message |
             telethon.tl.types.MessageService]
            a list of new messages;
        """
        known_ids = known_ids or []
        save_dates = save_dates or []
        messages = []
        edited_count = 0
        try:
            if save_dates and len(known_ids) != len(save_dates):
                raise ValueError(
                        'Lengths of the known_ids and save_dates lists '
                        'are not equal.')
            async for message in self.client.iter_messages(
                    entity=dialog,
                    min_id=min_id,
                    limit=limit,
                    search=search):
                is_new = message.id not in known_ids
                is_edited = (
                        not is_new
                        and message.edit_date is not None
                        and save_dates
                        and message.edit_date > save_dates[
                            known_ids.index(message.id)])
                if is_new or is_edited:
                    messages.append(message)
                    edited_count += 1 if is_edited else 0
        except Exception as e:
            await self._log_error(
                    error=e, context='fetching new messages', dialog=dialog)
        logger.info(
                f'{self.session_name}: '
                + (f'Search for ”{search}“: '
                if isinstance(search, str) and search else '')
                + f'Retrieved {len(messages) - edited_count} new and '
                f'{edited_count} updated messages from the dialog.')
        return messages

    async def get_all_messages(
            self,
            dialog: EntityLike,
            *,
            limit: int = None,
            search: str = None) -> TotalList[Message|MessageService]:
        """Gets all messages in a given group or channel.
        ----------
        Args:
        dialog: telethon.hints.EntityLike
            anything that can be associated by Telethon with a given
            dialog;
        limit: int = None (optional)
            the number of messages to be fetched (from the newest);
        search: string = None (optional)
            a string that will be searched in messages; only messages
            containing this string will be returned.
        ----------
        Returns:
        telethon.helpers.TotalList[
                telethon.tl.types.Message |
                telethon.tl.types.MessageService]
            a TotalList of all messages;
        """
        try:
            messages = await self.client.get_messages(
                    entity=dialog, limit=limit, search=search)
            logger.info(
                    f'{self.session_name}: '
                    + (f'Search for ”{search}“: '
                    if isinstance(search, str) and search else '')
                    + f'Retrieved {len(messages)} out of {messages.total} '
                    'available messages from the dialog.')
            return messages
        except Exception as e:
            await self._log_error(
                    error=e, context='fetching all messages', dialog=dialog)
            return TotalList()

    async def get_invite_links(self, dialog: EntityLike) -> TotalList[Message]:
        """Gets all messages in a given group or channel containing
        Telegram invite links.
        With a given argument 'dialog', this method is an alias for:
        TelegramReader.get_all_messages(
                dialog=dialog, search='https://t.me/joinchat/')
        ----------
        Argument:
        dialog: telethon.hints.EntityLike
            anything that can be associated by Telethon with a given
            dialog;
        ----------
        Returns:
        TotalList[telethon.tl.types.Message]
            a list of messages containing invite links;
        """
        link_pattern = 'https://t.me/joinchat/'
        return await self.get_all_messages(dialog=dialog, search=link_pattern)

    async def get_users(self, dialog: EntityLike) -> TotalList[User]:
        """Gets all participants of a given group or channel. Assigns
        channel id to each participant.
        ----------
        Argument:
        dialog: telethon.hints.EntityLike
            anything that can be associated by Telethon with a given
            dialog;
        ----------
        Returns:
        telethon.helpers.TotalList[telethon.tl.types.User]
            a TotalList of all participants
        """
        try:
            participants = await self.client.get_participants(entity=dialog)
            for user in participants:
                user.channel = await self.client.get_peer_id(peer=dialog)
            logger.info(
                    f'{self.session_name}: Retrieved {len(participants)} '
                    f'out of {participants.total} available participants '
                    'from the dialog.')
            return participants
        except Exception as e:
            await self._log_error(
                    error=e,
                    context='fetching participants',
                    dialog=dialog)
            return TotalList()

    async def get_full_users(self, *users: EntityLike) -> list[UserFull]:
        """Gets a list of UserFull objects containing full information
        about the given users.
        ----------
        Args:
        *users: telethon.hints.EntityLike
            anything that can be associated by Telethon with given
            users;
        ----------
        Returns:
        list[telethon.tl.types.UserFull]
            a list of UserFull objects of the given users;
        """
        async def get_single_full_user(user: EntityLike) -> UserFull:
            """A single fetch task for a full user utilizing a custom
            semaphore behavior: 
            - it does not get released after a task passes, until it
            gets locked;
            - when it gets locked, it performs all the outstanding
            releases after the past tasks.
            This manual semaphore control is very efficient and
            compatible with Telegram's flood wait limitations for cases
            of multiple asynchronous calls of self.get_full_users().
            """
            await self._full_users_semaphore.acquire()
            try:
                full_user = await self.client(GetFullUserRequest(user))
                return full_user
            except Exception as e:
                await self._log_error(
                        error=e, context='fetching full user')
                return None
            finally:
                if self._full_users_semaphore.locked():
                    # Sleep to prevent GetFullUserRequest flood wait.
                    await asyncio.sleep(delay=self._full_users_delay)
                    # Release the semaphore after the current task.
                    self._full_users_semaphore.release()
                    while self._users_unreleased > 0:
                        # Release the semaphore after all the past
                        # unreleased tasks.
                        self._full_users_semaphore.release()
                        self._users_unreleased -= 1
                else:
                    self._users_unreleased += 1
        if len(users) >= self._full_users_batch:
            wait_time = (
                    len(users)//self._full_users_batch*self._full_users_delay)
            logger.info(
                    f'{self.session_name}: Started fetching full '
                    f'information for {len(users)} users. It will take '
                    f'more than {wait_time}s for this batch to be fetched, '
                    'due to flood wait...')
        fetch_tasks = []
        async with asyncio.TaskGroup() as tg:
            fetch_tasks = [
                    tg.create_task(get_single_full_user(user))
                    for user in users]
        full_users = [task.result() for task in fetch_tasks]
        failed_count = sum(user is None for user in full_users)
        logger.info(
                f'{self.session_name}: Retrieved full information for '
                f'{len(full_users)-failed_count} '
                f'and failed for {failed_count} users.')
        return full_users

    async def _log_error(
            self,
            error: Exception,
            context: str,
            dialog: EntityLike = None) -> None:
        if isinstance(error, RPCError):
            context = ' '.join((context, '(Telegram API error)'))
        if dialog is not None:
            try:
                dialog_id = await self.client.get_peer_id(peer=dialog)
            except Exception as e:
                logger.warning(
                        f'{self.session_name}: {type(e).__name__}: '
                        f'Could not retrieve dialog id for given entity.')
                dialog_id = None
            logger.error(
                    f'{self.session_name}: {type(error).__name__}: '
                    f'{dialog_id=}: Error while {context}: {error}')
        else:
            logger.error(
                    f'{self.session_name}: {type(error).__name__}: '
                    f'Error while {context}: {error}')

def check_entity_type(*entities: Dialog|User|Chat|Channel) -> None:
    """Prints the Telegram type of given dialogs or entities.
    ----------
    Args:
    *entities: telethon.tl.custom.dialog.Dialog |
               telethon.tl.types.User |
               telethon.tl.types.Chat |
               telethon.tl.types.Channel
        entities to be checked;
    ----------
    Returns:
    None
    """
    for entity in entities:
        try:
            if isinstance(entity, Dialog):
                if isinstance(entity.entity, User):
                    print(f'Dialog “{entity.name}” is a private chat')
                elif isinstance(entity.entity, Chat):
                    print(f'Dialog “{entity.name}” is a group chat')
                elif isinstance(entity.entity, Channel):
                    print(f'Dialog “{entity.name}” is a',
                          'broadcast' if entity.entity.broadcast
                          else 'gigagroup' if entity.entity.gigagroup
                          else 'megagroup' if entity.entity.megagroup
                          else 'unknown type',
                          'channel')
                else:
                    print('Unable to determine the type of dialog.')
            elif isinstance(entity, User):
                print(f'Entity “{entity.first_name} {entity.last_name}” is',
                      'a user')
            elif isinstance(entity, Chat):
                print(f'Entity “{entity.title}” is a group')
            elif isinstance(entity, Channel):
                print(f'Entity “{entity.title}” is a',
                      'broadcast' if entity.broadcast 
                      else 'gigagroup' if entity.gigagroup 
                      else 'megagroup' if entity.megagroup 
                      else 'unknown type',
                      'channel')
            else:
                print(f'Entity is another kind of non-dialog entity')
        except Exception as e:
            logger.error(
                    f'{type(e).__name__}: Error while checking entity '
                    f'type: {e}')

async def main() -> None:
    """Prints basic information about channels the program tracks."""

    async def print_channel_info(
            session_name: str,
            reader: TelegramReader,
            channel: Dialog|Channel|Chat) -> None:
        line_len = 24
        users = await reader.get_users(dialog=channel)
        messages = await reader.get_all_messages(dialog=channel, limit=None)
        print('\n' + line_len*'-', session_name, line_len*'-')
        check_entity_type(channel)
        print(f'It claims to have {channel.entity.participants_count}',
              f'participants and {channel.message.id} messages')
        print(f'Retrieved {len(users)} out of {users.total}',
              'available participants.')
        print(f'Retrieved {len(messages)} out of {messages.total}',
              'available messages.')
        print((len(session_name) + 2*(line_len + 1))*'-')

    async def process_session(session_data: dict) -> None:
        line_len = 16
        try:
            session_name = session_data['session']
            api_id = session_data['api_id']
            api_hash = session_data['api_hash']
        except KeyError as e:
            try:
                print('\n' + line_len*'=', session_name, line_len*'=')
                print('Error while reading login credentials:',
                      f'missing key: {e}.')
                print((len(session_name) + 2*(line_len + 1))*'=')
            except UnboundLocalError:
                print('\n' + (2*line_len + 6)*'=')
                print('Error while reading login credentials for',
                      f'one of the sessions: missing key: {e}.')
                print((2*line_len + 6)*'=')
        else:
            async with TelegramReader(
                    session_name, api_id, api_hash) as reader:
                channels = await reader.get_channels(meta_info=True)
                print('\n' + line_len*'=', session_name, line_len*'=')
                print(f'You participate in {len(channels)} channels/groups.')
                print((len(session_name) + 2*(line_len + 1))*'=')
                async with asyncio.TaskGroup() as tg:
                    for channel in channels:
                        tg.create_task(
                                print_channel_info(
                                    session_name, reader, channel))

    config_fname = 'config.json'
    try:
        with open(config_fname, 'r') as f:
            config = json.load(f)
    except FileNotFoundError as e:
        print(f'Cannot find configuration file: {e}.')
    else:
        async with asyncio.TaskGroup() as tg:
            try:
                for session in config['sessions']['telegram']:
                    tg.create_task(process_session(session))
            except KeyError as e:
                print(f'Configuration file “{config_fname}” does not contain '
                      f'necessary key: {e}. Cannot print session information.')

if __name__ == '__main__':
    import json
    from logging_config import root_logger
    asyncio.run(main())
