# Channel Explorer

## Description
Channel Explorer is a program used to retrieve information related to channels and groups that the user-controlled accounts participate in within selected instant messaging services, as well as to interact with other users from the userbot level as a chatbot. Moreover, the program creates and manages a database of channels and groups, their users and messages sent in them. The project utilizes libraries for communication with the application programming interface (API) of instant messaging applications (e.g., Telegram) and large language models (e.g., GPT). This program consists of several separate modules, each carrying out specific purposes.

The project can be useful for a broad range of purposes in which immiediate contact with a target group of messaging service users is required. This includes, but is not limited to, customer service, community management, satisfaction surveys, marketing, etc.

The currently supported instant messaging service is Telegram but more services are going to be added in the future.

## General plan of the program operation
1. Updating the database of all observed channels and groups.
2. Updating the database of all users of these channels and groups.
3. Updating the database of conversations from these channels and groups.
4. (In preparation) Updating the database of highlighted messages that require action – as defined by the user (e.g., complaints, questions, doubts, etc.)
5. (In preparation) Using the large language model (LLM) to talk to users of channels and groups to achieve a specific goal.

## Module description

### chread
“chread” is a collective name for modules used to read information and manage channels and groups of different instant messengers. Each supported messenger has its own separate module called `xxread.py`, where `xx` is a shortcut identifying the messenger.

#### tgread.py
A tool module for exploring and managing Telegram groups and channels. Provides a comprehensive set of tools for interacting with the Telegram API using the Telethon library. Features include:
* Downloading dialogues, channels and groups that the user is participating in.
* Downloading detailed information about channels and their users, with the optimalization of fetch time by adapting to the flood wait limitations without exceeding them.
* Handling messages, including downloading and filtering by content.

### chdata
“chdata” is a collective name for modules used to transform data from instant messengers into a data frame of type `pandas.DataFrame`. Each supported messenger has its own separate module called `xxdata.py`, where `xx` is a shortcut identifying the messenger.

#### tgdata.py
A utility module for extracting data from Telegram groups and channels and saving it in `pandas.DataFrame` data frames. Features include:
* Downloading dialogs, channels and groups in which the user participates, as well as detailed information about channels and their participants.
* Managing asynchronous downloading of large amounts of data.
* Converting data to pandas data frames and performing necessary data wrangling and cleaning.

### chupdate.py
A program for updating a local SQLite database asynchronously with the data fetched from messaging services. Features include:
* Fetching channel, user and message data by utilizing “chdata” modules.
* Upserting appropriate tables of the database, ensuring proper deduplication and conflict resolution.
* Supporting configurable fetch depth enabling operations on partial data for avoiding flood wait or making it shorter.

#### Usage
`chupdate.py [-h] [-c {none,basic,full}] [-u {none,basic,full}] [-m {none,new,all}] [-l LIMIT_MESSAGES]`

##### Options
* `-h, --help` – show help message and exit
* `-c, --channels {none,basic,full}` – Determine the kind of channel data to fetch: `none` – do not update the channel table, `basic` – update the channel table with data from the basic columns only, `full` – update the channel table with data from all the columns. The default option value is `full`.
* `-u, --users {none,basic,full}` – Determine the kind of user data to fetch: `none` – do not update the user table, `basic` – update the user table with data from the basic columns only, `full` – update the user table with data from all the columns. The default option value is `basic`.
* `-m, --messages {none,new,all}` – Determine the kind of message data to fetch: `none` – do not update the message table, `new` – update the message table by fetching only new (unsaved so far) messages and those of the saved messages which have been edited, `all` – update the message table by fetching all messages or the number of messages from each channel determined by the option `-l`, `--limit_messages` if present. The default option value is `new`.
* `-l, --limit_messages LIMIT_MESSAGES` – Determine the limit of message number to be fetched for each channel if `-m`, `--messages` option value is `all`.

#### Database description
The program's database is stored in SQLite format in the `chexplore.db` file. The database consists of the following tables, which collect data about channels/groups, their users, and the messages they contain:
* `channel` – information of all channels and groups observed by the program.
* `channel_session` – session-specific information of channels and groups; a session is a connection with a given messaging service API authorized for a particular account that the program has credentials for.
* `user` – information of users of the channels or groups that the program observes.
* `user_session` – session-specific information of the users.
* `user_channel` – information relating the observed channels/groups with their users.
* `message` – information of messages from the channels or groups the program observes.

One row of a table represents one of the channel/user/message. The `chupdate.py` module is responsible for managing saving, updating, and maintaining order in the tables. Each table defines so-called key columns, which are columns in which any value changes are archived—rows with current values are marked with a `True` flag in the active column, while rows with outdated values have a `False` or `None` flag in that column. Appropriate conditions imposed on the tables and ordering commands ensure that no two rows have the same values in the key columns.
