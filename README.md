# Channel Explorer

## Description
Channel Explorer is a program used to retrieve information related to channels and groups that the user-controlled accounts participate in within selected instant messaging services, as well as to interact with other users from the userbot level as a chatbot. Moreover, the program creates and manages a database of channels and groups, their users and messages sent in them. The project utilizes libraries for communication with the application programming interface (API) of instant messaging applications (e.g., Telegram) and large language models (e.g., GPT). This program consists of several separate modules, each carrying out specific purposes.

The project can be useful for a broad range of purposes in which immiediate contact with a target group of messaging service users is required. This includes, but is not limited to, customer service, community management, satisfaction surveys, marketing, etc.

The currently supported instant messaging service is Telegram but more services are going to be added in the future.

## General plan of the program operation
1. Updating the database of all observed channels and groups.
2. (In preparation) Updating the database of all users of these channels and groups.
3. (In preparation) Updating the database of conversations from these channels and groups.
4. (In preparation) Updating the database of highlighted messages that require action – as defined by the user (e.g., complaints, questions, doubts, etc.)
5. (In preparation) Using the large language model (LLM) to talk to users of channels and groups to achieve a specific goal.
6. (In preparation) Signing up to new channels and groups via received invitation links obtained in points 4 and 5.

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
