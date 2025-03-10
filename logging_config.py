import logging
from logging.handlers import RotatingFileHandler

# Configuration of logging
formatter = logging.Formatter(
		'[%(levelname)-8s %(asctime)s] %(name)s: %(message)s')
rotating_handler = RotatingFileHandler(
		filename='chexplore.log',
		mode='a',
		maxBytes=1024*1024,
		backupCount=8)
rotating_handler.setFormatter(formatter)

# Configuration of root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(rotating_handler)

# Configuration of Telethon logger
telethon_logger = logging.getLogger('telethon')
telethon_logger.setLevel(logging.INFO) 

# Configuration of asyncio logger
asyncio_logger = logging.getLogger('asyncio')
asyncio_logger.setLevel(logging.INFO)
