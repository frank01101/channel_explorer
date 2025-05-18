"""sgdata.py: A utility module for extracting data from Signal groups
and channels, and processing it into pandas' data frames.
"""

__author__ = 'Franciszek Humieja'
__copyright__ = 'Copyright (c) 2025 Franciszek Humieja'
__license__ = 'MIT'
__version__ = '0.1.0'

class SignalDataHandler():
    """A handler class for processing Signal data, converting it into
    pandas DataFrames.
    """

    service_name = 'signal'

    def __init__(self, session_name: str, api_id: int, api_hash: str):
        raise NotImplementedError(
                'Class SignalDataHandler is not implemented yet')
