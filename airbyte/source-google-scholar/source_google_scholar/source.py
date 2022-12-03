#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import NoAuth


# Basic full refresh stream
class GoogleScholar(HttpStream, ABC):
    url_base = "https://serpapi.com/search"
    primary_key = None

    def __init__(
            self,
            config: Mapping[str, Any],
            **kwargs
    ):
        super().__init__()
        self.engine = config['engine']
        self.q = config['q']
        self.as_ylo = config['as_ylo']
        self.scisbd = config['scisbd']
        self.hl = config['hl']
        self.num = config['num']
        self.api_key = config['api_key']

    def next_page_token(
            self,
            response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        return None

    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return ""

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            'engine': self.engine,
            'q': self.q,
            'as_ylo': self.as_ylo,
            'scisbd': self.scisbd,
            'hl': self.hl,
            'num': self.num,
            'api_key': self.api_key
        }

    # def request_headers(
    #         self,
    #         stream_state: Mapping[str, Any],
    #         stream_slice: Mapping[str, Any] = None,
    #         next_page_token: Mapping[str, Any] = None,
    # ) -> MutableMapping[str, Any]:
    #     return None

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        return [response.json()]


# Source
class SourceGoogleScholar(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = NoAuth()  # Oauth2Authenticator is also available if you need oauth support
        return [GoogleScholar(authenticator=auth, config=config)]
