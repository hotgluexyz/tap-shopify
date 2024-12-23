from tap_shopify.context import Context
from tap_shopify.streams.base import Stream
import os
import sys
import shopify
import singer
import json
from singer.utils import strftime
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,shopify_error_handling)

LOGGER = singer.get_logger()


class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


class Markets(Stream):
    name = 'markets'
    replication_key = None
    results_per_page = 100
    gql_query = "query tapShopify($first: Int, $after: String) { markets(first: $first, after: $after) { edges { cursor node { currencySettings { baseCurrency { currencyCode currencyName enabled rateUpdatedAt } localCurrencies } enabled handle id name primary webPresence { alternateLocales defaultLocale id rootUrls { locale url } subfolderSuffix } } }, pageInfo { hasNextPage } } }"

    @shopify_error_handling
    def call_api_for_incoming_items(self):
        gql_client = shopify.GraphQL()
        with HiddenPrints():
            response = gql_client.execute(self.gql_query, dict(first=self.results_per_page))
        return json.loads(response)

    def get_objects(self):
        incoming_item = self.call_api_for_incoming_items()
        for edge in incoming_item["data"].get("markets", {}).get("edges", []):
            yield edge.get("node")

    def sync(self):
        bookmark = self.get_bookmark()
        self.max_bookmark = bookmark
        for incoming_item in self.get_objects():
            yield incoming_item
        self.update_bookmark(strftime(self.max_bookmark))


Context.stream_objects['markets'] = Markets
