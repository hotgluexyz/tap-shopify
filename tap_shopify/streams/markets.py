import copy
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
    replication_method = 'FULL_TABLE'
    replication_key = None
    results_per_page = 100
    gql_query = "query tapShopify($first: Int, $after: String) { markets(first: $first, after: $after) { edges { cursor node { currencySettings { baseCurrency { currencyCode currencyName enabled rateUpdatedAt } localCurrencies } enabled handle id name primary regions(first: 250) { edges { node { id name } } } webPresence { alternateLocales {locale marketWebPresences { id } name primary published } defaultLocale { locale marketWebPresences { id } name primary published } id rootUrls { locale url } subfolderSuffix } } }, pageInfo { hasNextPage } } }"
    @shopify_error_handling
    def call_api_for_incoming_items(self):
        gql_client = shopify.GraphQL()
        with HiddenPrints():
            response = gql_client.execute(self.gql_query, dict(first=self.results_per_page))
        return json.loads(response)

    @shopify_error_handling
    def get_region_details(self, market_region_countries_ids):
        region_query = """
        query getRegionDetails($ids: [ID!]!) {
            nodes(ids: $ids) {
                id
                ... on MarketRegionCountry {
                code
                name
                }
            }
        }
        """
        
        # Make API call
        gql_client = shopify.GraphQL()
        with HiddenPrints():
            response = gql_client.execute(region_query, {'ids': market_region_countries_ids})
        
        region_data = json.loads(response)
        
        # Extract relevant data from response
        if region_data.get('data', {}).get('nodes'):
            details = region_data['data']['nodes']
            return details
        
        LOGGER.warning(f"Could not fetch details for region ID: {market_region_countries_ids}")
        return None

    def get_regions(self, node):
        final_regions = []
        regions = node.get("regions")
        edges = regions.get("edges")
        market_region_countries_ids = []
        for edge in edges:
            edge_node = edge.get("node")
            market_region_countries_ids.append(edge_node.get("id"))
        new_node = copy.deepcopy(node)
        new_node["regions"] = self.get_region_details(market_region_countries_ids)
        return new_node

    def get_objects(self):
        incoming_item = self.call_api_for_incoming_items()
        data = incoming_item.get("data") if incoming_item else {}
        markets = data.get("markets") if data else {}
        edges = markets.get("edges") if markets else []
        if not edges:
            if incoming_item.get("errors"):
                message = ""
                errors_messages = incoming_item.get("errors", [])
                for index, error in enumerate(errors_messages):
                    message += f"{error.get('message')}"
                    if index < len(errors_messages) - 1:
                        message += "\n"
                LOGGER.error(message)
                raise Exception(message)
            LOGGER.warning("No data found in API response.")
            return
        for edge in edges:
            node = edge.get("node")
            node = self.get_regions(node)
            if node is not None:
                yield node
            else:
                LOGGER.warning("Edge without node found: %s... Ignoring it", edge)
                continue

    def sync(self):
        bookmark = self.get_bookmark()
        self.max_bookmark = bookmark
        for incoming_item in self.get_objects():
            yield incoming_item
        self.update_bookmark(strftime(self.max_bookmark))


Context.stream_objects['markets'] = Markets
