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
from tap_shopify.graph_ql import GraphQL

LOGGER = singer.get_logger()

def get_value_none_is_empty_default(data, key, default={}):
    if isinstance(data, dict) and data.get(key, default) is not None:
        return data.get(key, default)
    return default

class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout

class MarketPrices(Stream):
    name = 'market_prices'
    replication_method = 'FULL_TABLE'
    replication_key = None
    key_properties = ['price_list_id', "variant_id"]
    
    market_prices_query = """
        query GetMarketPrices($marketId: ID!, $first: Int, $after: String) {
            market(id: $marketId)    {
                id
                priceList {
                    currency
                    id
                    name
                    prices(first: $first, after: $after) {
                        nodes {
                            price {
                                amount
                                currencyCode
                            }
                            variant {
                                id
                                sku
                                title
                                product {
                                    id
                                    title
                                    handle
                                }
                            }
                        }
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                    }
                }
            }
        }
    """
    
    @shopify_error_handling
    def get_market_price_lists(self, market_id, cursor=None):
        """ 
        Fetch prices list for a specific market with pagination support.
        
        Args:
            market_id: The market ID (can be in format "gid://shopify/Market/123" or just the ID)
            cursor: Optional cursor for pagination
        """
        # Ensure market_id is in the correct format
        if not market_id.startswith("gid://shopify/Market/"):
            market_id = f"gid://shopify/Market/{market_id}"
        
        variables = {
            "marketId": market_id,
            "first": 100,
            "after": cursor
        }
        
        gql_client = GraphQL()
        with HiddenPrints():
            response = gql_client.execute(self.market_prices_query, variables)
        
        result = json.loads(response)
        
        if result.get("errors"):
            error_messages = [error.get("message", str(error)) for error in result.get("errors", [])]
            LOGGER.error(f"Error fetching prices for market {market_id}: {', '.join(error_messages)}")
            raise Exception(f"GraphQL errors: {', '.join(error_messages)}")
        
        return result
    
    def get_objects(self):
        """
        Iterate through all markets and fetch their prices.
        """
        selected_parent = Context.stream_objects['markets']()
        selected_parent.name = "markets"

        for market in selected_parent.get_objects():
            market_id = market.get("id")
            if not market_id:
                LOGGER.warning(f"Market object missing ID: {market}")
                continue
            
            # Fetch prices for this market  
            cursor = None
            page_count = 0
            
            while True:
                response = self.get_market_price_lists(market_id, cursor)

                market_data = get_value_none_is_empty_default(response, "data").get("market", {})
                
                price_list = get_value_none_is_empty_default(market_data, "priceList")

                if not price_list:
                    LOGGER.warning(f"Market {market_id} has no price list")
                
                prices = get_value_none_is_empty_default(price_list, "prices")
                price_nodes = get_value_none_is_empty_default(prices, "nodes", [])
                page_info = get_value_none_is_empty_default(prices, "pageInfo")
                
                # Yield each price record with market context
                for price_node in price_nodes:
                    record = {
                        "market_id": market_id,
                        "market_name": market.get("name"),
                        "price_list_name": price_list.get("name"),
                        "price_list_id": price_list.get("id"),
                        "price_list_currency": price_list.get("currency"),
                        "variant_id": get_value_none_is_empty_default(price_node, "variant").get("id"),
                        "variant": price_node.get("variant", {}),
                        "price": price_node.get("price", {}),
                    }
                    yield record
                
                # Check if there are more pages
                if page_info.get("hasNextPage"):
                    cursor = page_info.get("endCursor")
                    page_count += 1
                    LOGGER.info(f"Fetching additional prices for market {market_id}, page {page_count}")
                else:
                    break
    
    def sync(self):
        bookmark = self.get_bookmark()
        self.max_bookmark = bookmark
        for incoming_item in self.get_objects():
            yield incoming_item
        self.update_bookmark(strftime(self.max_bookmark))

Context.stream_objects['market_prices'] = MarketPrices
