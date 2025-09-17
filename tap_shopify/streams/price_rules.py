import json
import shopify
import singer
from singer.utils import strftime, strptime_to_utc
from tap_shopify.streams.base import (Stream, shopify_error_handling)
from tap_shopify.context import Context


LOGGER = singer.get_logger()


class PriceRules(Stream):
    name = 'price_rules'

    # GraphQL: discountNodes replaces REST PriceRule in 2024-10
    discount_nodes_query = """
        query GetDiscountNodes($first: Int!, $after: String) {
          discountNodes(first: $first, after: $after) {
            nodes {
              id
              discount {
                __typename
                ... on DiscountCodeApp {
                  title
                  status
                  startsAt
                  endsAt
                  createdAt
                  updatedAt
                  usageLimit
                }
                ... on DiscountAutomaticApp {
                  title
                  status
                  startsAt
                  endsAt
                  createdAt
                  updatedAt
                }
                ... on DiscountCodeBxgy {
                  title
                  status
                  startsAt
                  endsAt
                  createdAt
                  updatedAt
                  usesPerOrderLimit
                }
                ... on DiscountCodeFreeShipping {
                  title
                  status
                  startsAt
                  endsAt
                  createdAt
                  updatedAt
                  usageLimit
                }
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
    """

    @shopify_error_handling
    def _call_graphql(self, query, variables):
        shopify.ShopifyResource.activate_session(Context.shopify_graphql_session)
        gql_client = shopify.GraphQL()
        response = gql_client.execute(query, variables)
        result = json.loads(response)
        shopify.ShopifyResource.activate_session(Context.shopify_rest_session)
        if result.get("errors"):
            raise Exception(result["errors"])
        return result

    def _map_discount_node_to_record(self, node):
        discount = node.get('discount') or {}
        # Prefer updatedAt if present for replication
        updated_at = discount.get('updatedAt') or discount.get('endsAt') or discount.get('startsAt') or discount.get('createdAt')
        record = {
            # Keep GraphQL ID; schema updated to accept string IDs
            'id': node.get('id'),
            'title': discount.get('title'),
            'created_at': discount.get('createdAt'),
            'updated_at': updated_at,
            'starts_at': discount.get('startsAt'),
            'ends_at': discount.get('endsAt'),
            'usage_limit': discount.get('usageLimit') or discount.get('usesPerOrderLimit'),
            # Fields below are REST PriceRule-specific; leave as None
            'value_type': None,
            'value': None,
            'customer_selection': None,
            'target_type': None,
            'target_selection': None,
            'allocation_method': None,
            'allocation_limit': None,
            'once_per_customer': None,
            'entitled_collection_ids': None,
            'entitled_country_ids': None,
            'entitled_product_ids': None,
            'entitled_variant_ids': None,
            'prerequisite_customer_ids': None,
            'prerequisite_quantity_range': None,
            'prerequisite_saved_search_ids': None,
            'prerequisite_shipping_price_range': None,
            'prerequisite_subtotal_range': None,
            'prerequisite_to_entitlement_purchase': None,
            'prerequisite_product_ids': None,
            'prerequisite_variant_ids': None,
            'prerequisite_collection_ids': None,
            'prerequisite_to_entitlement_quantity_ratio': None,
        }
        return {k: v for k, v in record.items() if v is not None}

    def get_objects(self):
        # We paginate through all discount nodes and filter by bookmark
        bookmark = self.get_bookmark()
        max_bookmark = bookmark
        cursor = None
        while True:
            page = self._call_graphql(self.discount_nodes_query, {
                'first': Context.get_results_per_page(100),
                'after': cursor
            })
            nodes = page['data']['discountNodes']['nodes']
            page_info = page['data']['discountNodes']['pageInfo']

            for node in nodes:
                record = self._map_discount_node_to_record(node)
                if not record.get('updated_at'):
                    continue
                replication_value = strptime_to_utc(record['updated_at'])
                if replication_value >= bookmark:
                    yield record
                if replication_value > max_bookmark:
                    max_bookmark = replication_value

            if page_info['hasNextPage']:
                cursor = page_info['endCursor']
            else:
                break

        # Update bookmark at the end of the window
        self.update_bookmark(strftime(max_bookmark))

    def sync(self):
        # Delegate to get_objects which already yields dictionaries
        yield from self.get_objects()


Context.stream_objects['price_rules'] = PriceRules
