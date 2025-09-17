import json
import shopify
import singer
from singer.utils import strftime, strptime_to_utc
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream, shopify_error_handling)

LOGGER = singer.get_logger()


class DiscountCodes(Stream):
    name = 'discount_codes'
    replication_key = 'created_at'

    # Fetch codes for a given discount node id across supported code-based types
    discount_codes_query = """
        query GetDiscountCodes($id: ID!, $first: Int!, $after: String) {
          discountNode(id: $id) {
            id
            discount {
              __typename
              ... on DiscountCodeApp {
                codes(first: $first, after: $after) {
                  nodes { code createdAt updatedAt disabledAt usageCount }
                  pageInfo { hasNextPage endCursor }
                }
              }
              ... on DiscountCodeBxgy {
                codes(first: $first, after: $after) {
                  nodes { code createdAt updatedAt disabledAt usageCount }
                  pageInfo { hasNextPage endCursor }
                }
              }
              ... on DiscountCodeFreeShipping {
                codes(first: $first, after: $after) {
                  nodes { code createdAt updatedAt disabledAt usageCount }
                  pageInfo { hasNextPage endCursor }
                }
              }
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

    def _extract_codes_edge(self, discount_payload):
        typename = discount_payload.get('__typename')
        if typename in ('DiscountCodeApp', 'DiscountCodeBxgy', 'DiscountCodeFreeShipping'):
            return discount_payload.get('codes') or {}
        return {}

    def _map_code_record(self, parent_id, code_node):
        created_at = code_node.get('createdAt')
        updated_at = code_node.get('updatedAt') or created_at
        # Create a stable synthetic id combining parent and code string
        synthetic_id = f"{parent_id}:{code_node.get('code')}"
        return {
            'id': synthetic_id,
            'code': code_node.get('code'),
            'created_at': created_at,
            'updated_at': updated_at,
            'disabled_at': code_node.get('disabledAt'),
            'usage_count': code_node.get('usageCount'),
            'parent_discount_id': parent_id,
        }

    def _get_codes_for_parent(self, parent_id):
        cursor = None
        while True:
            page = self._call_graphql(self.discount_codes_query, {
                'id': parent_id,
                'first': Context.get_results_per_page(100),
                'after': cursor
            })
            discount_node = page['data']['discountNode']
            if not discount_node or not discount_node.get('discount'):
                break
            codes_edge = self._extract_codes_edge(discount_node['discount'])
            nodes = (codes_edge.get('nodes') or []) if codes_edge else []
            page_info = (codes_edge.get('pageInfo') or {}) if codes_edge else {}

            for code in nodes:
                yield self._map_code_record(parent_id, code)

            if page_info.get('hasNextPage'):
                cursor = page_info.get('endCursor')
            else:
                break

    def get_objects(self):
        # Iterate discount nodes via the parent price_rules stream but scoped under a distinct bookmark name
        selected_parent = Context.stream_objects['price_rules']()
        selected_parent.name = "discount_code_price_rules"

        bookmark = self.get_bookmark()
        self.max_bookmark = bookmark

        for parent in selected_parent.get_objects():
            parent_id = parent['id'] if isinstance(parent, dict) else getattr(parent, 'id', None)
            if not parent_id:
                continue
            for code_record in self._get_codes_for_parent(parent_id):
                replication_value = strptime_to_utc(code_record['created_at'])
                if replication_value >= bookmark:
                    yield code_record
                if replication_value > self.max_bookmark:
                    self.max_bookmark = replication_value

    def sync(self):
        for rec in self.get_objects():
            yield rec
        self.update_bookmark(strftime(self.max_bookmark))


Context.stream_objects['discount_codes'] = DiscountCodes
