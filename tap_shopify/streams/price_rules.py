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
                  appliesOncePerCustomer
                }
                ... on DiscountCodeBasic {
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
                ... on DiscountAutomaticBasic {
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
                  appliesOncePerCustomer
                }
                ... on DiscountAutomaticBxgy {
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
                  appliesOncePerCustomer
                }
                ... on DiscountAutomaticFreeShipping {
                  title
                  status
                  startsAt
                  endsAt
                  createdAt
                  updatedAt
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
        typename = discount.get('__typename')
        admin_gid = node.get('id')
        # Derive numeric id from GID if possible
        numeric_id = None
        if isinstance(admin_gid, str):
            last = admin_gid.rsplit('/', 1)[-1]
            if last.isdigit():
                try:
                    numeric_id = int(last)
                except Exception:
                    numeric_id = None

        # Prefer updatedAt if present for replication
        updated_at = discount.get('updatedAt') or discount.get('endsAt') or discount.get('startsAt') or discount.get('createdAt')

        # Start with legacy-shaped record including requested defaults where not inferable
        record = {
            'id': numeric_id if numeric_id is not None else admin_gid,
            'title': discount.get('title'),
            'created_at': discount.get('createdAt'),
            'updated_at': updated_at,
            'starts_at': discount.get('startsAt'),
            'ends_at': discount.get('endsAt'),
            'usage_limit': discount.get('usageLimit') or discount.get('usesPerOrderLimit') or None,
            'customer_selection': 'all',
            'target_selection': 'all',
            'allocation_method': 'each',
            'allocation_limit': None,
            'once_per_customer': bool(discount.get('appliesOncePerCustomer')) if discount.get('appliesOncePerCustomer') is not None else False,
            'entitled_product_ids': [],
            'entitled_variant_ids': [],
            'entitled_collection_ids': [],
            'entitled_country_ids': [],
            'prerequisite_customer_ids': [],
            'prerequisite_quantity_range': None,
            'prerequisite_shipping_price_range': None,
        }
        # Conditionally include usage_limit if present
        usage_limit = discount.get('usageLimit') or discount.get('usesPerOrderLimit')
        if usage_limit is not None:
            record['usage_limit'] = usage_limit
        # Only infer additional fields we can know from the current selection
        if typename in ('DiscountCodeFreeShipping', 'DiscountAutomaticFreeShipping'):
            record['value_type'] = 'percentage'
            record['value'] = '-100.0'
            record['target_type'] = 'shipping_line'

        # Infer basic/line-item discounts (percentage only if available)
        if typename in ('DiscountCodeBasic', 'DiscountAutomaticBasic'):
            record['target_type'] = 'line_item'
            # 2024-10 public schema doesn’t expose nested value breakdown reliably; skip unless present
            cg = (discount.get('customerGets') or {}).get('value') if isinstance(discount.get('customerGets'), dict) else None
            if isinstance(cg, dict) and cg.get('percentage') is not None:
                record['value_type'] = 'percentage'
                record['value'] = f"-{float(cg.get('percentage'))}"

        # Infer BxGy as a 100% off of Y items to keep parity
        if typename in ('DiscountCodeBxgy', 'DiscountAutomaticBxgy'):
            record['value_type'] = 'percentage'
            record['value'] = '-100.0'
            record['target_type'] = 'line_item'

        # once_per_customer for code-based types when available
        if typename in ('DiscountCodeApp', 'DiscountCodeBxgy', 'DiscountCodeFreeShipping') and discount.get('appliesOncePerCustomer') is not None:
            record['once_per_customer'] = discount.get('appliesOncePerCustomer')

        # Skip entitlements/minimums — public 2024-10 schema in this shop doesn't expose those types

        return record

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
