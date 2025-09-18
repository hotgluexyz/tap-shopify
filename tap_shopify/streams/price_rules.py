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

                # ---- Amount/Percent off ----
                ... on DiscountCodeBasic {
                  title status startsAt endsAt usageLimit
                  appliesOncePerCustomer
                  customerGets {
                    items {
                      __typename
                      ... on AllDiscountItems { allItems }
                      ... on DiscountProducts {
                        products(first: 100) { nodes { id } }
                        productVariants(first: 100) { nodes { id } }
                      }
                      ... on DiscountCollections {
                        collections(first: 100) { nodes { id } }
                      }
                    }
                    value {
                      __typename
                      ... on DiscountPercentage { percentage }
                      ... on DiscountAmount { amount { amount currencyCode } appliesOnEachItem }
                    }
                  }
                  minimumRequirement {
                    __typename
                    ... on DiscountMinimumSubtotal { greaterThanOrEqualToSubtotal { amount currencyCode } }
                    ... on DiscountMinimumQuantity { greaterThanOrEqualToQuantity }
                  }
                }
                ... on DiscountAutomaticBasic {
                  title status startsAt endsAt
                  customerGets {
                    items {
                      __typename
                      ... on AllDiscountItems { allItems }
                      ... on DiscountProducts {
                        products(first: 100) { nodes { id } }
                        productVariants(first: 100) { nodes { id } }
                      }
                      ... on DiscountCollections {
                        collections(first: 100) { nodes { id } }
                      }
                    }
                    value {
                      __typename
                      ... on DiscountPercentage { percentage }
                      ... on DiscountAmount { amount { amount currencyCode } appliesOnEachItem }
                    }
                  }
                  minimumRequirement {
                    __typename
                    ... on DiscountMinimumSubtotal { greaterThanOrEqualToSubtotal { amount currencyCode } }
                    ... on DiscountMinimumQuantity { greaterThanOrEqualToQuantity }
                  }
                }

                # ---- Buy X Get Y ----
                ... on DiscountCodeBxgy {
                  title status startsAt endsAt usageLimit
                  appliesOncePerCustomer
                  customerBuys {
                    items {
                      __typename
                      ... on AllDiscountItems { allItems }
                      ... on DiscountProducts {
                        products(first: 100) { nodes { id } }
                        productVariants(first: 100) { nodes { id } }
                      }
                      ... on DiscountCollections {
                        collections(first: 100) { nodes { id } }
                      }
                    }
                    value {
                      __typename
                      ... on DiscountQuantity { quantity }
                      ... on DiscountPurchaseAmount { amount }
                    }
                  }
                  customerGets {
                    items {
                      __typename
                      ... on AllDiscountItems { allItems }
                      ... on DiscountProducts {
                        products(first: 100) { nodes { id } }
                        productVariants(first: 100) { nodes { id } }
                      }
                      ... on DiscountCollections {
                        collections(first: 100) { nodes { id } }
                      }
                    }
                    value {
                      __typename
                      ... on DiscountPercentage { percentage }
                      ... on DiscountAmount { amount { amount currencyCode } appliesOnEachItem }
                    }
                  }
                }
                ... on DiscountAutomaticBxgy {
                  title status startsAt endsAt
                  customerBuys {
                    items {
                      __typename
                      ... on AllDiscountItems { allItems }
                      ... on DiscountProducts {
                        products(first: 100) { nodes { id } }
                        productVariants(first: 100) { nodes { id } }
                      }
                      ... on DiscountCollections {
                        collections(first: 100) { nodes { id } }
                      }
                    }
                    value {
                      __typename
                      ... on DiscountQuantity { quantity }
                      ... on DiscountPurchaseAmount { amount }
                    }
                  }
                  customerGets {
                    items {
                      __typename
                      ... on AllDiscountItems { allItems }
                      ... on DiscountProducts {
                        products(first: 100) { nodes { id } }
                        productVariants(first: 100) { nodes { id } }
                      }
                      ... on DiscountCollections {
                        collections(first: 100) { nodes { id } }
                      }
                    }
                    value {
                      __typename
                      ... on DiscountPercentage { percentage }
                      ... on DiscountAmount { amount { amount currencyCode } appliesOnEachItem }
                    }
                  }
                }

                # ---- Free shipping ----
                ... on DiscountCodeFreeShipping {
                  title status startsAt endsAt usageLimit
                  appliesOncePerCustomer
                  minimumRequirement {
                    __typename
                    ... on DiscountMinimumSubtotal { greaterThanOrEqualToSubtotal { amount currencyCode } }
                    ... on DiscountMinimumQuantity { greaterThanOrEqualToQuantity }
                  }
                  destinationSelection {
                    __typename
                    ... on DiscountCountries { countries includeRestOfWorld }
                    ... on DiscountCountryAll { allCountries }
                  }
                }
                ... on DiscountAutomaticFreeShipping {
                  title status startsAt endsAt
                  minimumRequirement {
                    __typename
                    ... on DiscountMinimumSubtotal { greaterThanOrEqualToSubtotal { amount currencyCode } }
                    ... on DiscountMinimumQuantity { greaterThanOrEqualToQuantity }
                  }
                  destinationSelection {
                    __typename
                    ... on DiscountCountries { countries includeRestOfWorld }
                    ... on DiscountCountryAll { allCountries }
                  }
                }

                # ---- App/Function discounts ----
                ... on DiscountCodeApp {
                  title status startsAt endsAt usageLimit
                  appliesOncePerCustomer
                }
                ... on DiscountAutomaticApp {
                  title status startsAt endsAt
                }
              }
            }
            pageInfo { hasNextPage endCursor }
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

        # Start with only 1-1 inferable base fields
        record = {
            'id': numeric_id if numeric_id is not None else admin_gid,
            'title': discount.get('title'),
            'created_at': discount.get('createdAt'),
            'updated_at': updated_at,
            'starts_at': discount.get('startsAt'),
            'ends_at': discount.get('endsAt'),
        }
        usage_limit = discount.get('usageLimit') or discount.get('usesPerOrderLimit')
        if usage_limit is not None:
            record['usage_limit'] = usage_limit
        # Conditionally include usage_limit if present
        usage_limit = discount.get('usageLimit') or discount.get('usesPerOrderLimit')
        if usage_limit is not None:
            record['usage_limit'] = usage_limit
        # Only infer additional fields we can know from the current selection
        if typename in ('DiscountCodeFreeShipping', 'DiscountAutomaticFreeShipping'):
            record['value_type'] = 'percentage'
            record['value'] = '-100.0'
            record['target_type'] = 'shipping_line'

        # Infer basic/line-item discounts (percentage or fixed amount)
        if typename in ('DiscountCodeBasic', 'DiscountAutomaticBasic'):
            record['target_type'] = 'line_item'
            cg = (discount.get('customerGets') or {}).get('value') if isinstance(discount.get('customerGets'), dict) else None
            if isinstance(cg, dict) and cg.get('percentage') is not None:
                record['value_type'] = 'percentage'
                record['value'] = f"-{float(cg.get('percentage'))}"
            elif isinstance(cg, dict) and isinstance(cg.get('amount'), dict) and cg.get('amount').get('amount') is not None:
                amt = float(cg.get('amount').get('amount'))
                record['value_type'] = 'fixed_amount'
                record['value'] = f"-{amt}"
                # allocation_method hint for fixed amount
                if cg.get('appliesOnEachItem') is True:
                    record['allocation_method'] = 'each'
                else:
                    record['allocation_method'] = 'across'

        # Infer BxGy: percent/amount off Y if present; fallback to 100% when percent/amount is not provided
        if typename in ('DiscountCodeBxgy', 'DiscountAutomaticBxgy'):
            record['target_type'] = 'line_item'
            yv = (discount.get('customerGets') or {}).get('value') if isinstance(discount.get('customerGets'), dict) else None
            if isinstance(yv, dict) and yv.get('__typename') == 'DiscountPercentage' and yv.get('percentage') is not None:
                record['value_type'] = 'percentage'
                record['value'] = f"-{float(yv.get('percentage'))}"
            elif isinstance(yv, dict) and yv.get('__typename') == 'DiscountAmount' and isinstance(yv.get('amount'), dict) and yv.get('amount').get('amount') is not None:
                amt = float(yv.get('amount').get('amount'))
                record['value_type'] = 'fixed_amount'
                record['value'] = f"-{amt}"
                if yv.get('appliesOnEachItem') is True:
                    record['allocation_method'] = 'each'
                else:
                    record['allocation_method'] = 'across'
            else:
                # Fallback parity when not provided by API
                record['value_type'] = 'percentage'
                record['value'] = '-100.0'

        # once_per_customer for code-based types when available
        if typename in ('DiscountCodeApp', 'DiscountCodeBxgy', 'DiscountCodeFreeShipping') and discount.get('appliesOncePerCustomer') is not None:
            record['once_per_customer'] = discount.get('appliesOncePerCustomer')

        # Entitled items and target selection
        def parse_nodes(nodes):
            ids = []
            for n in nodes or []:
                gid = n.get('id')
                if gid and gid.rsplit('/', 1)[-1].isdigit():
                    ids.append(int(gid.rsplit('/', 1)[-1]))
            return ids

        def handle_items(items):
            if not items:
                return
            t = items.get('__typename')
            if t == 'AllDiscountItems' and items.get('allItems'):
                record['target_selection'] = 'all'
            else:
                # Only set 'entitled' if any entitlements present
                ent_products = parse_nodes(((items.get('products') or {}).get('nodes'))) if t == 'DiscountProducts' else []
                ent_variants = parse_nodes(((items.get('productVariants') or {}).get('nodes'))) if t == 'DiscountProducts' else []
                ent_collections = parse_nodes(((items.get('collections') or {}).get('nodes'))) if t == 'DiscountCollections' else []
                if ent_products:
                    record['entitled_product_ids'] = ent_products
                if ent_variants:
                    record['entitled_variant_ids'] = ent_variants
                if ent_collections:
                    record['entitled_collection_ids'] = ent_collections
                if ent_products or ent_variants or ent_collections:
                    record['target_selection'] = 'entitled'

        if typename in ('DiscountCodeBasic', 'DiscountAutomaticBasic'):
            handle_items(((discount.get('customerGets') or {}).get('items')))
        if typename in ('DiscountCodeBxgy', 'DiscountAutomaticBxgy'):
            handle_items(((discount.get('customerGets') or {}).get('items')))
            # customerBuys items are prerequisites on X side — not mapped to entitled_* legacy fields

        # Minimum requirements
        min_req = discount.get('minimumRequirement') if isinstance(discount.get('minimumRequirement'), dict) else None
        if min_req and min_req.get('__typename') == 'DiscountMinimumSubtotal':
            sub = (min_req.get('greaterThanOrEqualToSubtotal') or {})
            if sub.get('amount') is not None:
                record['prerequisite_subtotal_range'] = { 'greater_than_or_equal_to': str(sub.get('amount')) }
        elif min_req and min_req.get('__typename') == 'DiscountMinimumQuantity':
            qty = min_req.get('greaterThanOrEqualToQuantity')
            if qty is not None:
                record['prerequisite_quantity_range'] = { 'greater_than_or_equal_to': int(qty) }

        # Free shipping destination countries → add proxy codes
        dest = discount.get('destinationSelection') if isinstance(discount.get('destinationSelection'), dict) else None
        # Do not include entitled_country_ids (no 1-1 mapping to legacy IDs or schema types)

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
