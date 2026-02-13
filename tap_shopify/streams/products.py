import shopify
from tap_shopify.streams.base import (Stream, shopify_error_handling)
from tap_shopify.context import Context
import json
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import singer
from singer.utils import strftime
from tap_shopify.streams.compatibility.product_compatibility import ProductCompatibility
from tap_shopify.streams.compatibility.metafield_compatibility import MetafieldCompatibility
from tap_shopify.streams.compatibility.product_category_compatibility import ProductCategoryCompatibility
from tap_shopify.graph_ql import GraphQL


LOGGER = singer.get_logger()

class Products(Stream):
    name = 'products'
    replication_object = shopify.Product

    # Reusable GraphQL fragments for product/variant queries
    _PRODUCT_NODE_FIELDS = """
                    status
                    publishedAt
                    createdAt
                    vendor
                    updatedAt
                    descriptionHtml
                    productType
                    tags
                    handle
                    templateSuffix
                    title
                    id
                    options {
                        id
                        name
                        position
                        values
                    }
                    images(first: 250) {
                        nodes {
                            id
                            altText
                            src
                            height
                            width
                        }
                    }
                    """
    _INVENTORY_ITEM_BASE = """
                                id
                                requiresShipping
                                tracked
                                measurement {
                                    weight {
                                        unit
                                        value
                                    }
                                }
                            """
    _INVENTORY_ITEM_WITH_FULFILLMENT = """
                                id
                                requiresShipping
                                tracked
                                inventoryLevels(first: 1) {
                                    nodes {
                                        location {
                                            fulfillmentService {
                                                handle
                                            }
                                        }
                                    }
                                }
                                measurement {
                                    weight {
                                        unit
                                        value
                                    }
                                }
                            """
    _VARIANT_NODES_BASE = """
                            id
                            title
                            sku
                            position
                            price
                            compareAtPrice
                            inventoryPolicy
                            inventoryQuantity
                            taxable
                            taxCode
                            updatedAt
                            image {
                                id
                            }
                            inventoryItem {""" + _INVENTORY_ITEM_BASE + """
                            }
                            createdAt
                            barcode
                            selectedOptions {
                                name
                                value
                            }
                            presentmentPrices (first: 30) {
                                nodes {
                                    compareAtPrice {
                                        amount
                                        currencyCode
                                    }
                                    price {
                                        amount
                                        currencyCode
                                    }
                                }
                            }
                        """
    _VARIANT_NODES_WITH_FULFILLMENT = """
                            id
                            title
                            sku
                            position
                            price
                            compareAtPrice
                            inventoryPolicy
                            inventoryQuantity
                            taxable
                            taxCode
                            updatedAt
                            image {
                                id
                            }
                            inventoryItem {""" + _INVENTORY_ITEM_WITH_FULFILLMENT + """
                            }
                            createdAt
                            barcode
                            selectedOptions {
                                name
                                value
                            }
                            presentmentPrices (first: 30) {
                                nodes {
                                    compareAtPrice {
                                        amount
                                        currencyCode
                                    }
                                    price {
                                        amount
                                        currencyCode
                                    }
                                }
                            }
                        """
    _VARIANTS_PAGE_INFO = """
                        }
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                    """

    products_gql_query = (
        """
        query GetProducts($query: String, $cursor: String) {
            products(first: 50, after: $cursor, query: $query, sortKey: UPDATED_AT) {
                nodes {
        """
        + _PRODUCT_NODE_FIELDS
        + """
                    variants(first: 10, sortKey: ID) {
                        nodes {
        """
        + _VARIANT_NODES_BASE
        + _VARIANTS_PAGE_INFO
        + """
                }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    """
    )

    product_variants_gql_query = (
        """
        query GetProductVariants($id: ID!, $variantsCursor: String) {
            product(id: $id) {
                variants(first: 150, after: $variantsCursor, sortKey: ID) {
                    nodes {
        """
        + _VARIANT_NODES_BASE
        + _VARIANTS_PAGE_INFO
        + """
                }
            }
        }
    """
    )

    product_variants_gql_query_with_fulfillment_service = (
        """
        query GetProductVariants($id: ID!, $variantsCursor: String) {
            product(id: $id) {
                variants(first: 100, after: $variantsCursor, sortKey: ID) {
                    nodes {
        """
        + _VARIANT_NODES_WITH_FULFILLMENT
        + _VARIANTS_PAGE_INFO
        + """
                }
            }
        }
    """
    )

    products_gql_query_with_fulfillment_service = (
        """
        query GetProducts($query: String, $cursor: String) {
            products(first: 20, after: $cursor, query: $query) {
                nodes {
        """
        + _PRODUCT_NODE_FIELDS
        + """
                    variants(first: 10, sortKey: ID) {
                        nodes {
        """
        + _VARIANT_NODES_WITH_FULFILLMENT
        + _VARIANTS_PAGE_INFO
        + """
                }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    """
    )

    products_category_gql_query = """
        query GetProducts($query: String, $cursor: String) {
            products(first: 250, after: $cursor, query: $query) {
                nodes {
                    id,
                    productType,
                    createdAt,
                    productCategory{
                        productTaxonomyNode{
                            id,
                            fullName,
                            isLeaf,
                            isRoot

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

    products_metafields_gql_query = """
        query GetProducts($query: String, $cursor: String, $metafields_cursor: String) {
            products(first: 250, after: $cursor, query: $query) {
                nodes {
                    id
                    metafields(first: 175, after: $metafields_cursor) {
                        nodes {
                            id
                            namespace
                            key
                            value
                            description
                            createdAt
                            updatedAt
                            ownerType
                        }
                        pageInfo {
                            endCursor
                            hasNextPage
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

    product_metafields_gql_query = """
        query GetProduct($id: ID!, $cursor: String) {
            product(id: $id) {
                metafields(first: 175, after: $cursor) {
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                    nodes {
                        id
                        namespace
                        key
                        value
                        description
                        createdAt
                        updatedAt
                        ownerType
                    }
                }
            }
        }
    """

    access_scopes_query = """
        query CheckAppAccessScopes {
        appInstallation {
            accessScopes {
            handle
            }
        }
        }
    """

    def __init__(self):
        super().__init__()
        if Context.config.get("use_created_at_replication_key_for_products") is True:
            self.replication_key = "created_at"
        else:
            self.replication_key = "updated_at"

    @shopify_error_handling
    def _call_api(self, query, variables):
        """
        Generalized method for making API calls with a GraphQL client.

        Args:
            query: The GraphQL query to execute
            variables: Variables to pass to the query
        """
        shopify.ShopifyResource.activate_session(Context.shopify_graphql_session)
        gql_client = GraphQL()
        response = gql_client.execute(query, variables)
        result = json.loads(response)
        shopify.ShopifyResource.activate_session(Context.shopify_rest_session)
        if result.get("errors"):
            raise Exception(result['errors'])
        return result

    def get_access_scopes(self):
        response = self._call_api(self.access_scopes_query, {})
        scope_dict = {}
        for scope in response['data']['appInstallation']['accessScopes']:
            scope_dict[scope['handle']] = True
        return scope_dict

    def get_products_metafields(self, updated_at_min, updated_at_max, cursor=None, metafields_cursor=None):
        query = f"{self.replication_key}:>'{updated_at_min.isoformat()}' AND {self.replication_key}:<'{updated_at_max.isoformat()}'"
        variables = {
            "query": query,
            "cursor": cursor,
            "metafields_cursor": metafields_cursor
        }
        return self._call_api(self.products_metafields_gql_query, variables)

    def get_product_metafields(self, product_id, cursor=None):
        variables = {
            "id": product_id,
            "cursor": cursor
        }
        return self._call_api(self.product_metafields_gql_query, variables)

    def get_products_category(self, updated_at_min, updated_at_max, cursor=None):
        query = f"{self.replication_key}:>'{updated_at_min.isoformat()}' AND {self.replication_key}:<'{updated_at_max.isoformat()}'"
        variables = {
            "query": query,
            "cursor": cursor
        }
        return self._call_api(self.products_category_gql_query, variables)

    def get_products(self, updated_at_min, updated_at_max, cursor=None):
        query = f"{self.replication_key}:>'{updated_at_min.isoformat()}' AND {self.replication_key}:<'{updated_at_max.isoformat()}'"
        variables = {
            "query": query,
            "cursor": cursor
        }
        if self.has_access_scope('read_locations'):
            return self._call_api(self.products_gql_query_with_fulfillment_service, variables)
        else:
            return self._call_api(self.products_gql_query, variables)

    def _get_graphql_context(self):
        """Return (endpoint, headers) for thread-safe GraphQL calls without global session."""
        shopify.ShopifyResource.activate_session(Context.shopify_graphql_session)
        endpoint = shopify.ShopifyResource.get_site() + "/graphql.json"
        headers = dict(shopify.ShopifyResource.get_headers())
        shopify.ShopifyResource.activate_session(Context.shopify_rest_session)
        headers.setdefault("Accept", "application/json")
        headers.setdefault("Content-Type", "application/json")
        return (endpoint, headers)

    @shopify_error_handling
    def _call_graphql_direct(self, endpoint, headers, query, variables, max_throttle_retries=5):
        """Execute GraphQL with pre-fetched (endpoint, headers). Uses GraphQL.execute_with_context + THROTTLED retry."""
        for attempt in range(max_throttle_retries + 1):
            response_str = GraphQL.execute_with_context(endpoint, headers, query, variables)
            result = json.loads(response_str)
            if not result.get("errors"):
                return result
            err = result["errors"][0]
            if err.get("extensions", {}).get("code") == "THROTTLED" and attempt < max_throttle_retries:
                LOGGER.info("GraphQL throttled, waiting 2s before retry (%s/%s)", attempt + 1, max_throttle_retries)
                time.sleep(2)
                continue
            raise Exception(result["errors"])

    def get_product_variants(self, product_id, variants_cursor=None, graphql_context=None):
        variables = {
            "id": product_id,
            "variantsCursor": variants_cursor
        }
        query = (
            self.product_variants_gql_query_with_fulfillment_service
            if self.has_access_scope("read_locations")
            else self.product_variants_gql_query
        )
        if graphql_context is not None:
            endpoint, headers = graphql_context
            return self._call_graphql_direct(endpoint, headers, query, variables)
        return self._call_api(query, variables)

    def paginate(self, fetch_page, updated_at_min, updated_at_max, process_item, item_type):
        """
        Generic pagination logic for fetching and processing paginated results.

        :param fetch_page: A function that takes (updated_at_min, updated_at_max, cursor)
                           and returns a page of results.
        :param updated_at_min: The minimum update time for the query.
        :param updated_at_max: The maximum update time for the query.
        :param process_item: A function to process individual items in the result.
        :param item_type: A string used in the log message.
        :returns: A generator that yields processed items.
        """
        cursor = None
        page_count = 0

        while True:
            log_message = f"Fetching {item_type} {self.replication_key.split('_')[0]} between {updated_at_min} and {updated_at_max}, page {page_count}"
            if cursor:
                log_message += f" with cursor {cursor}"
            LOGGER.info(log_message)

            page = fetch_page(updated_at_min, updated_at_max, cursor)
            items = page['data']['products']['nodes']
            page_info = page['data']['products']['pageInfo']

            for item in items:
                yield from process_item(item)  # Use `yield from` to handle nested generators

            if page_info['hasNextPage']:
                cursor = page_info['endCursor']
                page_count += 1
            else:
                break

    def get_objects_with_metafields(self):
        def process_product(product):
            metafields_cursor = None
            metafields = product['metafields']['nodes']
            metafields_page_info = product['metafields']['pageInfo']
            metafields_page_count = 0

            while True:
                for metafield in metafields:
                    yield MetafieldCompatibility(metafield)

                if metafields_page_info['hasNextPage']:
                    metafields_cursor = metafields_page_info['endCursor']
                    metafields_page_count += 1
                    LOGGER.info(f"Product {product['id']} has additional metafields, fetching page {metafields_page_count} with cursor {metafields_cursor}")
                    metafields_page = self.get_product_metafields(product['id'], metafields_cursor)
                    metafields = metafields_page['data']['product']['metafields']['nodes']
                    metafields_page_info = metafields_page['data']['product']['metafields']['pageInfo']
                else:
                    break

        updated_at_min = self.get_bookmark()
        stop_time = singer.utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", 1))

        while updated_at_min < stop_time:
            updated_at_max = min(updated_at_min + timedelta(days=date_window_size), stop_time)

            yield from self.paginate(
                fetch_page=self.get_products_metafields,
                updated_at_min=updated_at_min,
                updated_at_max=updated_at_max,
                process_item=process_product,
                item_type="products_metafields"
            )
            updated_at_min = updated_at_max
            self.update_bookmark(strftime(updated_at_min))

    def get_objects_with_categories(self):
        def process_product(product):
            if product.get('productCategory'):
                yield ProductCategoryCompatibility(product).to_dict()

        updated_at_min = self.get_bookmark()
        stop_time = singer.utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", 1))

        while updated_at_min < stop_time:
            updated_at_max = min(updated_at_min + timedelta(days=date_window_size), stop_time)

            yield from self.paginate(
                fetch_page=self.get_products_category,
                updated_at_min=updated_at_min,
                updated_at_max=updated_at_max,
                process_item=process_product,
                item_type="product_categories"
            )
            updated_at_min = updated_at_max
            self.update_bookmark(strftime(updated_at_min))

    def _has_next_page_variants(self, product):
        return product.get("variants", {}).get("pageInfo", {}).get("hasNextPage", False)

    def _expand_product_variants(self, product, graphql_context=None):
        """Fetch all variant pages for one product; returns product with variants expanded (same format)."""
        variant_nodes = list(product.get("variants", {}).get("nodes", []))
        has_next_page = self._has_next_page_variants(product)
        variants_page_count = 0
        while has_next_page:
            variants_cursor = product["variants"]["pageInfo"]["endCursor"]
            variants_page_count += 1
            LOGGER.info(
                "Product %s has additional variants, fetching page %s with cursor %s",
                product["id"],
                variants_page_count,
                variants_cursor,
            )
            variants_page = self.get_product_variants(
                product["id"], variants_cursor, graphql_context=graphql_context
            )
            product_variants = variants_page["data"]["product"]["variants"]
            variant_nodes.extend(product_variants.get("nodes", []))
            product["variants"] = product_variants
            has_next_page = self._has_next_page_variants(product)
        product["variants"] = {"nodes": variant_nodes}
        return product

    def get_objects(self):
        # read_locations is a new requirement as of GraphQL API v2024-07 to fetch the "fulfillment_service" key for a variant.
        # This logic of fetching scopes is used to support existing tenants who may not have been granted the read_locations scope.
        # If the read_locations scope is not granted, we will log a warning message, and continue the sync without pulling this field.
        self.access_scopes = self.get_access_scopes()
        if not self.has_access_scope('read_locations'):
            LOGGER.warning("The `read_locations` access scope is not granted. The `fulfillment_service` field will not be available for product variants")

        max_workers = int(Context.config.get("variant_fetch_workers", 8))
        updated_at_min = self.get_bookmark()
        stop_time = singer.utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", 1))

        while updated_at_min < stop_time:
            updated_at_max = min(updated_at_min + timedelta(days=date_window_size), stop_time)
            cursor = None
            page_count = 0

            while True:
                log_message = f"Fetching products updated between {updated_at_min} and {updated_at_max}, page {page_count}"
                if cursor:
                    log_message += f" with cursor {cursor}"
                LOGGER.info(log_message)

                page = self.get_products(updated_at_min, updated_at_max, cursor)
                items = page["data"]["products"]["nodes"]
                page_info = page["data"]["products"]["pageInfo"]

                if not items:
                    if page_info["hasNextPage"]:
                        cursor = page_info["endCursor"]
                        page_count += 1
                        continue
                    break

                graphql_context = self._get_graphql_context()
                if max_workers <= 1:
                    for product in items:
                        expanded = self._expand_product_variants(product, graphql_context)
                        yield ProductCompatibility(expanded)
                else:
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = {
                            executor.submit(self._expand_product_variants, p, graphql_context): i
                            for i, p in enumerate(items)
                        }
                        results = [None] * len(items)
                        for future in as_completed(futures):
                            idx = futures[future]
                            results[idx] = future.result()
                        for expanded in results:
                            yield ProductCompatibility(expanded)

                if page_info["hasNextPage"]:
                    cursor = page_info["endCursor"]
                    page_count += 1
                else:
                    break

            updated_at_min = updated_at_max
            self.update_bookmark(strftime(updated_at_min))

    def has_access_scope(self, scope):
        return self.access_scopes.get(scope, False)

Context.stream_objects['products'] = Products
