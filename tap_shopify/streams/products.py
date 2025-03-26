import shopify
from tap_shopify.streams.base import (Stream, shopify_error_handling)
from tap_shopify.context import Context
import json
from datetime import timedelta
import singer
from singer.utils import strftime
from tap_shopify.streams.compatibility.product_compatibility import ProductCompatibility
from tap_shopify.streams.compatibility.metafield_compatibility import MetafieldCompatibility
from tap_shopify.streams.compatibility.product_category_compatibility import ProductCategoryCompatibility

LOGGER = singer.get_logger()

class Products(Stream):
    name = 'products'
    replication_object = shopify.Product

    def __init__(self):
        super().__init__()
        # read_locations is a new requirement as of GraphQL API v2024-07 to fetch the "fulfillment_service" key for a variant.
        # This logic of fetching scopes is used to support existing tenants who may not have been granted the read_locations scope.
        # If the read_locations scope is not granted, we will log a warning message, and continue the sync without pulling this field.
        self.access_scopes = self.get_access_scopes()
        if not self.has_access_scope('read_locations'):
            LOGGER.warning("The `read_locations` access scope is not granted. The `fulfillment_service` field will not be available for product variants")

    def has_access_scope(self, scope):
        return self.access_scopes.get(scope, False)

    products_gql_query = """
        query GetProducts($query: String, $cursor: String) {
            products(first: 20, after: $cursor, query: $query) {
                nodes {
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
                    variants(first: 100) {
                        nodes {
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
                            inventoryItem {
                                id
                                requiresShipping
                                tracked
                                measurement {
                                    weight {
                                        unit
                                        value
                                    }
                                }
                            }
                            createdAt
                            barcode
                            selectedOptions {
                                name
                                value
                            }
                            presentmentPrices (first: 30) {
                                nodes {
                                    price {
                                        amount
                                        currencyCode
                                    }
                                }
                            }
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

    products_gql_query_with_fulfillment_service = """
        query GetProducts($query: String, $cursor: String) {
            products(first: 20, after: $cursor, query: $query) {
                nodes {
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
                    variants(first: 100) {
                        nodes {
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
                            inventoryItem {
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
                            }
                            createdAt
                            barcode
                            selectedOptions {
                                name
                                value
                            }
                            presentmentPrices (first: 30) {
                                nodes {
                                    price {
                                        amount
                                        currencyCode
                                    }
                                }
                            }
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

    @shopify_error_handling
    def _call_api(self, query, variables):
        """
        Generalized method for making API calls with a GraphQL client.

        Args:
            query: The GraphQL query to execute
            variables: Variables to pass to the query
        """
        shopify.ShopifyResource.activate_session(Context.shopify_graphql_session)
        gql_client = shopify.GraphQL()
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
        query = f"updated_at:>'{updated_at_min.isoformat()}' AND updated_at:<'{updated_at_max.isoformat()}'"
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
        query = f"updated_at:>'{updated_at_min.isoformat()}' AND updated_at:<'{updated_at_max.isoformat()}'"
        variables = {
            "query": query,
            "cursor": cursor
        }
        return self._call_api(self.products_category_gql_query, variables)

    def get_products(self, updated_at_min, updated_at_max, cursor=None):
        query = f"updated_at:>'{updated_at_min.isoformat()}' AND updated_at:<'{updated_at_max.isoformat()}'"
        variables = {
            "query": query,
            "cursor": cursor
        }
        if self.has_access_scope('read_locations'):
            return self._call_api(self.products_gql_query_with_fulfillment_service, variables)
        else:
            return self._call_api(self.products_gql_query, variables)

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
            log_message = f"Fetching {item_type} updated between {updated_at_min} and {updated_at_max}, page {page_count}"
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

    def get_objects(self):
        def process_product(product):
            yield ProductCompatibility(product)

        updated_at_min = self.get_bookmark()
        stop_time = singer.utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", 1))

        while updated_at_min < stop_time:
            updated_at_max = min(updated_at_min + timedelta(days=date_window_size), stop_time)

            yield from self.paginate(
                fetch_page=self.get_products,
                updated_at_min=updated_at_min,
                updated_at_max=updated_at_max,
                process_item=process_product,
                item_type="products"
            )
            updated_at_min = updated_at_max
            self.update_bookmark(strftime(updated_at_min))

Context.stream_objects['products'] = Products
