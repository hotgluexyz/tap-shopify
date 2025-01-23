import shopify
from tap_shopify.streams.base import (Stream, shopify_error_handling)
from tap_shopify.context import Context
import json
from datetime import timedelta
import singer
from singer.utils import strftime
from tap_shopify.streams.compatibility.product_compatibility import ProductCompatibility
from tap_shopify.streams.compatibility.metafield_compatibility import MetafieldCompatibility

LOGGER = singer.get_logger()

class Products(Stream):
    name = 'products'
    replication_object = shopify.Product

    products_gql_query = """
        query GetProducts($query: String, $cursor: String) {
            products(first: 250, after: $cursor, query: $query) {
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
                            weight
                            weightUnit
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
                            }
                            createdAt
                            barcode
                            fulfillmentService {
                                handle
                            }
                            selectedOptions {
                                name
                                value
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

    # @shopify_error_handling
    def call_api_for_products_metafields(self, gql_client, query, cursor=None, metafields_cursor=None):
        variables = {
            "query": query,
            "cursor": cursor,
            "metafields_cursor": metafields_cursor
        }
        response = gql_client.execute(self.products_metafields_gql_query, variables)
        result = json.loads(response)
        if result.get("errors"):
            raise Exception(result['errors'])
        return result

    def get_products_metafields(self, updated_at_min, updated_at_max, cursor=None, metafields_cursor=None):
        gql_client = shopify.GraphQL()
        query = f"updated_at:>'{updated_at_min.isoformat()}' AND updated_at:<'{updated_at_max.isoformat()}'"
        page = self.call_api_for_products_metafields(gql_client, query, cursor, metafields_cursor)
        return page

    # @shopify_error_handling
    def get_product_metafields(self, product_id, cursor=None):
        gql_client = shopify.GraphQL()

        gql_query = """
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
        variables = {
            "id": product_id,
            "cursor": cursor
        }

        response = gql_client.execute(gql_query, variables)
        result = json.loads(response)
        if result.get("errors"):
            raise Exception(result['errors'])
        return result

    def get_objects_with_metafields(self):
        updated_at_min = self.get_bookmark()
        stop_time = singer.utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", 1))

        while updated_at_min < stop_time:
            updated_at_max = updated_at_min + timedelta(days=date_window_size)
            if updated_at_max > stop_time:
                updated_at_max = stop_time

            cursor = None

            page_count = 0
            while True:
                log_message = f"Fetching metafields for products updated between {updated_at_min} and {updated_at_max}, page {page_count}"
                if cursor:
                    log_message += f" with cursor {cursor}"
                LOGGER.info(log_message)

                page = self.get_products_metafields(updated_at_min, updated_at_max, cursor)
                products = page['data']['products']['nodes']
                page_info = page['data']['products']['pageInfo']

                for product in products:
                    metafields_cursor = None
                    metafields = product['metafields']['nodes']
                    metafields_page_info = product['metafields']['pageInfo']

                    metafields_page_count = 0
                    while True:
                        for metafield in metafields:
                            yield MetafieldCompatibility(metafield)

                        # Fetch the next page of metafields if available
                        if metafields_page_info['hasNextPage']:
                            metafields_page_count +=1
                            metafields_cursor = metafields_page_info['endCursor']
                            log_message = f"Product with id {product['id']} has additional metafields, fetching page {metafields_page_count}"
                            if cursor:
                                log_message += f" with cursor {cursor}"
                            LOGGER.info(log_message)
                            product_page = self.get_product_metafields(product["id"], metafields_cursor)
                            metafields = product_page['data']['product']['metafields']['nodes']
                            metafields_page_info = product_page['data']['product']['metafields']['pageInfo']
                        else:
                            break  # Exit metafields loop

                # Update the cursor for the next page of products
                if page_info['hasNextPage']:
                    cursor = page_info['endCursor']
                else:
                    break  # Exit products pagination loop

            # Update the bookmark for the next batch
            updated_at_min = updated_at_max
            self.update_bookmark(strftime(updated_at_min))


    # @shopify_error_handling
    def call_api_for_products(self, gql_client, query, cursor=None):
        variables = {
            "query": query,
            "cursor": cursor
        }
        response = gql_client.execute(self.products_gql_query, variables)
        result = json.loads(response)
        if result.get("errors"):
            raise Exception(result['errors'])
        return result

    def get_products(self, updated_at_min, updated_at_max, cursor=None):
        gql_client = shopify.GraphQL()
        query = f"updated_at:>'{updated_at_min.isoformat()}' AND updated_at:<'{updated_at_max.isoformat()}'"

        page = self.call_api_for_products(gql_client, query, cursor)
        return page


    def get_objects(self):
        updated_at_min = self.get_bookmark()
        stop_time = singer.utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", 1))

        while updated_at_min < stop_time:
            updated_at_max = updated_at_min + timedelta(days=date_window_size)
            if updated_at_max > stop_time:
                updated_at_max = stop_time

            cursor = None

            page_count = 0
            while True:
                log_message = f"Fetching products updated between {updated_at_min} and {updated_at_max}, page {page_count}"
                if cursor:
                    log_message += f" with cursor {cursor}"
                LOGGER.info(log_message)

                page = self.get_products(updated_at_min, updated_at_max, cursor)
                products = page['data']['products']['nodes']
                page_info = page['data']['products']['pageInfo']

                for product in products:
                    yield ProductCompatibility(product)

                # Update the cursor and check if there's another page
                if page_info['hasNextPage']:
                    cursor = page_info['endCursor']
                    page_count += 1
                else:
                    page_count = 0
                    break

            # Update the bookmark for the next batch
            updated_at_min = updated_at_max
            self.update_bookmark(strftime(updated_at_min))

Context.stream_objects['products'] = Products
