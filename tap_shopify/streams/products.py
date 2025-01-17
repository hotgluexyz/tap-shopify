import shopify
from tap_shopify.streams.base import (Stream, shopify_error_handling)
from tap_shopify.context import Context
import json
from datetime import timedelta
import singer
from singer.utils import strftime
from tap_shopify.streams.compatibility.product_compatibility import ProductCompatibility

LOGGER = singer.get_logger()

class Products(Stream):
    name = 'products'
    replication_object = shopify.Product

    gql_query = """
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

    @shopify_error_handling
    def call_api_for_products(self, gql_client, query, cursor=None):
        variables = {
            "query": query,
            "cursor": cursor
        }
        response = gql_client.execute(self.gql_query, variables)
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

            LOGGER.info(f"Fetching products updated between {updated_at_min} and {updated_at_max}")
            cursor = None

            while True:
                page = self.get_products(updated_at_min, updated_at_max, cursor)
                products = page['data']['products']['nodes']
                page_info = page['data']['products']['pageInfo']

                for product in products:
                    yield ProductCompatibility(product)

                # Update the cursor and check if there's another page
                if page_info['hasNextPage']:
                    cursor = page_info['endCursor']
                else:
                    break

            # Update the bookmark for the next batch
            updated_at_min = updated_at_max
            self.update_bookmark(strftime(updated_at_min))

Context.stream_objects['products'] = Products
