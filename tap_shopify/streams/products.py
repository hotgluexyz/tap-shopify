import shopify
from tap_shopify.streams.base import (Stream, shopify_error_handling)
from tap_shopify.context import Context
import json
from datetime import timedelta
import singer
from singer.utils import strftime

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

    def convert_graphql_to_rest(self, graphql_product):
        """Convert a GraphQL product object to the REST API equivalent for compatibility."""
        product_id = int(graphql_product["id"].split("/")[-1])
        rest_product = {
            "body_html": graphql_product["descriptionHtml"],
            "created_at": graphql_product["createdAt"],
            "handle": graphql_product["handle"],
            "id": product_id,
            "image": None,  # No longer supported by GraphQL API
            "product_type": graphql_product["productType"],
            "published_at": graphql_product["publishedAt"],
            "published_scope": None,  # No longer supported by GraphQL API
            "status": graphql_product["status"],
            "tags": ", ".join(graphql_product["tags"]),
            "template_suffix": graphql_product["templateSuffix"],
            "title": graphql_product["title"],
            "updated_at": graphql_product["updatedAt"],
            "vendor": graphql_product["vendor"],
            "options": [
                {
                    "id": int(option["id"].split("/")[-1]),
                    "product_id": product_id,
                    "name": option["name"],
                    "position": option["position"],
                    "values": option["values"]
                }
                for option in graphql_product["options"]
            ],
            "images": [
                {
                    "id": int(image["id"].split("/")[-1]),
                    "position": idx + 1,
                    "created_at": None, # No longer supported by GraphQL API
                    "updated_at": None, # No longer suppported by GraphQL API
                    "width": image["width"],
                    "height": image["height"],
                    "src": image["src"],
                    "variant_ids": None  # No longer supported by GraphQL API
                }
                for idx, image in enumerate(graphql_product.get("images", {}).get("nodes", []))
            ],
            "variants": [
                {
                    "barcode": variant["barcode"],
                    "compare_at_price": variant["compareAtPrice"],
                    "created_at": variant["createdAt"],
                    "fulfillment_service": variant["fulfillmentService"]["handle"],
                    "grams": None, # No longer supported by GraphQL API
                    "id": int(variant["id"].split("/")[-1]),
                    "inventory_item_id": int(variant["inventoryItem"]["id"].split("/")[-1]),
                    "inventory_management": None, # No longer supported by GraphQL API
                    "inventory_policy": variant["inventoryPolicy"],
                    "inventory_quantity": variant["inventoryQuantity"],
                    "old_inventory_quantity": None, # No longer supported by GraphQL API
                    "option1": None,  # FIXME, needs to be constructed from selectedOptions
                    "option2": None,  # FIXME, needs to be constructed from selectedOptions
                    "option3": None,  # FIXME, needs to be constructed from selectedOptions
                    "position": variant["position"],
                    "price": variant["price"],
                    "requires_shipping": variant["inventoryItem"]["requiresShipping"],
                    "sku": variant["sku"],
                    "tax_code": variant["taxCode"],
                    "taxable": variant["taxable"],
                    "title": variant["title"],
                    "updated_at": variant["updatedAt"],
                    "weight": variant["weight"],
                    "weight_unit": variant["weightUnit"],
                }
                for variant in graphql_product.get("variants", {}).get("nodes", [])
            ]
        }

        return rest_product


    def get_objects(self):
        updated_at_min = self.get_bookmark()
        stop_time = singer.utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", 1))

        while updated_at_min < stop_time:
            updated_at_max = updated_at_min + timedelta(days=date_window_size)
            if updated_at_max > stop_time:
                updated_at_max = stop_time

            LOGGER.info(f"Fetching products updated between {updated_at_min} and {updated_at_max}")
            cursor = None # Start with no cursor for the first page of this date range

            while True:
                page = self.get_products(updated_at_min, updated_at_max, cursor)
                products = page['data']['products']['nodes']
                page_info = page['data']['products']['pageInfo']

                for product in products:
                    print("about to yield")
                    yield product

                # Update the cursor and check if there's another page
                if page_info['hasNextPage']:
                    cursor = page_info['endCursor']
                else:
                    break

            # Update the bookmark for the next batch
            updated_at_min = updated_at_max
            self.update_bookmark(strftime(updated_at_min))
    
Context.stream_objects['products'] = Products
