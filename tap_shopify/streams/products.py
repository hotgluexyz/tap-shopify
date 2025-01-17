import shopify
from tap_shopify.streams.base import (Stream, shopify_error_handling)
from tap_shopify.context import Context
import json

class Products(Stream):
    name = 'products'
    replication_object = shopify.Product
    status_key = "published_status"

    gql_query = """
        query GetProducts {
            products(first: 250) {
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
            }
        }
    """

    # @shopify_error_handling
    def call_api_for_products(self, gql_client, query, cursor=None):
        response = gql_client.execute(self.gql_query, dict(query=query, cursor=cursor))
        result = json.loads(response)
        if result.get("errors"):
            raise Exception(result['errors'])
        return result

    def get_products(self, query):
        # set to new version
        gql_client = shopify.GraphQL()
        page = self.call_api_for_products(gql_client, query)
        return page
        # reset to previous version
        # yield page

    def get_objects(self):
        page = self.get_products("")
        print("HELLO")


Context.stream_objects['products'] = Products
