import shopify
import singer
from singer.utils import strftime, strptime_to_utc
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream, shopify_error_handling)

LOGGER = singer.get_logger()

DELETED_PRODUCTS_RESULTS_PER_PAGE = 100

class DeletedProducts(Stream):
    name = 'deleted_products'
    replication_key = 'created_at'
    replication_object = shopify.Event

    @shopify_error_handling
    def call_api_for_deleted_products(self):
        return self.replication_object.find(
            limit=DELETED_PRODUCTS_RESULTS_PER_PAGE,
            filter="Product",
            verb="destroy",
            created_at_min=self.get_bookmark()
        )

    def get_deleted_products(self):
        page = self.call_api_for_deleted_products()
        yield from page

        while page.has_next_page():
            page = self.get_next_page(page)
            yield from page

    def get_objects(self):
        deleted_products = self.get_deleted_products()
        for deleted_product in deleted_products:
            yield deleted_product

    def sync(self):
        bookmark = self.get_bookmark()
        self.max_bookmark = bookmark
        
        for deleted_product in self.get_objects():
            deleted_product_dict = deleted_product.to_dict()
            replication_value = strptime_to_utc(deleted_product_dict[self.replication_key])
            if replication_value >= bookmark:
                yield deleted_product_dict
            if replication_value > self.max_bookmark:
                self.max_bookmark = replication_value

        self.update_bookmark(strftime(self.max_bookmark))

Context.stream_objects['deleted_products'] = DeletedProducts 