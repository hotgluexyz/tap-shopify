import os
import sys
import singer
from singer.utils import strftime, strptime_to_utc

from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

LOGGER = singer.get_logger()

class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout

class ProductCategory(Stream):
    name = 'product_category'
    replication_key = 'createdAt'

    def get_objects(self):
        selected_parent = Context.stream_objects['products']()
        selected_parent.name = "products_categories"
        for product_category in selected_parent.get_objects_with_categories():
            yield product_category

    def sync(self):
        bookmark = self.get_bookmark()
        self.max_bookmark = bookmark
        for incoming_item in self.get_objects():
            replication_value = strptime_to_utc(incoming_item[self.replication_key])
            if replication_value >= bookmark:
                yield incoming_item
            if replication_value > self.max_bookmark:
                self.max_bookmark = replication_value

        self.update_bookmark(strftime(self.max_bookmark))


Context.stream_objects['product_category'] = ProductCategory
