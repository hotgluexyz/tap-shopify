import pyactiveresource
import shopify
import singer
from singer.utils import strftime, strptime_to_utc
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling)

LOGGER = singer.get_logger()

EVENTS_RESULTS_PER_PAGE = 100


class EventsProducts(Stream):
    name = 'events_products'
    replication_key = 'created_at'
    replication_object = shopify.Event

    def __init__(self):
        super().__init__()
        self.events_page_size = EVENTS_RESULTS_PER_PAGE

    @shopify_error_handling
    def call_api_for_events_products(self):
        return self.replication_object.find(
            limit=self.events_page_size,
            filter="Product",
            # verb = "destroy",
            created_at_min=self.get_bookmark()
        )

    def get_events_products(self):
        while True:
            try:
                page = self.call_api_for_events_products()
                break
            except pyactiveresource.connection.ServerError:
                new_size = self.reduce_page_size(self.events_page_size)
                if new_size:
                    self.events_page_size = new_size
                else:
                    raise
        yield from page

        while page.has_next_page():
            page = self.get_next_page(page)
            yield from page

    def get_objects(self):
        events_products = self.get_events_products()
        for events_product in events_products:
            yield events_product

    def sync(self):
        bookmark = self.get_bookmark()
        self.max_bookmark = bookmark
        for events_product in self.get_objects():
            events_product_dict = events_product.to_dict()
            replication_value = strptime_to_utc(events_product_dict[self.replication_key])
            if replication_value >= bookmark:
                yield events_product_dict
            if replication_value > self.max_bookmark:
                self.max_bookmark = replication_value

        self.update_bookmark(strftime(self.max_bookmark))


Context.stream_objects['events_products'] = EventsProducts
