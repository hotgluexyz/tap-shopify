import pyactiveresource
import shopify
import singer
from singer.utils import strftime, strptime_to_utc
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling)

LOGGER = singer.get_logger()

DISCOUNT_CODES_RESULTS_PER_PAGE = 100


class DiscountCodes(Stream):
    name = 'discount_codes'
    replication_key = 'created_at'
    replication_object = shopify.DiscountCode

    def __init__(self):
        super().__init__()
        self.discount_codes_page_size = DISCOUNT_CODES_RESULTS_PER_PAGE

    @shopify_error_handling
    def call_api_for_discount_codes(self, parent_object):
        return self.replication_object.find(
            limit=self.discount_codes_page_size,
            price_rule_id=parent_object.id,
        )

    def get_discount_codes(self, parent_object):
        while True:
            try:
                page = self.call_api_for_discount_codes(parent_object)
                break
            except pyactiveresource.connection.ServerError:
                new_size = self.reduce_page_size(self.discount_codes_page_size)
                if new_size:
                    self.discount_codes_page_size = new_size
                else:
                    raise
        yield from page

        while page.has_next_page():
            page = self.get_next_page(page)
            yield from page

    def get_objects(self):
        selected_parent = Context.stream_objects['price_rules']()
        selected_parent.name = "discount_code_price_rules"

        for parent_object in selected_parent.get_objects():
            discount_codes = self.get_discount_codes(parent_object)
            for discount_code in discount_codes:
                yield discount_code

    def sync(self):
        bookmark = self.get_bookmark()
        self.max_bookmark = bookmark
        for discount_code in self.get_objects():
            discount_code_dict = discount_code.to_dict()
            replication_value = strptime_to_utc(discount_code_dict[self.replication_key])
            if replication_value >= bookmark:
                yield discount_code_dict
            if replication_value > self.max_bookmark:
                self.max_bookmark = replication_value

        self.update_bookmark(strftime(self.max_bookmark))


Context.stream_objects['discount_codes'] = DiscountCodes
