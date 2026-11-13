import datetime

import pyactiveresource
import shopify
import singer
from singer import metrics, utils

from tap_shopify.context import Context
from tap_shopify.streams.base import DATE_WINDOW_SIZE, Stream


class Orders(Stream):
    name = 'orders'
    replication_object = shopify.Order

    def __init__(self):
        super().__init__()
        if Context.config.get("use_created_at_replication_key_for_orders") is True:
            self.replication_key = "created_at"
        else:
            self.replication_key = "updated_at"

    def get_objects(self):
        # Parent reuse (refund_orders / transaction_orders) keeps base since_id path.
        if self.name != "orders":
            yield from super().get_objects()
            return

        updated_at_min = self.get_bookmark()
        stop_time = singer.utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))
        
        # Remove the since_id from the state
        Context.state.get('bookmarks', {}).get(self.name, {}).pop('since_id', None)

        # Page through till the end of the resultset
        while updated_at_min < stop_time:
            updated_at_max = updated_at_min + datetime.timedelta(days=date_window_size)
            if updated_at_max > stop_time:
                updated_at_max = stop_time

            query_params = {
                self.replication_key + "_min": updated_at_min,
                self.replication_key + "_max": updated_at_max,
                "limit": self.results_per_page,
                "status": "any",
            }

            while True:
                # Make the first call to the API
                # If fail, reduce page size, then retry
                try:
                    with metrics.http_request_timer(self.name):
                        page = self.call_api(query_params)
                    break
                except pyactiveresource.connection.ServerError:
                    if self.reduce_page_size():
                        query_params["limit"] = self.results_per_page
                        continue
                    raise

            while True:
                # After first page is sucess, get the following until no next page
                yield from page
                if not page.has_next_page():
                    break
                with metrics.http_request_timer(self.name):
                    # Get the next page
                    page = self.get_next_page(page)
                
            self.update_bookmark(utils.strftime(updated_at_max))
            updated_at_min = updated_at_max


Context.stream_objects['orders'] = Orders
