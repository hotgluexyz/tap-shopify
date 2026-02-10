import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class Orders(Stream):
    name = 'orders'
    replication_object = shopify.Order

    def __init__(self):
        super().__init__()
        if Context.config.get("use_created_at_replication_key_for_orders") is True:
            self.replication_key = "created_at"
        else:
            self.replication_key = "updated_at"


Context.stream_objects['orders'] = Orders
