import gc
import unittest
import weakref
from unittest import mock

import shopify
from shopify.collection import PaginatedCollection
from singer import utils

from tap_shopify.context import Context
from tap_shopify.streams.orders import Orders


class TestOrdersPagination(unittest.TestCase):
    def test_consumed_pages_are_released_during_sync(self):
        page_ids = [[91, 12], [44], [8, 21]]
        page_refs = []

        def find_orders(**params):
            page_index = int(params['from_'].rsplit('=', 1)[1]) if 'from_' in params else 0
            headers = {}
            if page_index + 1 < len(page_ids):
                headers['Link'] = (
                    '<https://example.myshopify.com/admin/api/2023-04/'
                    'orders.json?page_info={}>; rel="next"'.format(page_index + 1)
                )
            page = PaginatedCollection(
                [shopify.Order({'id': order_id}) for order_id in page_ids[page_index]],
                metadata={'resource_class': shopify.Order, 'headers': headers},
            )
            page_refs.append(weakref.ref(page))
            return page

        stop_time = utils.strptime_with_tz('2026-11-13T20:00:00Z')
        with shopify.Session.temp('example.myshopify.com', '2023-04', 'test-token'), mock.patch.object(Context, 'config', {
            'start_date': '2026-11-12T00:00:00Z',
            'use_created_at_replication_key_for_orders': True,
        }), mock.patch.object(Context, 'state', {}), mock.patch(
            'shopify.Order.find', side_effect=find_orders
        ), mock.patch('singer.utils.now', return_value=stop_time), mock.patch(
            'singer.write_state'
        ):
            records = []
            for record in Orders().sync():
                records.append(record['id'])
                gc.collect()
                self.assertLessEqual(
                    sum(ref() is not None for ref in page_refs), 1,
                    'Forward pagination must not retain consumed pages',
                )

            self.assertEqual(records, [91, 12, 44, 8, 21])
            self.assertEqual(
                Context.state['bookmarks']['orders']['created_at'],
                utils.strftime(stop_time),
            )
