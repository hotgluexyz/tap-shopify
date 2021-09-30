"""
Test tap pagination of streams
"""
from tap_tester import menagerie, runner

from base import BaseTapTest


class PaginationTest(BaseTapTest):
    """ Test the tap pagination to get multiple pages of data """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = '2021-04-01T00:00:00Z'        

    def name(self):
        return "tap_tester_shopify_pagination_test"

    def get_properties(self, *args, **kwargs):
        props = super().get_properties(*args, **kwargs)
        props['results_per_page'] = '50'
        return props

    def test_run(self):
        with self.subTest(store="store_1"):
            conn_id = self.create_connection(original_credentials=True)
            self.pagination_test(conn_id, self.store_1_streams)

        with self.subTest(store="store_2"):
            conn_id = self.create_connection(original_properties=False, original_credentials=False)
            self.pagination_test(conn_id, self.store_2_streams)

    
    def pagination_test(self, conn_id, testable_streams):
        """
        Verify that for each stream you can get multiple pages of data
        and that when all fields are selected more than the automatic fields are replicated.

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """

        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        incremental_streams = {key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL and key in testable_streams}


        # our_catalogs = [catalog for catalog in found_catalogs if
        #                 catalog.get('tap_stream_id') in incremental_streams.difference(
        #                     untested_streams)]
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in testable_streams]


        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)
        # Run a sync job using orchestrator
        record_count_by_stream = self.run_sync(conn_id)
        actual_fields_by_stream = runner.examine_target_output_for_fields()

        api_limit = int(self.get_properties().get('results_per_page', self.DEFAULT_RESULTS_PER_PAGE))

        for stream in testable_streams:
            with self.subTest(stream=stream):

                # verify that we can paginate with all fields selected
                stream_metadata = self.expected_metadata().get(stream, {})
                minimum_record_count = 100 if stream == 'transactions' else api_limit
                self.assertGreater(
                    record_count_by_stream.get(stream, -1),
                    minimum_record_count,
                    msg="The number of records is not over the stream max limit")

                # verify that the automatic fields are sent to the target
                self.assertTrue(
                    actual_fields_by_stream.get(stream, set()).issuperset(
                        self.expected_primary_keys().get(stream, set()) |
                        self.expected_replication_keys().get(stream, set()) |
                        self.expected_foreign_keys().get(stream, set())),
                    msg="The fields sent to the target don't include all automatic fields"
                )

                # verify we have more fields sent to the target than just automatic fields
                # SKIP THIS ASSERTION IF ALL FIELDS ARE INTENTIONALLY AUTOMATIC FOR THIS STREAM
                self.assertTrue(
                    actual_fields_by_stream.get(stream, set()).symmetric_difference(
                        self.expected_primary_keys().get(stream, set()) |
                        self.expected_replication_keys().get(stream, set()) |
                        self.expected_foreign_keys().get(stream, set())),
                    msg="The fields sent to the target don't include non-automatic fields"
                )
