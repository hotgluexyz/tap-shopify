import json
import shopify
import singer

from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling,
                                      RESULTS_PER_PAGE,
                                      OutOfOrderIdsError)

LOGGER = singer.get_logger()

def get_selected_parents():
    for parent_stream in ['orders', 'customers', 'products', 'custom_collections']:
        if Context.is_selected(parent_stream):
            yield Context.stream_objects[parent_stream]()

@shopify_error_handling
def get_metafields(parent_object, since_id):
    # This call results in an HTTP request - the parent object never has a
    # cache of this data so we have to issue that request.
    return parent_object.metafields(
        limit=Context.get_results_per_page(RESULTS_PER_PAGE),
        since_id=since_id)

class Metafields(Stream):
    name = 'metafields'
    replication_object = shopify.Metafield

    def get_objects(self):
        # Get top-level shop metafields
        yield from super().get_objects()
        # Get parent objects, bookmarking at `metafield_<object_name>`
        for selected_parent in get_selected_parents():
            # The name member controls many things, but most importantly
            # the bookmark key. This switches us over to the
            # `metafield_<parent_type>` bookmark. We track that separately
            # to make resetting individual streams easier.
            selected_parent.name = "metafield_{}".format(selected_parent.name)

            if selected_parent.name == "metafield_products":
                for metafield in selected_parent.get_objects_with_metafields():
                    yield metafield
            else:
                for parent_object in selected_parent.get_objects():
                    since_id = 1
                    while True:
                        metafields = get_metafields(parent_object, since_id)
                        for metafield in metafields:
                            if metafield.id < since_id:
                                raise OutOfOrderIdsError("metafield.id < since_id: {} < {}".format(
                                    metafield.id, since_id))
                            yield metafield
                        if len(metafields) < self.results_per_page:
                            break
                        if metafields[-1].id != max([o.id for o in metafields]):
                            raise OutOfOrderIdsError("{} is not the max id in metafields ({})".format(
                                metafields[-1].id, max([o.id for o in metafields])))
                        since_id += metafields[-1].id

    def sync(self):
        # Shop metafields
        for metafield in self.get_objects():
            metafield = metafield.to_dict()
            value_type = metafield.get("value_type")
            if value_type and value_type == "json_string":
                value = metafield.get("value")
                try:
                    metafield["value"] = json.loads(value) if value is not None else value
                except json.decoder.JSONDecodeError:
                    LOGGER.info("Failed to decode JSON value for metafield %s", metafield.get('id'))
                    metafield["value"] = value

            yield metafield

Context.stream_objects['metafields'] = Metafields
