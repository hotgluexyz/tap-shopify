import json
import os
from tap_shopify.streams.compatibility.compatibility_mixin import CompatibilityMixin

class MetafieldCompatibility(CompatibilityMixin):
    def __init__(self, graphql_metafield):
        """Initialize with a GraphQL metafield object."""
        self.graphql_metafield =  graphql_metafield
        self.admin_graphql_api_id = graphql_metafield["id"]
        self.metafield_id = self._extract_int_id(graphql_metafield["id"])

        current_dir = os.path.dirname(os.path.abspath(__file__))
        value_map_path = os.path.join(current_dir, "value_maps", "metafield.json")
        with open(value_map_path, 'r') as file:
            self.value_map = json.load(file)

    def to_dict(self):
        metafield_dict = {
            "admin_graphql_api_id": self.admin_graphql_api_id,
            "owner_resource": self.graphql_metafield["ownerType"],
            "key": self.graphql_metafield["key"],
            "created_at": self.graphql_metafield["createdAt"],
            "id": self.metafield_id,
            "namespace": self.graphql_metafield["namespace"],
            "description": self.graphql_metafield["description"],
            "value": self.graphql_metafield["value"],
            "updated_at": self.graphql_metafield["updatedAt"],
            "owner_id": None # no longer supported in GraphQL
        }

        return self._cast_values(metafield_dict, self.value_map)
