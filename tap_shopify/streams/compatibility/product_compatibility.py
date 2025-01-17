import json
import os
import shopify.resources

class ProductCompatibility():
    def __init__(self, graphql_product):
        """Initialize with a GraphQL product object."""
        self.graphql_product = graphql_product
        self.admin_graphql_api_id = graphql_product["id"]
        self.product_id = self._extract_int_id(graphql_product["id"])

        current_dir = os.path.dirname(os.path.abspath(__file__))
        value_map_path = os.path.join(current_dir, "value_maps", "product.json")
        with open(value_map_path, 'r') as file:
            self.value_map = json.load(file)

    def metafields(self, _options=None, **kwargs):
        if _options is None:
            _options = kwargs
        return shopify.resources.Metafield.find(resource="products", resource_id=self.product_id, **_options)

    def _extract_int_id(self, string_id):
        return int(string_id.split("/")[-1])

    def _convert_options(self):
        return [
            {
                "id": self._extract_int_id(option["id"]),
                "product_id": self.product_id,
                "name": option["name"],
                "position": option["position"],
                "values": option["values"]
            }
            for option in self.graphql_product["options"]
        ]

    def _convert_images(self):
        return [
            {
                "id": self._extract_int_id(image["id"]),
                "admin_graphql_api_id": image["id"],
                "position": idx + 1,
                "alt": image["altText"],
                "created_at": None,  # No longer supported by GraphQL API
                "updated_at": None,  # No longer supported by GraphQL API
                "width": image["width"],
                "height": image["height"],
                "src": image["src"],
                "variant_ids": None  # No longer supported by GraphQL API
            }
            for idx, image in enumerate(self.graphql_product.get("images", {}).get("nodes", []))
        ]

    def _extract_variant_options(self, variant):
        option_dict = {
            "option1": None,
            "option2": None,
            "option3": None
        } # The maximum number of selectedOptions returned from a ProductVariant is 3
        selected_options = variant["selectedOptions"]
        for idx, option in enumerate(selected_options):
            option_dict[f"option{idx + 1}"] = option["value"]
        return option_dict

    def _cast_variant_values(self, variant):
        """Cast variant values based on the value_map."""
        for key, value in variant.items():
            if key in self.value_map["variants"]:
                key_map = self.value_map["variants"][key]
                if value in key_map:
                    variant[key] = key_map[value]
        return variant

    def _convert_variants(self):
        return [
            {
                "admin_graphql_api_id": variant["id"],
                "barcode": variant["barcode"],
                "compare_at_price": variant["compareAtPrice"],
                "created_at": variant["createdAt"],
                "fulfillment_service": variant["fulfillmentService"]["handle"],
                "grams": None,  # No longer supported by GraphQL API
                "id": self._extract_int_id(variant["id"]),
                "image_id": self._extract_int_id(variant["image"]["id"]) if variant.get("image") else None,
                "inventory_item_id": self._extract_int_id(variant["inventoryItem"]["id"]),
                "inventory_management": None,  # No longer supported by GraphQL API
                "inventory_policy": variant["inventoryPolicy"],
                "inventory_quantity": variant["inventoryQuantity"],
                "old_inventory_quantity": None,  # No longer supported by GraphQL API
                "position": variant["position"],
                "price": variant["price"],
                "requires_shipping": variant["inventoryItem"]["requiresShipping"],
                "sku": variant["sku"],
                "tax_code": variant["taxCode"],
                "taxable": variant["taxable"],
                "title": variant["title"],
                "updated_at": variant["updatedAt"],
                "weight": variant["weight"],
                "weight_unit": variant["weightUnit"],
            } | self._extract_variant_options(variant)
            for variant in self.graphql_product.get("variants", {}).get("nodes", [])
        ]

    def _cast_values(self, data, mappings):
        """
        Recursively traverse and cast values in a dictionary or list based on mappings.
        :param data: The data to process (dictionary, list, or scalar).
        :param mappings: The mapping dictionary to use for casting.
        :return: The processed data with values cast according to the mappings.
        """
        if isinstance(data, dict):
            return {
                key: self._cast_values(value, mappings.get(key, {}))
                for key, value in data.items()
            }
        elif isinstance(data, list):
            return [self._cast_values(item, mappings) for item in data]
        elif data in mappings:
            return mappings[data]
        return data

    def to_dict(self):
        """Return the REST API-compatible product as a dictionary."""
        product_dict = {
            "admin_graphql_api_id": self.graphql_product["id"],
            "body_html": self.graphql_product["descriptionHtml"] or "",
            "created_at": self.graphql_product["createdAt"],
            "handle": self.graphql_product["handle"],
            "id": self.product_id,
            "image": None,  # No longer supported by GraphQL API
            "product_type": self.graphql_product["productType"],
            "published_at": self.graphql_product["publishedAt"],
            "published_scope": None,  # No longer supported by GraphQL API
            "status": self.graphql_product["status"],
            "tags": ", ".join(self.graphql_product["tags"]),
            "template_suffix": self.graphql_product["templateSuffix"],
            "title": self.graphql_product["title"],
            "updated_at": self.graphql_product["updatedAt"],
            "vendor": self.graphql_product["vendor"],
            "options": self._convert_options(),
            "images": self._convert_images(),
            "variants": self._convert_variants()
        }

        return self._cast_values(product_dict, self.value_map)
