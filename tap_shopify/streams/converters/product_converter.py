class ProductConverter():
    def __init__(self, graphql_product):
        """Initialize with a GraphQL product object."""
        self.graphql_product = graphql_product
        self.product_id = self._extract_int_id(graphql_product["id"])
    
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
                "position": idx + 1,
                "created_at": None,  # No longer supported by GraphQL API
                "updated_at": None,  # No longer supported by GraphQL API
                "width": image["width"],
                "height": image["height"],
                "src": image["src"],
                "variant_ids": None  # No longer supported by GraphQL API
            }
            for idx, image in enumerate(self.graphql_product.get("images", {}).get("nodes", []))
        ]

    def _convert_variants(self):
        return [
            {
                "barcode": variant["barcode"],
                "compare_at_price": variant["compareAtPrice"],
                "created_at": variant["createdAt"],
                "fulfillment_service": variant["fulfillmentService"]["handle"],
                "grams": None,  # No longer supported by GraphQL API
                "id": self._extract_int_id(variant["id"]),
                "inventory_item_id": self._extract_int_id(variant["inventoryItem"]["id"]),
                "inventory_management": None,  # No longer supported by GraphQL API
                "inventory_policy": variant["inventoryPolicy"],
                "inventory_quantity": variant["inventoryQuantity"],
                "old_inventory_quantity": None,  # No longer supported by GraphQL API
                "option1": None,  # FIXME, needs to be constructed from selectedOptions
                "option2": None,  # FIXME, needs to be constructed from selectedOptions
                "option3": None,  # FIXME, needs to be constructed from selectedOptions
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
            }
            for variant in self.graphql_product.get("variants", {}).get("nodes", [])
        ]

    def to_dict(self):
        """Return the REST API-compatible product as a dictionary."""
        return {
            "body_html": self.graphql_product["descriptionHtml"],
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
