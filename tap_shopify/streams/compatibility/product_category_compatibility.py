
class ProductCategoryCompatibility():
    def __init__(self, graphql_product):
        """Initialize with a GraphQL product object that has selected product category."""
        self.graphql_product = graphql_product
        # Unlike other compatibility objects, this one leaves the fully qualified gid intact, as this is what the existing stream does.
        self.product_id = graphql_product["id"]

    def to_dict(self):
        return {
            "id": self.product_id,
            "category_id": self.graphql_product["productCategory"]["productTaxonomyNode"]["id"],
            "full_name": self.graphql_product["productCategory"]["productTaxonomyNode"]["fullName"],
            "is_leaf": self.graphql_product["productCategory"]["productTaxonomyNode"]["isLeaf"],
            "is_root": self.graphql_product["productCategory"]["productTaxonomyNode"]["isRoot"],
            "createdAt": self.graphql_product["createdAt"] # for bookmarking
        }
