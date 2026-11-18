from hotglue_smoke_test.vcr.tap import VCRTapTestRunner


class ShopifyTestRunner(VCRTapTestRunner):
    FILTER_HEADERS = [*VCRTapTestRunner.FILTER_HEADERS, "X-Shopify-Access-Token", "Host"]
    PRESERVE_KEYS = {"id", "updatedAt", "createdAt", "cursor", "endCursor", "hasNextPage", "handle"}

    def module(self) -> str:
        return "tap_shopify"

    def launch(self):
        from tap_shopify import main as shopify_main

        shopify_main()


if __name__ == "__main__":
    ShopifyTestRunner.main()
