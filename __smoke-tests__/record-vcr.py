import json

from hotglue_smoke_test.vcr.tap import VCRTapTestRunner


class ShopifyTestRunner(VCRTapTestRunner):
    FILTER_HEADERS = [*VCRTapTestRunner.FILTER_HEADERS, "X-Shopify-Access-Token", "Host"]
    PRESERVE_KEYS = {"id", "updatedAt", "createdAt", "cursor", "endCursor", "hasNextPage"}

    def module(self) -> str:
        return "tap_shopify"

    def launch(self):
        from tap_shopify import main as shopify_main

        shopify_main()

    def scrub_response_body(self, body, faker, cache):
        """Scrub response values while preserving access-scope handles the tap consumes."""
        response = json.loads(super().scrub_response_body(body, faker, cache))
        original = json.loads(body)
        original_scopes = original.get("data", {}).get("appInstallation", {}).get("accessScopes", [])
        scrubbed_scopes = response.get("data", {}).get("appInstallation", {}).get("accessScopes", [])
        for original_scope, scrubbed_scope in zip(original_scopes, scrubbed_scopes):
            scrubbed_scope["handle"] = original_scope["handle"]
        return json.dumps(response)

if __name__ == "__main__":
    ShopifyTestRunner.main()
