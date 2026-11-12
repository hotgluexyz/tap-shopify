import json
from pathlib import Path

from hotglue_smoke_test.vcr.tap import VCRTapTestRunner
from hotglue_smoke_test.vcr.sanitize import redact_credential, scrub_response_body


class ShopifyTestRunner(VCRTapTestRunner):
    FILTER_HEADERS = [*VCRTapTestRunner.FILTER_HEADERS, "X-Shopify-Access-Token", "Host"]
    TOKEN_KEYS = [*VCRTapTestRunner.TOKEN_KEYS, "shop"]
    PRESERVE_KEYS = {"id", "updatedAt", "createdAt", "cursor", "endCursor", "hasNextPage"}

    def module(self) -> str:
        return "tap_shopify"

    def launch(self):
        from tap_shopify import main as shopify_main

        shopify_main()

    def scrub_uri(self, uri: str) -> str:
        """Replace the configured Shopify host in recorded request URIs."""
        config_path = Path(self.test_case_path) / "config.json"
        shop = json.loads(config_path.read_text()).get("shop")
        if not shop:
            return uri
        scrubbed_shop = redact_credential(shop.split(".")[0])
        return uri.replace(f"https://{shop.split('.')[0]}.myshopify.com", f"https://{scrubbed_shop}.myshopify.com")

    def scrub_response_body(self, body, faker, cache):
        """Scrub response values while preserving access-scope handles the tap consumes."""
        # `shop` is a credential in config.json, but a response object whose fields must be scrubbed.
        response = json.loads(
            scrub_response_body(
                body,
                set(self.PRESERVE_KEYS),
                faker,
                cache,
                set(self.TOKEN_KEYS) - {"shop"},
            )
        )
        original = json.loads(body)
        original_scopes = original.get("data", {}).get("appInstallation", {}).get("accessScopes", [])
        scrubbed_scopes = response.get("data", {}).get("appInstallation", {}).get("accessScopes", [])
        for original_scope, scrubbed_scope in zip(original_scopes, scrubbed_scopes):
            scrubbed_scope["handle"] = original_scope["handle"]
        return json.dumps(response)


if __name__ == "__main__":
    ShopifyTestRunner.main()
