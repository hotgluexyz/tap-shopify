"""Refresh expiring Shopify offline access tokens.

A config with a ``refresh_token`` holds an expiring access token. It is refreshed
with ``grant_type=refresh_token`` when it is missing or near expiry, and once
more when Shopify answers a request with 401. Configs without a refresh_token
are never refreshed.

``expires_in`` is Shopify's token lifetime in seconds until this tap refreshes
the token, after which it holds the Unix time the token expires.

https://shopify.dev/docs/apps/build/authentication-authorization/migrate-to-expiring-offline-access-tokens
"""
import json
import os
import shutil
import tempfile
import threading
import time
import urllib.error

import backoff
import pyactiveresource.connection
import requests
import shopify
import singer

from tap_shopify.context import Context

LOGGER = singer.get_logger()

ACCESS_TOKEN_HEADER = "X-Shopify-Access-Token"
REFRESH_SECONDS_BEFORE_EXPIRY = 120
# Lifetimes from Shopify are about an hour; Unix times are above this
SMALLEST_UNIX_TIME = 10**9
REST_OR_GRAPHQL_HTTP_ERRORS = (pyactiveresource.connection.ClientError, urllib.error.HTTPError)

_one_refresh_at_a_time = threading.Lock()


def uses_expiring_token():
    return bool(Context.config.get("refresh_token"))


def _needs_refresh():
    expires_in = Context.config.get("expires_in")
    if not Context.config.get("access_token") or expires_in is None:
        return True
    if expires_in < SMALLEST_UNIX_TIME:
        return True
    return expires_in - time.time() <= REFRESH_SECONDS_BEFORE_EXPIRY


class TransientRefreshError(Exception):
    """Shopify answered the refresh with 429 or 5xx; the same refresh token can be retried."""


# The old refresh token stays valid until the new one is used, so retrying is safe even if
# Shopify processed an attempt whose response never arrived
@backoff.on_exception(
    backoff.expo,
    (TransientRefreshError, requests.exceptions.ConnectionError, requests.exceptions.Timeout),
    max_tries=5,
)
def _request_new_tokens():
    shop = Context.config["shop"].split(".")[0]
    response = requests.post(
        f"https://{shop}.myshopify.com/admin/oauth/access_token",
        data={
            "client_id": Context.config["client_id"],
            "client_secret": Context.config["client_secret"],
            "grant_type": "refresh_token",
            "refresh_token": Context.config["refresh_token"],
        },
        timeout=300,
    )
    if response.status_code == 429 or response.status_code >= 500:
        raise TransientRefreshError(
            f"Failed to refresh access token (HTTP {response.status_code}): {response.text}"
        )
    if not response.ok:
        raise ValueError(f"Failed to refresh access token: {response.text}")
    return response.json()


def _refresh_access_token():
    requested_at = int(time.time())
    token = _request_new_tokens()
    new_tokens = {
        "access_token": token["access_token"],
        "refresh_token": token["refresh_token"],
        "expires_in": requested_at + token["expires_in"],
    }
    _save_tokens_to_config_file(new_tokens)
    Context.config.update(new_tokens)
    LOGGER.info("Refreshed the Shopify access token.")


def _save_tokens_to_config_file(new_tokens):
    """Write the config to a temp file and swap it in, so a failed write can't truncate it."""
    # Only the token keys: Context.config also holds values the tap derives (shop, shop_id)
    with open(Context.config_path) as config_file:
        original_config_text = config_file.read()
    saved_config = json.loads(original_config_text)
    saved_config.update(new_tokens)
    config_dir = os.path.dirname(os.path.abspath(Context.config_path))
    temp_fd, temp_path = tempfile.mkstemp(dir=config_dir, suffix=".tmp")
    try:
        with os.fdopen(temp_fd, "w") as temp_file:
            json.dump(saved_config, temp_file, indent=2)
        try:
            os.replace(temp_path, Context.config_path)
        except OSError:
            # A config mounted on its own (single-file Docker bind mount) can't be renamed over,
            # so it is rewritten in place, and put back as it was if that write fails
            try:
                shutil.copyfile(temp_path, Context.config_path)
            except OSError:
                with open(Context.config_path, "w") as config_file:
                    config_file.write(original_config_text)
                raise
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def _use_access_token(token):
    shopify.ShopifyResource.headers[ACCESS_TOKEN_HEADER] = token
    sessions_created = hasattr(Context, "shopify_rest_session")
    if sessions_created:
        Context.shopify_rest_session.token = token
        Context.shopify_graphql_session.token = token


def get_valid_access_token():
    """Return the expiring access token, refreshed first if needed; None for other configs."""
    if not uses_expiring_token():
        return None
    with _one_refresh_at_a_time:
        if _needs_refresh():
            _refresh_access_token()
    _use_access_token(Context.config["access_token"])
    return Context.config["access_token"]


def refresh_access_token_after_401(exc, rejected_token):
    """Refresh the expiring token after a 401; return True if the request should be retried.

    When another thread already replaced ``rejected_token``, the request is retried with
    that newer token instead of refreshing (and rotating the token pair) again.
    """
    is_401 = isinstance(exc, pyactiveresource.connection.UnauthorizedAccess) or (
        isinstance(exc, urllib.error.HTTPError) and exc.code == 401
    )
    if not (is_401 and uses_expiring_token()):
        return False
    with _one_refresh_at_a_time:
        already_refreshed_by_another_thread = Context.config["access_token"] != rejected_token
        if not already_refreshed_by_another_thread:
            _refresh_access_token()
    _use_access_token(Context.config["access_token"])
    return True


def headers_with_latest_access_token(headers):
    if not uses_expiring_token():
        return headers
    return {**headers, ACCESS_TOKEN_HEADER: Context.config["access_token"]}
