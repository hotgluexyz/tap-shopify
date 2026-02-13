# tap-shopify Configuration

This document describes the configuration options for the tap-shopify Singer tap, which extracts data from the Shopify REST and GraphQL APIs.

---

## Configuration Options

### Connection

#### `shop` (string, required)
The Shopify store name (subdomain only, without `.myshopify.com`). If you provide the full domain, the tap will use the subdomain part only.
- **Example**: `"my-store"` or `"my-store.myshopify.com"` (both resolve to `my-store`)

---

### Authentication

You must provide either an access token (or API key) or OAuth client credentials.

#### `access_token` (string, optional)
Shopify Admin API access token. Takes precedence over `api_key` when both are set.
- **Example**: `"shpat_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`

#### `api_key` (string, optional)
Shopify API key (e.g. private app password). Used when `access_token` is not set.
- **Example**: `"foobarcharnockbat"`

#### `client_id` (string, optional)
OAuth application client ID. Used with `client_secret` to obtain an access token via the client credentials flow when neither `access_token` nor `api_key` is provided.
- **Example**: `"your-oauth-client-id"`

#### `client_secret` (string, optional)
OAuth application client secret. Must be set together with `client_id` for token exchange.
- **Example**: `"your-oauth-client-secret"`

---

### API Configuration

#### `api_version` (string, optional)
Shopify REST API version. Use the version string without a `v` prefix.
- **Default**: `"2026-01"`
- **Example**: `"2026-01"` or `"2024-01"`

#### `graphql_api_version` (string, optional)
Shopify GraphQL Admin API version. Used for GraphQL requests (e.g. products, shop ID lookup).
- **Default**: `"2026-01"`
- **Example**: `"2026-01"`

#### `shop_id` (string, optional)
Shop ID used for GraphQL requests. If not provided, the tap fetches it from the Shopify GraphQL API using the configured `shop` and credentials.
- **Example**: `"your-store-name"` (often the same as `shop`)

---

### Sync Behavior

#### `start_date` (string, required for incremental streams)
Start date for incremental replication, in ISO 8601 format. Used as the initial bookmark when no state exists.
- **Example**: `"2017-01-01T00:00:00Z"`

#### `date_window_size` (number, optional)
Size of the date window in days used when querying incremental streams. Smaller values reduce memory and can avoid API issues; larger values reduce the number of requests.
- **Default**: `365` for most incremental streams; the products stream uses `1` when this option is not set.
- **Example**: `30` or `365`

#### `results_per_page` (integer, optional)
Number of records to request per API page for REST streams that support it. Invalid or non-integer values are ignored and the stream default is used.
- **Default**: `175` (stream default; some streams use fixed limits such as 100 or 250)
- **Example**: `100` or `175`

#### `variant_fetch_workers` (integer, optional)
Maximum number of concurrent workers used when fetching product variants in the **products** stream. Higher values can speed up syncs but increase load on the API.
- **Default**: `8`
- **Example**: `4` or `16`

#### `use_created_at_replication_key_for_orders` (boolean, optional)
When `true`, the **orders** stream uses `created_at` instead of `updated_at` as its replication key. Use this when you want to replicate orders by creation time rather than last update time.
- **Default**: `false` (orders use `updated_at`)
- **Example**: `true`

---

## Example: Minimal Configuration

Only the required connection and authentication options, plus `start_date` for incremental syncs:

```json
{
  "shop": "my-store",
  "access_token": "shpat_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "start_date": "2017-01-01T00:00:00Z"
}
```

Alternatively, using API key and OAuth client credentials:

```json
{
  "shop": "my-store",
  "api_key": "your-private-app-password",
  "start_date": "2017-01-01T00:00:00Z"
}
```

```json
{
  "shop": "my-store",
  "client_id": "your-oauth-client-id",
  "client_secret": "your-oauth-client-secret",
  "start_date": "2017-01-01T00:00:00Z"
}
```

---

## Example: Full Configuration

All options with sample values:

```json
{
  "shop": "my-store",
  "access_token": "shpat_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "api_version": "2026-01",
  "graphql_api_version": "2026-01",
  "shop_id": "my-store",
  "start_date": "2017-01-01T00:00:00Z",
  "date_window_size": 365,
  "results_per_page": 175,
  "variant_fetch_workers": 8,
  "use_created_at_replication_key_for_orders": false
}
```
