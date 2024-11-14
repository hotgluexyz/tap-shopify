import json


class ShopifyError(Exception):
    def __init__(self, error, msg=''):
        super().__init__('{}\n{}'.format(error.__class__.__name__, msg))

SHOPIFY_API_RESPONSES = {
    200: {"description": "OK", "message": "The request was successfully processed by Shopify."},
    201: {"description": "Created", "message": "The request has been fulfilled and a new resource has been created."},
    202: {"description": "Accepted", "message": "The request has been accepted, but not yet processed."},
    204: {"description": "No Content", "message": "The request has been accepted, but no content will be returned. For example, a client might use an update operation to save a document temporarily, and not refresh to a new page."},
    205: {"description": "Reset Content", "message": "The request has been accepted, but no content will be returned. The client must reset the document from which the original request was sent. For example, if a user fills out a form and submits it, then the 205 code means that the server is making a request to the browser to clear the form."},
    303: {"description": "See Other", "message": "The response to the request can be found under a different URL in the Location header and can be retrieved using a GET method on that resource."},
    400: {"description": "Bad Request", "message": "The request wasn't understood by the server, generally due to bad syntax or because the Content-Type header wasn't correctly set to application/JSON."},
    401: {"description": "Unauthorized", "message": "The necessary authentication credentials are not present in the request or are incorrect."},
    402: {"description": "Payment Required", "message": "The requested shop is currently frozen. The shop owner needs to log in to the shop's admin and pay the outstanding balance to unfreeze the shop."},
    403: {"description": "Forbidden", "message": "The server is refusing to respond to the request. This status is generally returned if you haven't requested the appropriate scope for this action."},
    404: {"description": "Not Found", "message": "The requested resource was not found but could be available again in the future."},
    405: {"description": "Method Not Allowed", "message": "The server recognizes the request but rejects the specific HTTP method."},   
    406: {"description": "Not Acceptable", "message": "The request's Accept header doesn't specify any content formats that the server is able to fulfill."},
    409: {"description": "Resource Conflict", "message": "The requested resource couldn't be processed because of conflict in the request. For example, the requested resource might not be in an expected state, or processing the request would create a conflict within the resource."},
    414: {"description": "URI Too Long", "message": "The server is refusing to accept the request because the Uniform Resource Identifier (URI) provided was too long."},
    415: {"description": "Unsupported Media Type", "message": "The request's Content-Type header specifies a payload format that the server doesn't support."},
    422: {"description": "Unprocessable Entity", "message": "The request body was well-formed but contains semantic errors. A 422 error code can be returned from a variety of scenarios including, but not limited to: Incorrectly formatted input Checking out products that are out of stock Canceling an order that has fulfillments Creating an order with tax lines on both line items and the order Creating a customer without an email or name Creating a product without a title The response body provides details in the errors or error parameters."},
    423: {"description": "Locked", "message": "The requested shop is currently locked. Shops are locked if they repeatedly exceed their API request limit, or if there is an issue with the account, such as a detected compromise or fraud risk. Contact support if your shop is locked."},
    429: {"description": "Too Many Requests", "message": "The request was not accepted because the application has exceeded the rate limit. Learn more about Shopify’s API rate limits."},
    430: {"description": "Shopify Security Rejection", "message": "The request was not accepted because the request might be malicious, and Shopify has responded by rejecting it to protect the app from any possible attacks."},
    500: {"description": "Internal Server Error", "message": "An internal error occurred in Shopify. Simplify or retry your request. If the issue persists, then please record any error codes, timestamps and contact Partner Support so that Shopify staff can investigate."},
    501: {"description": "Not Implemented", "message": "The requested endpoint is not available on that particular shop, e.g. requesting access to a Shopify Plus–only API on a non-Plus shop. This response may also indicate that this endpoint is reserved for future use."},
    502: {"description": "Bad Gateway", "message": "The server, while acting as a gateway or proxy, received an invalid response from the upstream server. A 502 error isn't typically something you can fix. It usually requires a fix on the web server or the proxies that you're trying to get access through."},
    503: {"description": "Service Unavailable", "message": "The server is currently unavailable. Check the Shopify status page for reported service outages."},
    504: {"description": "Gateway Timeout", "message": "The request couldn't complete in time. Shopify waits up to 10 seconds for a response. Try breaking it down in multiple smaller requests."},
    530: {"description": "Origin DNS Error", "message": "Cloudflare can't resolve the requested DNS record. Check the Shopify status page for reported service outages."},
    540: {"description": "Temporarily Disabled", "message": "The requested endpoint isn't currently available. It has been temporarily disabled, and will be back online shortly."},
    783: {"description": "Unexpected Token", "message": "The request includes a JSON syntax error, so the API code is failing to convert some of the data that the server returned."},
}


def get_message(exc, custom_message=None):
    if not hasattr(exc, 'code'):
        return str(exc)
    try:
        body_json = exc.response.body.decode()
        body = json.loads(body_json)
        error_message = body.get('errors', 'No error message provided')
    except (AttributeError, json.JSONDecodeError):
        error_message = 'Failed to decode error message from response'

    msg = custom_message or ''
    msg += f" - {error_message}" if msg else error_message
    msg = f"{exc.code} {SHOPIFY_API_RESPONSES.get(exc.code, {}).get('description', 'Unknown Error')} - {msg}"
    return msg
