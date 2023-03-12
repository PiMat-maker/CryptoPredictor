import time
import uuid
from collections import OrderedDict
from urllib.parse import urlencode
from binance.lib.authentication import hmac_hashing
from binance.error import (
    WebsocketClientError,
)


def get_timestamp():
    return int(time.time() * 1000)


def get_uuid():
    return str(uuid.uuid4())


def purge_map(map: map):
    """Remove None values from map"""
    return {k: v for k, v in map.items() if v is not None and v != "" and v != 0}


def websocket_api_signature(api_key: str, api_secret: str, parameters: dict):
    """Generate signature for websocket API
    Args:
        api_key (str): API key.
        api_secret (str): API secret.
        params (dict): Parameters.
    """

    if not api_key or not api_secret:
        raise WebsocketClientError(
            "api_key and api_secret are required for websocket API signature"
        )

    parameters["timestamp"] = get_timestamp()
    parameters["apiKey"] = api_key

    parameters = OrderedDict(sorted(parameters.items()))
    parameters["signature"] = hmac_hashing(api_secret, urlencode(parameters))

    return parameters