from binance.lib.utils import get_uuid, purge_map


def klines(self, symbol: str, interval: str, **kwargs):
    """Klines/candlesticks
    Args:
        symbol (str): symbol to get klines
        interval (str): interval of klines
    Keyword Args:
        startTime (int): start time to fetch from
        endTime (int): end time to fetch from
        limit (int): limit of klines
    Message sent:
    .. code-block:: json
        {
            "id": "5494febb-d167-46a2-996d-70533eb4d976",
            "method": "klines",
            "params": {
                "symbol": "BNBBTC",
                "interval": "1m",
                "startTime": 1655969280000,
                "limit": 1
            }
        }
    Response:
    .. code-block:: json
        {
            "id": "5494febb-d167-46a2-996d-70533eb4d976",
            "status": 200,
            "result": [
                [
                    1660009530807,
                    "0.01361000",
                    "0.01361000",
                    "0.01361000",
                    "0.01361000",
                    "0.01400000",
                    1660009530807,
                    "0.00019054",
                    0,
                    "0.00000000",
                    "0.00000000",
                    "0"
                ]
            ],
            "rateLimits": [
                {
                    "rateLimitType": "REQUEST_WEIGHT",
                    "interval": "MINUTE",
                    "intervalNum": 1,
                    "limit": 1200,
                    "count": 1
                }
            ]
        }
    """
    parameters = {"symbol": symbol.upper(), "interval": interval, **kwargs}

    parameters = purge_map(parameters)

    payload = {
        "id": parameters.pop("id", get_uuid()),
        "method": "klines",
        "params": parameters,
    }

    self.send(payload)
