import json
import logging
import os
import time

import pandas as pd
from binance.websocket.spot.websocket_api import SpotWebsocketAPIClient
from binance.error import (
    NotSuccessStatusError,
    RequestLimitExceededError,
)

mutex = 0


class DataMiningStreamManager:

    def __init__(
            self,
            symbol,
            logger=None,
            mining_limit=None
    ):
        """

        :param symbol(str): Mining symbol
        :param logger: Logger
        :param mining_limit(int): Needing amount of data
        """
        if not logger:
            logger = logging.getLogger(__name__)
        self.logger = logger
        self.symbol = symbol
        self.mining_limit = mining_limit

    def init_klines(
            self,
            interval: str,
            start_time: int = None,
            end_time: int = None,
            limit=1000
    ):
        """

        :param interval: Mining interval
        :param start_time: Mining start time
        :param end_time: Mining end time
        :param limit: Mining limit
        :return: None
        """
        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time
        self.limit = limit

    @staticmethod
    def message_handler(_, message):
        global mutex
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler("binance_msg.log", mode='w')
        formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.debug(message)

        meta_info = {}
        raw_data = json.loads(message)
        meta_info['status'] = raw_data['status']
        meta_info['rateLimit'] = raw_data['rateLimits'][0]
        if raw_data['status'] != 200:
            DataMiningStreamManager.write_meta_file(meta_info)
            mutex = 1
            return

        data = DataMiningStreamManager.collect_result(raw_data['result'])
        df = pd.DataFrame(data)
        meta_info['first_result'] = data[0]

        output_path = os.path.join(os.getcwd(), 'binance_data.csv')
        df.to_csv(output_path, index=False, mode='a', header=not os.path.exists(output_path))
        meta_info['result_len'] = df.shape[0]
        DataMiningStreamManager.write_meta_file(meta_info)
        mutex = 1

    @staticmethod
    def write_meta_file(meta_info:dict):
        with open("last_request_meta.json", 'w+') as meta_file:
            meta = json.dumps(meta_info)
            meta_file.write(meta)

    @staticmethod
    def collect_result(data: list[list]) -> list[dict]:
        """
        Change from response result view to result dict view
        Example:
            data - [[1678438320000, '19948.26000000', '19957.08000000', '19947.39000000', '19950.61000000',
             '189.22216000', 1678438379999, '3775399.81635490', 5471, '116.40893000', '2322643.69255990', '0']]

            dict_data - [{'open_time':1678438320000, 'open_price':19948.26000000, 'high_price':19957.08000000,
             'low_price':19947.39000000, 'close_price':19950.61000000, 'volume':189.22216000,
              'close_time':1678438379999, 'quote_asset_volume':3775399.81635490, 'trades_number':5471,
               'taker_buy_base_asset_volume':116.40893000, 'taker_buy_quote_asset_volume':2322643.69255990}]

        :param data: Raw result data from Stream API response
        :return: dict_data(list): Collected result data into dict
        """
        dict_data = [{
            'open_time': row[0], 'open_price': float(row[1]), 'high_price': float(row[2]),
            'low_price': float(row[3]), 'close_price': float(row[4]), 'volume': float(row[5]),
            'close_time': row[6], 'quote_asset_volume': float(row[7]), 'trades_number': row[8],
            'taker_buy_base_asset_volume': float(row[9]), 'taker_buy_quote_asset_volume': float(row[10])
        } for row in data]
        return dict_data

    def run(self):
        global mutex
        my_client = SpotWebsocketAPIClient(on_message=self.message_handler)
        start_time = self.start_time
        end_time = self.end_time
        limit = self.limit

        amount_data_received = 0
        while amount_data_received < self.mining_limit:
            mutex = 0
            if self.mining_limit - amount_data_received < self.limit:
                limit = self.mining_limit - amount_data_received

            self.logger.debug(f"CURRENT END TIME: {end_time}")
            my_client.klines(
                symbol=self.symbol,
                interval=self.interval,
                startTime=start_time,
                endTime=end_time,
                limit=limit
            )

            while not mutex:
                pass

            with open('last_request_meta.json', 'r') as meta_file:
                meta_info = json.load(meta_file)

            self.logger.debug(f"META {meta_info}")
            try:
                if meta_info['status'] != 200:
                    if meta_info['status'] == 418 or meta_info['status'] == 429:
                        raise RequestLimitExceededError(
                            f"Request limit {meta_info['rateLimit']['limit']} exceeded. Your count is {meta_info['rateLimit']['count']}"
                        )
                    raise NotSuccessStatusError(f"Response status is {meta_info['status']}")
                interval_int = self.count_interval(
                    meta_info['first_result']['open_time'],
                    meta_info['first_result']['close_time']
                )
                end_time = meta_info['first_result']['open_time'] - interval_int
                amount_data_received += meta_info['result_len']
            except RequestLimitExceededError as limit_exceed_error:
                self.logger.error(limit_exceed_error)
                break
            except Exception as e:
                self.logger.error(e)

        time.sleep(5)
        self.logger.info("closing ws connection")
        my_client.stop()

    @staticmethod
    def count_interval(open_time: int, close_time: int) -> int:
        """

        :param open_time: Open candle time in ms
        :param close_time: Close candle time in ms
        :return: time: Interval in ms
        """
        return close_time - open_time + 1
