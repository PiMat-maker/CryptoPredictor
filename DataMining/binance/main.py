import json
import logging
import os

from binance.manager.data_mining import DataMiningStreamManager


def main():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler("binance.log", mode='w')
    formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    end_time = None
    if os.path.exists('last_request_meta.json'):
        with open('last_request_meta.json', 'r') as meta_file:
            meta_info = json.load(meta_file)

        interval_int = DataMiningStreamManager.count_interval(
            meta_info['first_result']['open_time'],
            meta_info['first_result']['close_time']
        )
        end_time = meta_info['first_result']['open_time'] - interval_int

    mining_manager = DataMiningStreamManager("BTCUSDT", mining_limit=2000, logger=logger)
    mining_manager.init_klines("1m", end_time=end_time)
    mining_manager.run()


if __name__ == '__main__':
    main()
