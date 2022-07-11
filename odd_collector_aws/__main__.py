import asyncio
import logging
import os
from os import path

from odd_collector_sdk.collector import Collector

from odd_collector_aws.domain.plugin import AvailablePlugin

logging.basicConfig(
    level=os.getenv("LOGLEVEL", "INFO"),
    format="[%(asctime)s] %(levelname)s in %(name)s: %(message)s",
)

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()

        cur_dirname = path.dirname(path.realpath(__file__))
        config_path = path.join(cur_dirname, "../collector_config.yaml")
        root_package = "odd_collector_aws.adapters"

        collector = Collector(config_path, root_package, AvailablePlugin)

        loop.run_until_complete(collector.register_data_sources())

        collector.start_polling()

        asyncio.get_event_loop().run_forever()
    except Exception as e:
        logging.error(e, exc_info=True)
        asyncio.get_event_loop().stop()
