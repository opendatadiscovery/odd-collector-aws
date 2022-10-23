import asyncio
import logging
import os
from os import path

from odd_collector_sdk.collector import Collector

from odd_collector_aws.domain.plugin import PLUGIN_FACTORY

logging.basicConfig(
    level=os.getenv("LOGLEVEL", "INFO"),
    format="[%(asctime)s] %(levelname)s in %(name)s: %(message)s",
)
logger = logging.getLogger("odd-collector-aws")

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()

        cur_dirname = path.dirname(path.realpath(__file__))
        config_path = os.getenv("CONFIG_PATH", path.join(cur_dirname, "../collector_config.yaml"))
        root_package = "odd_collector_aws.adapters"

        collector = Collector(config_path, root_package, PLUGIN_FACTORY)

        loop.run_until_complete(collector.register_data_sources())

        collector.start_polling()

        asyncio.get_event_loop().run_forever()
    except Exception as e:
        logger.error(e)
        asyncio.get_event_loop().stop()
