import asyncio
import logging

from .domain.adapters_folder_meta import AdapterFolderMetadata
from .domain.collector import Collector
from .config import config

from os import path

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s"
)

try:
    cur_dirname = path.dirname(path.realpath(__file__))
    config_path = path.join(cur_dirname, "../collector_config.yaml")
    adapters_path = path.join(cur_dirname, "adapters")
    adapters_folder_metadata: AdapterFolderMetadata = AdapterFolderMetadata(
        adapters_path, "odd_collector.adapters"
    )

    collector = Collector(
        config_path, adapters_folder_metadata, config.platform_host_url
    )
    collector.start_polling()
    asyncio.get_event_loop().run_forever()
except Exception as e:
    logging.error(e)
    asyncio.get_event_loop().stop()
