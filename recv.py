import os
import asyncio
from datetime import date, datetime
import json
from azure.eventhub.aio import EventHubProducerClient  # The package name suffixed with ".aio" for async
from azure.eventhub import EventData
from logging import getLogger,config
import yaml
import send

from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import (
    BlobCheckpointStore,
)

from dotenv import load_dotenv
load_dotenv()

BLOB_STORAGE_CONNECTION_STRING = os.environ['BLOB_STORAGE_CONNECTION_STRING']
BLOB_CONTAINER_NAME = os.environ['BLOB_CONTAINER_NAME']
EVENT_HUB_CONNECTION_STR = os.environ['EVENT_HUB_CONNECTION_STR']
EVENT_HUB_NAME = os.environ['EVENT_HUB_NAME']

#定義ファイルの読み込み
yaml_path = os.path.join(os.path.dirname(__file__), 'logger.yaml')
with open(yaml_path, 'r') as yml:
    dic= yaml.safe_load(yml)

#定義ファイルを使ったloggingの設定
config.dictConfig(dic)

async def on_event(partition_context, event):
    logger = getLogger(__name__)
    # Print the event data.
    logger.debug(
        'Received the event: "{}" from the partition with ID: "{}"'.format(
            event.body_as_str(encoding="UTF-8"), partition_context.partition_id
        )
    )

    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    await partition_context.update_checkpoint(event)

async def main(loop, log):
    loop.create_task(send.send(log))
    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore.from_connection_string(
        BLOB_STORAGE_CONNECTION_STRING, BLOB_CONTAINER_NAME
    )

    # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string(
        EVENT_HUB_CONNECTION_STR,
        consumer_group="$Default",
        eventhub_name=EVENT_HUB_NAME,
        checkpoint_store=checkpoint_store,
    )
    async with client:
        # Call the receive method. Read from the beginning of the
        # partition (starting_position: "-1")
        await client.receive(on_event=on_event, starting_position="-1")

if __name__ == "__main__":
    #ロガーの取得
    logger = getLogger(__name__)
    # Run the main method.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main(loop, logger))