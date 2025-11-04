#!/usr/bin/env python
import os
import asyncio
import random
from datetime import date, datetime
import json
from azure.eventhub.aio import EventHubProducerClient  # The package name suffixed with ".aio" for async
from azure.eventhub import EventData
from logging import getLogger,config
import yaml
from dotenv import load_dotenv

#環境変数(.env)の読み込み
load_dotenv()
 
#定義ファイルの読み込み
yaml_path = os.path.join(os.path.dirname(__file__), 'logger.yaml')
with open(yaml_path, 'r') as yml:
    dic= yaml.safe_load(yml)

#定義ファイルを使ったloggingの設定
config.dictConfig(dic)

connection_str = os.environ['EVENT_HUB_CONNECTION_STR']
# consumer_group = '<< CONSUMER GROUP >>' #not use
eventhub_name = os.environ['EVENT_HUB_NAME']
batch_size = int(os.environ['batch_size'])
delay_time = float(os.environ['delay_time'])

def json_serial(obj):

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError (f'Type {obj} not serializable')

async def create_batch(client, logger):
    event_data_batch = await client.create_batch()
    for i in range(batch_size):
        try:
            sensorid = random.randint(a=1, b=3)
            temperature = random.uniform(a=0, b=50) % 100
            humidity = random.uniform(a=0, b=100)
            status = "OK" if (40 < temperature or 30 >= humidity) else "NG"
            senddatetime = datetime.now()
            payload = json.dumps({'sensor_id': sensorid, 'sensor_temp': temperature,'sensor_humidity': humidity ,'sensor_status': status, 'sensor_sentdatetime': senddatetime}, default=json_serial)
            logger.info(f'"payload={payload}"')

            event_data_batch.add(EventData(payload))
        except ValueError as e:
            print(e)
            logger.error(f'ValueError: {e}')
            raise e
        except Exception as e:
            print(e)
            logger.error(f'Exception: {e}')
            raise e
    return event_data_batch

async def send(logger):
    client = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name)
    async with client:
        can_send = True
        while can_send:
            try:
                batch_data = await create_batch(client, logger)
                await client.send_batch(batch_data)
                await asyncio.sleep(delay_time)
            except Exception as e:
                logger.error(f'Exception during send: {e}')
                can_send = False

if __name__ == '__main__':
    #ロガーの取得
    logger = getLogger(__name__)
    asyncio.run(send(logger))