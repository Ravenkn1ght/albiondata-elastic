#!/usr/bin/env python

import asyncio
import configparser
import elasticsearch
import json
import traceback
from datetime import datetime
from pprint import pprint
from nats.aio.client import Client


def build_item_lookup(item_data_file='data/items.json'):
    lookup_dict = {}

    with open(item_data_file) as data_file:
        json_data = json.load(data_file)

        for idata in json_data:
            if idata['LocalizedNames']:
                lookup_dict[idata['UniqueName']] = [x for x in idata['LocalizedNames'] if x['Key'] == 'EN-US'][0]['Value']
            else:
                lookup_dict[idata['UniqueName']] = "NO_VALUE"

    return lookup_dict


def build_location_lookup(item_data_file='data/world.json'):
    lookup_dict = {}

    with open(item_data_file) as data_file:
        json_data = json.load(data_file)

        for wdata in json_data:
            lookup_dict[wdata['Index']] = wdata['UniqueName']

    return lookup_dict


async def run(loop):
    # load the config
    config = configparser.RawConfigParser()
    config.read('config.conf')

    item_lookup = build_item_lookup()
    world_lookup = build_location_lookup()
    elastic = elasticsearch.Elasticsearch([config.get('albiondata', 'elastic_url')])
    nc = Client()

    await nc.connect("public:thenewalbiondata@www.albion-online-data.com:4222", loop=loop)

    async def goldprices_processor(msg):
        data = msg.data.decode()

        try:
            json_data = json.loads(data)
            # pprint(json_data)

        except:
            print(traceback.format_exc())

    async def mapdata_processor(msg):
        data = msg.data.decode()

        try:
            json_data = json.loads(data)
            # pprint(json_data)

        except:
            print(traceback.format_exc())

    async def marketorder_processor(msg):
        data = msg.data.decode()

        try:
            market_data = json.loads(data)

            # get the english name
            if market_data['ItemTypeId'] in item_lookup.keys():
                market_data['ItemName'] = item_lookup[market_data['ItemTypeId']]
            else:
                market_data['ItemName'] = 'NO_VALUE'

            # get the location as a normal string
            if str(market_data['LocationId']) in world_lookup.keys():
                market_data['LocationName'] = world_lookup[str(market_data['LocationId'])]
            else:
                market_data['LocationName'] = 'NO_VALUE'

            # add the timestamp
            market_data['timestamp'] = datetime.utcnow()

            # calcuate the index name
            index_name = f"albion-marketorders-{datetime.now().strftime('%Y-%m')}"

            # index the data to elasticsearch
            elastic.index(index=index_name, doc_type='marketorder', body=market_data)

        except:
            print(traceback.format_exc())

    # Simple publisher and async subscriber via coroutine.
    await nc.subscribe("marketorders.deduped", cb=marketorder_processor)
    await nc.subscribe("goldprices.deduped", cb=goldprices_processor)
    await nc.subscribe("mapdata.deduped", cb=mapdata_processor)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.run_forever()
