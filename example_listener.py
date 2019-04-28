#!/usr/bin/env python

import asyncio
from nats.aio.client import Client as NATS


async def run(loop):
    nc = NATS()

    await nc.connect("public:thenewalbiondata@www.albion-online-data.com:4222", loop=loop)

    async def goldprices_processor(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("[GOLD] '{subject} {reply}': {data}".format(subject=subject, reply=reply, data=data))

    async def mapdata_processor(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("[MAPDATA] '{subject} {reply}': {data}".format(subject=subject, reply=reply, data=data))

    async def marketorder_processor(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("[MARKET] '{subject} {reply}': {data}".format(subject=subject, reply=reply, data=data))

    # Simple publisher and async subscriber via coroutine.
    await nc.subscribe("marketorders.deduped", cb=marketorder_processor)
    await nc.subscribe("goldprices.deduped", cb=goldprices_processor)
    await nc.subscribe("mapdata.deduped", cb=mapdata_processor)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.run_forever()
