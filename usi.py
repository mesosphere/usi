#!/usr/bin/env python3
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('usi')

import asyncio
import json

async def usi():
    logger.info("Starting USI cli...")

    proc = await asyncio.create_subprocess_exec(
        "cli-0.1/bin/cli",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)


    launch_pod = {
        "podId": { "value":"my_pod" },
        "runSpec": { "shellCommand":"sleep 3600", "role":"foo", "resourceRequirements": [], "fetch": []}
    }
    scheduler_command = "event:launchpod\ndata:{}\n\n".format(json.dumps(launch_pod)).encode('utf-8')
    proc.stdin.write(scheduler_command)
    await proc.stdin.drain()
    while True:
       logger.info(await proc.stdout.readline())

def main():
    asyncio.run(usi())

if __name__ == "__main__":
    main()