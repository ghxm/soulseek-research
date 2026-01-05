#!/usr/bin/env python3
"""Test with tiger2 credentials"""

import asyncio
import logging
from aioslsk.client import SoulSeekClient
from aioslsk.events import SearchRequestReceivedEvent
from aioslsk.settings import Settings, CredentialsSettings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

search_count = 0

async def on_search(event: SearchRequestReceivedEvent):
    global search_count
    search_count += 1
    logger.info(f"🎯 SEARCH #{search_count}: '{event.query[:40]}' from {event.username}")

async def main():
    logger.info("="*60)
    logger.info("Testing with tiger2 account")
    logger.info("="*60)

    settings = Settings(
        credentials=CredentialsSettings(
            username='tiger2',
            password='tigertigertiger'
        )
    )

    client = SoulSeekClient(settings)
    client.events.register(SearchRequestReceivedEvent, on_search)

    await client.start()
    logger.info("✅ Connected - listening for searches (3 min test)...")

    for i in range(6):
        await asyncio.sleep(30)
        logger.info(f"⏰ {(i+1)*30}s - {search_count} searches")

    logger.info(f"\n{'='*60}")
    logger.info(f"RESULT: {search_count} searches received")
    logger.info("="*60)

    await client.stop()
    return search_count

if __name__ == "__main__":
    asyncio.run(main())
