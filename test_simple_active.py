#!/usr/bin/env python3
"""Simple test - perform search then monitor"""
import asyncio
import logging
from aioslsk.client import SoulSeekClient
from aioslsk.settings import Settings, CredentialsSettings, NetworkSettings, UpnpSettings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    logger.info("TEST: Search then monitor")
    
    settings = Settings(
        credentials=CredentialsSettings(username='tiger2', password='tigertigertiger'),
        network=NetworkSettings(upnp=UpnpSettings(enabled=False))
    )

    client = SoulSeekClient(settings)
    await client.start()
    logger.info("✅ Connected")
    
    # Try to perform a search
    logger.info("🔍 Searching for 'test'...")
    try:
        search_req = await client.searches.search('test')
        await asyncio.sleep(5)
        logger.info(f"Search got {len(search_req.results)} results")
    except Exception as e:
        logger.error(f"Search failed: {e}")
    
    # Monitor for 1 minute
    for i in range(2):
        await asyncio.sleep(30)
        received = len(client.searches.received_searches)
        logger.info(f"⏰ {(i+1)*30}s - received_searches: {received}")
    
    await client.stop()

if __name__ == "__main__":
    asyncio.run(main())
