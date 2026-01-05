#!/usr/bin/env python3
"""
Test if performing a search activates distributed network and causes
us to start receiving search requests from others
"""

import asyncio
import logging
from aioslsk.client import SoulSeekClient
from aioslsk.settings import Settings, CredentialsSettings, NetworkSettings, UpnpSettings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    logger.info("="*70)
    logger.info("TEST: Perform search to activate distributed network")
    logger.info("="*70)

    settings = Settings(
        credentials=CredentialsSettings(
            username='tiger2',
            password='tigertigertiger'
        ),
        network=NetworkSettings(
            upnp=UpnpSettings(enabled=False)
        )
    )

    client = SoulSeekClient(settings)
    await client.start()
    logger.info("✅ Connected to server")
    
    # Perform a search to potentially activate distributed network
    logger.info("\n🔍 Performing search for 'music' to activate network...")
    search_req = await client.searches.search('music', searchers=3)
    await asyncio.sleep(10)
    logger.info(f"📊 Search returned {len(search_req.results)} results")
    
    # Now monitor received_searches
    logger.info("\n👂 Monitoring received_searches for 2 minutes...")
    for i in range(4):
        await asyncio.sleep(30)
        received = len(client.searches.received_searches)
        logger.info(f"⏰ {(i+1)*30}s - {received} searches in received_searches")
        if received > 0:
            for search in list(client.searches.received_searches)[-3:]:
                logger.info(f"  🔍 {search}")
    
    total = len(client.searches.received_searches)
    logger.info(f"\n{'='*70}")
    logger.info(f"RESULT: {total} searches received")
    logger.info("="*70)
    
    await client.stop()
    return total

if __name__ == "__main__":
    count = asyncio.run(main())
    exit(0 if count > 0 else 1)
