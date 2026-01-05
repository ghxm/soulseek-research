#!/usr/bin/env python3
"""
Simple test to check client.searches.received_searches attribute
"""

import asyncio
import logging
from aioslsk.client import SoulSeekClient
from aioslsk.settings import Settings, CredentialsSettings, UpnpSettings, NetworkSettings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    logger.info("="*70)
    logger.info("Testing client.searches.received_searches attribute")
    logger.info("="*70)

    # Disable UPnP since we don't need it on cloud servers
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
    logger.info("✅ Connected to Soulseek server")
    logger.info(f"📊 SearchManager type: {type(client.searches)}")
    logger.info(f"📊 Has received_searches: {hasattr(client.searches, 'received_searches')}")
    
    if hasattr(client.searches, 'received_searches'):
        logger.info(f"📊 received_searches type: {type(client.searches.received_searches)}")
        logger.info(f"📊 Initial count: {len(client.searches.received_searches)}")
    
    logger.info("\n👂 Listening for searches (will check every 10s for 2 minutes)...\n")

    for i in range(12):  # 12 * 10s = 2 minutes
        await asyncio.sleep(10)
        
        if hasattr(client.searches, 'received_searches'):
            searches = client.searches.received_searches
            logger.info(f"⏰ {(i+1)*10}s - {len(searches)} searches in received_searches")
            
            # Show the last few searches if any
            if len(searches) > 0:
                for idx, search in enumerate(list(searches)[-3:]):
                    logger.info(f"  🔍 Search: {search}")
        else:
            logger.info(f"⏰ {(i+1)*10}s - No received_searches attribute")

    logger.info("\n" + "="*70)
    if hasattr(client.searches, 'received_searches'):
        total = len(client.searches.received_searches)
        logger.info(f"RESULT: {total} searches received")
        if total > 0:
            logger.info("✅ SUCCESS: Searches are being received!")
        else:
            logger.info("❌ No searches received (network/config issue)")
    logger.info("="*70)

    await client.stop()

if __name__ == "__main__":
    asyncio.run(main())
