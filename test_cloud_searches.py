#!/usr/bin/env python3
"""
Cloud server test - check if searches are received on public server
Use sr26_de credentials for Germany server
"""

import asyncio
import logging
from aioslsk.client import SoulSeekClient
from aioslsk.settings import Settings, CredentialsSettings, UpnpSettings, NetworkSettings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    logger.info("="*70)
    logger.info("CLOUD SERVER TEST: Checking received_searches on public server")
    logger.info("="*70)

    # Disable UPnP - cloud server ports are already open
    settings = Settings(
        credentials=CredentialsSettings(
            username='sr26_de',
            password='4ThZWboLdNah4aVZHhMHCfR2uQHpmwAG'
        ),
        network=NetworkSettings(
            upnp=UpnpSettings(enabled=False)
        )
    )

    client = SoulSeekClient(settings)
    await client.start()
    logger.info("✅ Connected - monitoring received_searches")
    logger.info(f"📊 Type: {type(client.searches.received_searches)}\n")

    # Monitor for 5 minutes
    for i in range(10):  # 10 * 30s = 5 minutes
        await asyncio.sleep(30)
        
        searches = client.searches.received_searches
        logger.info(f"⏰ {(i+1)*30}s - {len(searches)} searches")
        
        # Show last 3 searches
        if len(searches) > 0:
            for search in list(searches)[-3:]:
                logger.info(f"  🔍 {search.query if hasattr(search, 'query') else search}")

    logger.info("\n" + "="*70)
    total = len(client.searches.received_searches)
    logger.info(f"FINAL: {total} searches received in 5 minutes")
    if total > 0:
        logger.info("✅ SUCCESS - searches ARE being received!")
    else:
        logger.info("❌ NO SEARCHES - need to investigate further")
    logger.info("="*70)

    await client.stop()
    return total

if __name__ == "__main__":
    count = asyncio.run(main())
    exit(0 if count > 0 else 1)
