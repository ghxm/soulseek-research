#!/usr/bin/env python3
"""
CRITICAL TEST: Confirm search reception on cloud server
This is a minimal test - no database, just pure search counting
"""

import asyncio
import logging
from aioslsk.client import SoulSeekClient
from aioslsk.events import SearchRequestReceivedEvent
from aioslsk.settings import Settings, CredentialsSettings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

search_count = 0

async def on_search(event: SearchRequestReceivedEvent):
    """Count and log searches"""
    global search_count
    search_count += 1
    logger.info(f"🎯 SEARCH #{search_count}: '{event.query[:50]}' from {event.username[:15]}")

async def main():
    logger.info("="*70)
    logger.info("CRITICAL SEARCH RECEPTION TEST - NO DATABASE")
    logger.info("Testing if aioslsk receives searches on cloud server")
    logger.info("="*70)

    settings = Settings(
        credentials=CredentialsSettings(
            username='sr26_de',
            password='4ThZWboLdNah4aVZHhMHCfR2uQHpmwAG'
        )
    )

    client = SoulSeekClient(settings)
    client.events.register(SearchRequestReceivedEvent, on_search)

    logger.info("Starting Soulseek client...")
    await client.start()
    logger.info("✅ Connected to server")
    logger.info("\n👂 LISTENING FOR SEARCHES (will run for 5 minutes)...\n")

    # Run for 5 minutes with status updates every 30 seconds
    for elapsed in range(0, 300, 30):
        await asyncio.sleep(30)
        logger.info(f"⏰ {elapsed + 30}s - {search_count} searches received")

    logger.info("\n" + "="*70)
    if search_count > 0:
        logger.info(f"✅ SUCCESS: {search_count} searches received!")
        logger.info("✅ Search reception CONFIRMED - aioslsk is working")
    else:
        logger.info("❌ FAILED: No searches received after 5 minutes")
        logger.info("❌ This indicates a network/configuration issue")
    logger.info("="*70)

    await client.stop()
    return search_count

if __name__ == "__main__":
    count = asyncio.run(main())
    exit(0 if count > 0 else 1)
