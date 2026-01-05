#!/usr/bin/env python3
"""
Simple test to confirm search reception works
This runs the client for 2 minutes and counts searches
"""

import asyncio
import logging
import signal
import sys

# Import aioslsk directly
from aioslsk.client import SoulSeekClient
from aioslsk.events import SearchRequestReceivedEvent
from aioslsk.settings import Settings, CredentialsSettings

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global counter
search_count = 0
client = None

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    logger.info(f"\n🛑 Interrupted - Total searches received: {search_count}")
    if client:
        asyncio.create_task(client.stop())
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

async def on_search_received(event: SearchRequestReceivedEvent):
    """Handle search events"""
    global search_count
    search_count += 1
    logger.info(f"🔍 SEARCH #{search_count}: '{event.query[:40]}...' from '{event.username[:10]}...'")

async def main():
    """Test search reception"""
    global client

    logger.info("="*60)
    logger.info("🎯 CRITICAL TEST: Confirming aioslsk search reception")
    logger.info("="*60)

    # Create simple aioslsk client
    settings = Settings(
        credentials=CredentialsSettings(
            username='sr_reception_test',
            password='test123reception'
        )
    )

    client = SoulSeekClient(settings)

    # Register search event handler
    client.events.register(SearchRequestReceivedEvent, on_search_received)

    logger.info("Starting Soulseek client...")
    await client.start()
    logger.info("✅ Soulseek client connected to server")

    logger.info("\n👂 LISTENING FOR SEARCHES...")
    logger.info("⏰ Will run for 2 minutes (120 seconds)")
    logger.info("🚨 Press Ctrl+C to stop early\n")

    # Wait for 2 minutes, logging every 10 seconds
    for elapsed in range(0, 120, 10):
        await asyncio.sleep(10)
        logger.info(f"⏰ {elapsed + 10}s elapsed - {search_count} searches received so far")

    logger.info("\n" + "="*60)
    if search_count > 0:
        logger.info(f"✅ SUCCESS: Received {search_count} searches!")
        logger.info("✅ Search reception is WORKING - safe to deploy")
    else:
        logger.info(f"❌ CRITICAL: No searches received after 2 minutes")
        logger.info("❌ DO NOT DEPLOY - need to investigate why searches aren't coming in")
        logger.info("❌ Possible causes:")
        logger.info("   - UPnP port mapping failed (check logs above)")
        logger.info("   - Router blocking incoming connections")
        logger.info("   - Need to test in cloud environment with proper port forwarding")
    logger.info("="*60)

    await client.stop()
    return search_count > 0

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
