"""Simple Soulseek research client - FIXED VERSION"""

import asyncio
import base64
import datetime
import hashlib
import logging
import os
from collections import deque
from typing import Optional

from cryptography.fernet import Fernet

from aioslsk.client import SoulSeekClient
from aioslsk.events import SearchRequestReceivedEvent
from aioslsk.settings import Settings, CredentialsSettings

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from sqlalchemy import Integer, String, Text, DateTime, func

logger = logging.getLogger(__name__)

# Simple database models (no archival here)
Base = declarative_base()

class SearchRecord(Base):
    """Simple search record"""
    __tablename__ = 'searches'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    client_id: Mapped[str] = mapped_column(String(50), nullable=False)
    timestamp: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    username: Mapped[str] = mapped_column(String(255), nullable=False)
    query: Mapped[str] = mapped_column(Text, nullable=False)


class ResearchClient:
    """Simple research client - focus only on search collection"""
    
    def __init__(self, 
                 username: str,
                 password: str, 
                 database_url: str,
                 client_id: Optional[str] = None,
                 batch_size: int = 500,
                 encryption_key: Optional[str] = None):
        
        self.username = username
        self.password = password
        self.database_url = database_url
        self.client_id = client_id or f"client-{username}"
        self.batch_size = batch_size
        
        # Setup encryption for usernames
        self._setup_encryption(encryption_key)
        
        # Configure Settings for distributed network participation
        from aioslsk.settings import (
            SharesSettings, DirectoryShareMode, SharedDirectorySettingEntry,
            NetworkSettings, UpnpSettings
        )

        # Create a minimal share directory (empty is fine, we just need to show willingness to share)
        share_dir = "/tmp/soulseek_share"
        os.makedirs(share_dir, exist_ok=True)

        settings = Settings(
            credentials=CredentialsSettings(
                username=username,
                password=password
            ),
            shares=SharesSettings(
                directories=[
                    SharedDirectorySettingEntry(
                        path=share_dir,
                        share_mode=DirectoryShareMode.EVERYONE
                    )
                ],
                scan_on_start=True
            ),
            network=NetworkSettings(
                upnp=UpnpSettings(enabled=False)  # Disable UPnP for cloud servers
            )
        )
        
        # Soulseek client
        self.soulseek_client = SoulSeekClient(settings)
        
        # Database (will be setup when needed, not blocking startup)
        self.engine = None
        self.session_maker = None
        self._db_ready = False
        
        # Search queue for batching
        self._search_queue = deque()
        self._batch_task = None
        self._running = False
        
        # Stats
        self.searches_logged = 0
    
    def _setup_encryption(self, encryption_key: Optional[str] = None):
        """Setup username encryption"""
        if encryption_key:
            # Use provided key
            if len(encryption_key) == 32:
                # Assume it's already base64 encoded
                self.encryption_key = encryption_key.encode()
            else:
                # Hash the key to get proper length
                key_hash = hashlib.sha256(encryption_key.encode()).digest()
                self.encryption_key = base64.urlsafe_b64encode(key_hash)
        else:
            # Generate from environment or create deterministic key
            env_key = os.environ.get('ENCRYPTION_KEY')
            if env_key:
                key_hash = hashlib.sha256(env_key.encode()).digest()
                self.encryption_key = base64.urlsafe_b64encode(key_hash)
            else:
                # Use a deterministic key based on database URL (not secure, but consistent)
                logger.warning("No encryption key provided, using deterministic key. Set ENCRYPTION_KEY environment variable for security.")
                key_hash = hashlib.sha256(self.database_url.encode()).digest()
                self.encryption_key = base64.urlsafe_b64encode(key_hash)
        
        self.fernet = Fernet(self.encryption_key)
    
    def encrypt_username(self, username: str) -> str:
        """Encrypt username for storage"""
        try:
            encrypted_bytes = self.fernet.encrypt(username.encode())
            return base64.urlsafe_b64encode(encrypted_bytes).decode()
        except Exception as e:
            logger.error(f"Failed to encrypt username: {e}")
            # Return a hash as fallback
            return hashlib.sha256(username.encode()).hexdigest()
    
    async def _setup_database(self):
        """Setup database connection asynchronously after Soulseek starts"""
        if self._db_ready:
            return
            
        try:
            logger.info("Setting up database connection...")
            
            # Setup database
            self.engine = create_async_engine(self.database_url)
            self.session_maker = async_sessionmaker(self.engine)
            
            # Create tables
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            
            self._db_ready = True
            logger.info("✅ Database connection ready")
            
        except Exception as e:
            logger.error(f"❌ Failed to setup database: {e}")
            # Continue without database - better than crashing
            self._db_ready = False
    
    async def start(self):
        """Start the client - Soulseek first, database after"""
        logger.info(f"Starting research client {self.client_id}")
        
        # Subscribe to search events FIRST (before starting)
        self.soulseek_client.events.register(
            SearchRequestReceivedEvent,
            self._on_search_received
        )
        
        # Start soulseek FIRST - don't let database block this
        logger.info("Starting Soulseek client...")
        await self.soulseek_client.start()
        logger.info("✅ Soulseek client connected to server")

        # CRITICAL: Must login to join distributed network and receive searches
        logger.info("Logging in to Soulseek...")
        await self.soulseek_client.login()
        logger.info("✅ Logged in - distributed network active")
        
        # Now setup database connection asynchronously
        await self._setup_database()
        
        # Start batch processor
        self._running = True
        self._batch_task = asyncio.create_task(self._batch_worker())
        
        logger.info(f"Client {self.client_id} fully started")
        
        # Keep running
        try:
            heartbeat_counter = 0
            while self._running:
                await asyncio.sleep(1)
                heartbeat_counter += 1
                # Log heartbeat every 30 seconds for debugging
                if heartbeat_counter % 30 == 0:
                    received_count = len(self.soulseek_client.searches.received_searches)
                    logger.info(f"💓 Client {self.client_id} - logged: {self.searches_logged}, received_searches deque: {received_count}")
        except KeyboardInterrupt:
            pass
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the client"""
        logger.info(f"Stopping client {self.client_id}")
        self._running = False
        
        if self._batch_task:
            await self._batch_task
        
        # Flush remaining searches
        await self._flush_searches()
        
        # Stop soulseek
        await self.soulseek_client.stop()
        
        # Close database
        if self.engine:
            await self.engine.dispose()
    
    async def _on_search_received(self, event: SearchRequestReceivedEvent):
        """Handle search events"""
        logger.info(f"🔍 Search received: {event.query[:50]}... from {event.username[:10]}...")
        
        # Encrypt username for privacy
        encrypted_username = self.encrypt_username(event.username)
        
        search = SearchRecord(
            client_id=self.client_id,
            username=encrypted_username,
            query=event.query,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        self._search_queue.append(search)
        self.searches_logged += 1
        
        # Immediate flush if queue full
        if len(self._search_queue) >= self.batch_size:
            await self._flush_searches()
    
    async def _batch_worker(self):
        """Background worker to flush searches every 10 seconds"""
        while self._running:
            await asyncio.sleep(10)
            if self._search_queue:
                await self._flush_searches()
    
    async def _flush_searches(self):
        """Flush search queue to database"""
        if not self._search_queue or not self._db_ready:
            return
            
        batch = list(self._search_queue)
        self._search_queue.clear()
        
        try:
            async with self.session_maker() as session:
                session.add_all(batch)
                await session.commit()
                
            logger.debug(f"Saved {len(batch)} searches (total: {self.searches_logged})")
            
        except Exception as e:
            logger.error(f"Failed to save searches: {e}")
            # Put them back in queue
            self._search_queue.extendleft(reversed(batch))