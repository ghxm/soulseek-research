"""Simple Soulseek research client"""

import asyncio
import base64
import datetime
import gzip
import hashlib
import logging
import os
import tempfile
from collections import deque
from pathlib import Path
from typing import Optional, Dict, Any

from cryptography.fernet import Fernet

from aioslsk.client import SoulSeekClient
from aioslsk.events import SearchRequestReceivedEvent
from aioslsk.settings import Settings

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from sqlalchemy import Integer, String, Text, DateTime, Boolean, func, select, delete

logger = logging.getLogger(__name__)

# Simple database models
Base = declarative_base()

class SearchRecord(Base):
    """Simple search record"""
    __tablename__ = 'searches'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    client_id: Mapped[str] = mapped_column(String(50), nullable=False)
    timestamp: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    username: Mapped[str] = mapped_column(String(255), nullable=False)
    query: Mapped[str] = mapped_column(Text, nullable=False)

class ArchiveRecord(Base):
    """Archive tracking"""
    __tablename__ = 'archives'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    month: Mapped[str] = mapped_column(String(7), nullable=False)  # YYYY-MM
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    record_count: Mapped[int] = mapped_column(Integer, nullable=False)
    file_size: Mapped[int] = mapped_column(Integer, nullable=False)
    archived_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    deleted: Mapped[bool] = mapped_column(Boolean, default=False)


class ResearchClient:
    """Simple research client for collecting Soulseek searches"""
    
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
        
        # Create aioslsk settings with manual configuration
        settings = Settings(
            credentials={
                'username': username,
                'password': password
            },
            network={'port': 60000}
        )
        
        # Soulseek client
        self.soulseek_client = SoulSeekClient(settings)
        
        # Database
        self.engine = None
        self.session_maker = None
        
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
    
    def decrypt_username(self, encrypted_username: str) -> str:
        """Decrypt username (for analysis/debugging only)"""
        try:
            encrypted_bytes = base64.urlsafe_b64decode(encrypted_username.encode())
            decrypted_bytes = self.fernet.decrypt(encrypted_bytes)
            return decrypted_bytes.decode()
        except Exception as e:
            logger.error(f"Failed to decrypt username: {e}")
            return "<encrypted>"
    
    async def start(self):
        """Start the client"""
        logger.info(f"Starting research client {self.client_id}")
        
        # Setup database
        self.engine = create_async_engine(self.database_url)
        self.session_maker = async_sessionmaker(self.engine)
        
        # Create tables
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        
        # Subscribe to search events
        self.soulseek_client.events.register(
            SearchRequestReceivedEvent,
            self._on_search_received
        )
        
        # Start soulseek
        await self.soulseek_client.start()
        
        # Start batch processor
        self._running = True
        self._batch_task = asyncio.create_task(self._batch_worker())
        
        logger.info(f"Client {self.client_id} started")
        
        # Keep running
        try:
            heartbeat_counter = 0
            while self._running:
                await asyncio.sleep(1)
                heartbeat_counter += 1
                # Log heartbeat every 5 minutes (300 seconds) 
                if heartbeat_counter % 300 == 0:
                    logger.info(f"💓 Client {self.client_id} heartbeat - {self.searches_logged} searches logged")
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
        if not self._search_queue:
            return
            
        batch = list(self._search_queue)
        self._search_queue.clear()
        
        try:
            async with self.session_maker() as session:
                session.add_all(batch)
                await session.commit()
                
            self.searches_logged += len(batch)
            logger.debug(f"Saved {len(batch)} searches (total: {self.searches_logged})")
            
        except Exception as e:
            logger.error(f"Failed to save searches: {e}")
            # Put them back in queue
            self._search_queue.extendleft(reversed(batch))
    
    async def archive_month(self, year: int, month: int, 
                           archive_dir: str = "/tmp/archives", 
                           delete_after: bool = True) -> Optional[str]:
        """Archive a month of data to compressed file"""
        
        # Date boundaries
        start_date = datetime.datetime(year, month, 1, tzinfo=datetime.timezone.utc)
        if month == 12:
            end_date = datetime.datetime(year + 1, 1, 1, tzinfo=datetime.timezone.utc)
        else:
            end_date = datetime.datetime(year, month + 1, 1, tzinfo=datetime.timezone.utc)
        
        month_str = f"{year:04d}-{month:02d}"
        
        # Check if already archived
        async with self.session_maker() as session:
            result = await session.execute(
                select(ArchiveRecord).where(ArchiveRecord.month == month_str)
            )
            if result.scalar_one_or_none():
                logger.info(f"Month {month_str} already archived")
                return None
        
        # Count records
        async with self.session_maker() as session:
            result = await session.execute(
                select(func.count(SearchRecord.id)).where(
                    SearchRecord.timestamp >= start_date,
                    SearchRecord.timestamp < end_date
                )
            )
            record_count = result.scalar() or 0
        
        if record_count == 0:
            logger.info(f"No records found for {month_str}")
            return None
        
        # Export to compressed file
        Path(archive_dir).mkdir(parents=True, exist_ok=True)
        archive_path = Path(archive_dir) / f"searches_{month_str}.csv.gz"
        
        # Create temporary CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
            
            # Export data
            async with self.session_maker() as session:
                result = await session.execute(
                    select(SearchRecord).where(
                        SearchRecord.timestamp >= start_date,
                        SearchRecord.timestamp < end_date
                    ).order_by(SearchRecord.timestamp)
                )
                
                # Write header
                temp_file.write("client_id,timestamp,username,query\\n")
                
                # Write data
                for record in result.scalars():
                    # Escape quotes in query
                    query_escaped = record.query.replace('"', '""')
                    temp_file.write(f'{record.client_id},{record.timestamp.isoformat()},{record.username},"{query_escaped}"\\n')
        
        # Compress
        with open(temp_path, 'rb') as f_in:
            with gzip.open(archive_path, 'wb') as f_out:
                f_out.writelines(f_in)
        
        temp_path.unlink()  # Delete temp file
        
        file_size = archive_path.stat().st_size
        
        # Save archive record
        archive_record = ArchiveRecord(
            month=month_str,
            file_path=str(archive_path),
            record_count=record_count,
            file_size=file_size
        )
        
        async with self.session_maker() as session:
            session.add(archive_record)
            await session.commit()
        
        logger.info(f"Archived {record_count:,} records to {archive_path} ({file_size:,} bytes)")
        
        # Delete from main table if requested
        if delete_after:
            async with self.session_maker() as session:
                await session.execute(
                    delete(SearchRecord).where(
                        SearchRecord.timestamp >= start_date,
                        SearchRecord.timestamp < end_date
                    )
                )
                await session.commit()
                
                # Mark as deleted
                archive_record.deleted = True
                session.add(archive_record)
                await session.commit()
            
            logger.info(f"Deleted {record_count:,} records from main table")
        
        return str(archive_path)
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get client stats"""
        async with self.session_maker() as session:
            result = await session.execute(select(func.count(SearchRecord.id)))
            total_in_db = result.scalar() or 0
            
            result = await session.execute(select(func.count(ArchiveRecord.id)))
            total_archives = result.scalar() or 0
        
        return {
            "client_id": self.client_id,
            "running": self._running,
            "searches_logged": self.searches_logged,
            "pending_batch": len(self._search_queue),
            "total_in_database": total_in_db,
            "total_archives": total_archives
        }