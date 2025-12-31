"""Simple CLI for soulseek research"""

import asyncio
import datetime
import logging

import click

from .client import ResearchClient, SearchRecord, ArchiveRecord
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import func, select


@click.group()
@click.option('--log-level', default='INFO', help='Logging level')
def cli(log_level: str):
    """Soulseek Research CLI"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


@cli.command()
@click.option('--username', required=True, help='Soulseek username')
@click.option('--password', required=True, help='Soulseek password')  
@click.option('--database-url', required=True, help='Database connection URL')
@click.option('--client-id', help='Optional client ID')
@click.option('--encryption-key', help='Optional encryption key for usernames (uses ENCRYPTION_KEY env var if not provided)')
def start(username: str, password: str, database_url: str, client_id: str, encryption_key: str):
    """Start a research client"""
    
    async def run_client():
        client = ResearchClient(
            username=username,
            password=password,
            database_url=database_url,
            client_id=client_id,
            encryption_key=encryption_key
        )
        
        await client.start()
    
    click.echo(f"Starting research client for {username}")
    asyncio.run(run_client())


@cli.command()
@click.option('--database-url', required=True, help='Database connection URL')
@click.option('--year', type=int, help='Year to archive (defaults to last month)')
@click.option('--month', type=int, help='Month to archive (defaults to last month)')
@click.option('--archive-path', default='/tmp/archives', help='Archive storage path')
@click.option('--keep-in-db', is_flag=True, help='Keep data in main table after archiving')
def archive(database_url: str, year: int, month: int, archive_path: str, keep_in_db: bool):
    """Archive monthly search data to compressed files"""
    
    async def run_archive():
        # Default to last month if not specified
        if year is None or month is None:
            last_month = datetime.datetime.now() - datetime.timedelta(days=32)
            year = last_month.year
            month = last_month.month
        
        click.echo(f"Archiving data for {year}-{month:02d}")
        
        # Create a temporary client just for archiving
        client = ResearchClient("temp", "temp", database_url)
        client.engine = create_async_engine(database_url)
        client.session_maker = async_sessionmaker(client.engine)
        
        try:
            result = await client.archive_month(
                year=year,
                month=month, 
                archive_dir=archive_path,
                delete_after=not keep_in_db
            )
            
            if result:
                click.echo(f"✅ Archived to {result}")
                if not keep_in_db:
                    click.echo("🗑️ Deleted records from main table")
            else:
                click.echo("ℹ️ No data found to archive or already archived")
                
        finally:
            await client.engine.dispose()
    
    asyncio.run(run_archive())


@cli.command() 
@click.option('--database-url', required=True, help='Database connection URL')
def archive_status(database_url: str):
    """Show archive status and summary"""
    
    async def show_status():
        engine = create_async_engine(database_url)
        session_maker = async_sessionmaker(engine)
        
        try:
            async with session_maker() as session:
                # Get archives
                result = await session.execute(
                    select(ArchiveRecord).order_by(ArchiveRecord.month.desc())
                )
                archives = result.scalars().all()
                
                if not archives:
                    click.echo("No archives found")
                    return
                
                click.echo(f"{'Month':<10} {'Records':<12} {'Size (MB)':<10} {'Deleted':<8} {'Archived'}")
                click.echo("-" * 60)
                
                total_records = 0
                total_size_mb = 0
                
                for archive in archives:
                    size_mb = archive.file_size / (1024 * 1024)
                    deleted_icon = "✅" if archive.deleted else "❌"
                    click.echo(
                        f"{archive.month:<10} "
                        f"{archive.record_count:>11,} "
                        f"{size_mb:>9.1f} "
                        f"{deleted_icon:<8} "
                        f"{archive.archived_at.strftime('%Y-%m-%d')}"
                    )
                    total_records += archive.record_count
                    total_size_mb += size_mb
                
                click.echo("-" * 60)
                click.echo(f"Total: {total_records:,} records, {total_size_mb:.1f} MB archived")
            
        finally:
            await engine.dispose()
    
    asyncio.run(show_status())


@cli.command()
@click.option('--database-url', required=True, help='Database connection URL')
def stats(database_url: str):
    """Show current database stats"""
    
    async def show_stats():
        engine = create_async_engine(database_url)
        session_maker = async_sessionmaker(engine)
        
        try:
            async with session_maker() as session:
                # Count searches
                result = await session.execute(select(func.count(SearchRecord.id)))
                search_count = result.scalar() or 0
                
                # Count archives  
                result = await session.execute(select(func.count(ArchiveRecord.id)))
                archive_count = result.scalar() or 0
                
                click.echo(f"Searches in database: {search_count:,}")
                click.echo(f"Total archives: {archive_count}")
                
        finally:
            await engine.dispose()
    
    asyncio.run(show_stats())


@cli.command() 
def version():
    """Show version information"""
    click.echo("soulseek-research 0.1.0")


def main():
    """Main CLI entry point"""
    cli()