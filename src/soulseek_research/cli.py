"""Simple CLI for soulseek research - FIXED VERSION"""

import asyncio
import logging

import click

from .client import ResearchClient


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


# Archival commands removed - these should be handled by database server scripts


@cli.command() 
def version():
    """Show version information"""
    click.echo("soulseek-research 0.1.0")


def main():
    """Main CLI entry point"""
    cli()