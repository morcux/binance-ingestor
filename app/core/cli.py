import asyncio

import click
import uvicorn


@click.group()
def cli():
    pass


@cli.command()
def init_db():
    from app.db.clickhouse import db

    async def _init():
        print("Initializing Database...")
        await db.connect()
        try:
            with open("sql/init.sql", "r") as f:
                content = f.read()
                queries = content.split(";")
                for q in queries:
                    if q.strip():
                        await db.execute(q)
            print("Database initialized successfully")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await db.close()

    asyncio.run(_init())


@cli.command()
@click.option("--host", default="0.0.0.0", help="Host to bind")
@click.option("--port", default=8000, help="Port to bind")
@click.option("--reload", is_flag=True, help="Enable auto-reload")
def start_api(host, port, reload):
    uvicorn.run("app.main:app", host=host, port=port, reload=reload)


@cli.command()
def start_ingestor():
    from app.services.ingestor import IngestorService

    try:
        import uvloop

        uvloop.install()
    except ImportError:
        pass

    service = IngestorService()
    try:
        asyncio.run(service.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
