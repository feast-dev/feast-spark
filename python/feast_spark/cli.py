import click

from feast_spark.job_service import start_job_service


@click.group()
def cli():
    pass


@cli.command(name="server")
def server():
    """
    Start Feast Job Service
    """
    start_job_service()


if __name__ == "__main__":
    cli()
