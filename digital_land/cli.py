import sys
import click
import logging
from .load import load, load_csv, load_csv_dict
from .collect import Collector
from .save import save
from .normalise import Normaliser
from .map import Mapper
from .schema import Schema


@click.group()
@click.option("-d", "--debug/--no-debug", default=False)
def cli(debug):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


@cli.command(
    "collect",
    short_help="collect resources from sources into a digital-land collection",
)
@click.argument(
    "path", type=click.Path(exists=True), default="collection/source.csv",
)
def collect_cmd(path):
    """collect resources listed in the resource-url column of the path CSV file"""
    collector = Collector()
    collector.collect(path)


@cli.command("convert", short_help="convert to a well-formed, UTF-8 encoded CSV file")
@click.argument("input_path", type=click.Path(exists=True))
@click.argument("output_path", type=click.Path())
def convert_cmd(input_path, output_path):
    reader = load(input_path)
    if not reader:
        logging.error(f"Unable to convert {input_path}")
        sys.exit(2)
    save(reader, output_path)


@cli.command("normalise", short_help="removed padding, drop empty rows")
@click.argument("input_path", type=click.Path(exists=True))
@click.argument("output_path", type=click.Path())
@click.option(
    "--null-path",
    type=click.Path(exists=True),
    help="patterns for null fields",
    default=None,
)
@click.option(
    "--skip-path",
    type=click.Path(exists=True),
    help="patterns for skipped lines",
    default=None,
)
def normalise_cmd(input_path, output_path, null_path, skip_path):
    normaliser = Normaliser(null_path=null_path, skip_path=skip_path)
    stream = load_csv(input_path)
    stream = normaliser.normalise(stream)
    save(stream, output_path)


@cli.command("map", short_help="map misspelt column names to those in a schema")
@click.argument("input_path", type=click.Path(exists=True))
@click.argument("output_path", type=click.Path())
@click.argument("schema_path", type=click.Path(exists=True))
def map_cmd(input_path, output_path, schema_path):
    schema = Schema(schema_path)
    mapper = Mapper(schema)
    stream = load_csv_dict(input_path)
    stream = mapper.mapper(stream)
    save(stream, output_path, fieldnames=schema.fieldnames)
