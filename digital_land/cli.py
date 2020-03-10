import sys
import click
import logging
from .pipeline.load import load, load_csv
from .pipeline.save import save
from .pipeline.normalise import Normaliser


@click.group()
@click.option("-d", "--debug/--no-debug", default=False)
def cli(debug):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


@cli.command("convert", short_help="convert to a well-formed, UTF-8 encoded CSV file")
@click.argument("input_path", type=click.Path(exists=True))
@click.argument("output_path", type=click.Path())
def convert_cmd(input_path, output_path):
    reader = load(input_path)
    if not reader:
        logging.error(f"Unable to convert {input_path}")
        sys.exit(2)
    save(reader, output_path)


@cli.command("normalise", short_help="removed padding, drop obviously spurious rows")
@click.argument("input_path", type=click.Path(exists=True))
@click.argument("output_path", type=click.Path())
def normalise_cmd(input_path, output_path):
    normaliser = Normaliser()
    stream = load_csv(input_path)
    stream = normaliser.normalise(stream)
    save(stream, output_path)
