import csv
import logging
import os
import shutil
import pandas as pd

from collections import defaultdict

from digital_land.collect import Collector
from digital_land.collection import Collection, ResourceLogStore
from digital_land.commands import resource_from_path
from digital_land.log import DatasetResourceLog, ColumnFieldLog, IssueLog

from digital_land.phase.concat import ConcatFieldPhase
from digital_land.phase.convert import ConvertPhase
from digital_land.phase.default import DefaultPhase
from digital_land.phase.filter import FilterPhase
from digital_land.phase.harmonise import HarmonisePhase
from digital_land.phase.lookup import key
from digital_land.phase.map import MapPhase
from digital_land.phase.migrate import MigratePhase
from digital_land.phase.normalise import NormalisePhase
from digital_land.phase.organisation import OrganisationPhase
from digital_land.phase.parse import ParsePhase
from digital_land.phase.patch import PatchPhase
from digital_land.phase.phase import Phase
from digital_land.phase.prefix import EntityPrefixPhase
from digital_land.phase.prune import FieldPrunePhase
from digital_land.phase.reference import EntityReferencePhase

from digital_land.pipeline import run_pipeline
from digital_land.organisation import Organisation
from digital_land.schema import Schema
from digital_land.store.item import ItemStore
from digital_land.register import hash_value
from pathlib import Path


# =====================================================================
# Main Process Tasks
# =====================================================================
def task_preprocess(ctx):
    """
    preparatory steps to tidy up previous runs,
    and populate the context
    :param ctx:
    :return:
    """

    collection_dir = ctx.obj["COLLECTION_DIR"]
    root_coll_dir = collection_dir.parent
    datasource_log_dir = root_coll_dir / "log"
    issues_log_dir = root_coll_dir / "log"
    log_dir = collection_dir / "log"
    tmp_dir = root_coll_dir / "tmp"
    collection_resource_dir = collection_dir / "resource"

    ctx.obj["PIPELINE_DIR"] = Path(ctx.obj["PIPELINE"].path)
    ctx.obj["DATASOURCE_LOG_DIR"] = datasource_log_dir
    ctx.obj["ISSUES_LOG_DIR"] = issues_log_dir
    ctx.obj["LOG_DIR"] = log_dir
    ctx.obj["TMP_DIR"] = tmp_dir
    ctx.obj["COLLECTION_RESOURCE_DIR"] = collection_resource_dir

    if log_dir.is_dir():
        shutil.rmtree(log_dir)

    try:
        os.mkdir(log_dir)
        os.mkdir(tmp_dir)
    except OSError:
        pass


def task_create_source_and_endpoint_entries(ctx):
    """
    appends entries to both source.csv and endpoints.csv from
    the records in the csv at csv_file_path
    :param ctx:
    :return:
    """

    csv_file_path = ctx.obj["CSV_FILE_PATH"]
    collection_dir = ctx.obj["COLLECTION_DIR"].absolute()

    expected_cols = [
        "dataset",
        "organisation",
        "name",
        "documentation-url",
        "endpoint-url",
        "start-date",
    ]

    collection = Collection()
    ctx.obj["COLLECTION"] = collection

    collection.name = ctx.obj["DATASET"]
    collection.directory = collection_dir
    collection.load()

    # read and process each record of the new endpoints csv at csv_file_path
    line_number = 2
    with open(csv_file_path) as new_endpoints_file:
        reader = csv.DictReader(new_endpoints_file)
        csv_columns = reader.fieldnames

        # validate the columns
        for expected_col in expected_cols:
            if expected_col not in csv_columns:
                raise Exception(f"required column ({expected_col}) not found in csv")

        for row in reader:
            param_dataset = row["dataset"]
            param_endpoint_url = row["endpoint-url"]
            param_documentation_url = row["documentation-url"]
            param_organisation_name = row["organisation"]
            param_reference = row["name"]
            param_start_date = row["start-date"]

            if not ctx.obj["PIPELINE"].name:
                ctx.obj["PIPELINE"].name = param_dataset

            # validate csv file data
            if not collection.validate.organisation_name(param_organisation_name):
                err_msg = f"ERROR: line {line_number} - organisation name ({param_organisation_name}) is not valid"
                logging.error(err_msg)
                raise ValueError(err_msg)
            if not collection.validate.reference(param_reference):
                err_msg = f"ERROR: line {line_number} - reference ({param_reference}) is not valid"
                logging.error(err_msg)
                raise ValueError(err_msg)
            if not collection.validate.endpoint_url(param_endpoint_url):
                err_msg = f"ERROR: line {line_number} - endpoint ({param_endpoint_url}) is not valid"
                logging.error(err_msg)
                raise ValueError(err_msg)

            # transform date format
            str_date_fmt = collection.format_date(param_start_date)

            # prepare row for processing
            entry = defaultdict(str)
            entry["collection"] = param_dataset
            entry["organisation"] = param_organisation_name
            entry["licence"] = param_reference
            entry["attribution"] = param_reference
            entry["documentation-url"] = param_documentation_url
            entry["start-date"] = str_date_fmt
            entry["end-date"] = ""
            entry["pipelines"] = param_dataset

            # process valid row entries
            collection.add_source_endpoint(entry, param_endpoint_url)

            line_number += 1

    return


def task_collect_resources(ctx):
    collection = ctx.obj["COLLECTION"]
    pipeline = ctx.obj["PIPELINE"]
    collection_dir = ctx.obj["COLLECTION_DIR"]

    collector = Collector(pipeline.name, collection_dir)

    for endpoint_item in collection.new_endpoints:
        endpoint_url = endpoint_item["endpoint-url"]
        endpoint = endpoint_item["endpoint"]
        end_date = endpoint_item["end-date"]
        plugin = endpoint_item["plugin"]

        collector.fetch(
            endpoint_url,
            endpoint=endpoint,
            end_date=end_date,
            plugin=plugin,
        )


def task_populate_resource_and_log_csvs(ctx):
    collection = ctx.obj["COLLECTION"]
    logs_path = ctx.obj["COLLECTION_DIR"] / "log/*/"

    collection.log = CollectionLogStore(Schema("log"))
    collection.log.load(directory=logs_path)

    collection.resource = ResourceLogStore(Schema("resource"))
    collection.resource.load(
        log=collection.log, source=collection.source, directory=None
    )

    collection.log.save_csv(directory=collection.directory)
    collection.resource.save_csv(directory=collection.directory)


def task_generate_lookup_entries(ctx):
    """
    uses the dataset entity ID ranges declared in specification, and collected
    resources to generate new entries for the pipeline's lookup.csv
    :param ctx:
    :return:
    """

    pipeline_dir = ctx.obj["PIPELINE_DIR"]
    pipeline = ctx.obj["PIPELINE"]
    dataset_name = pipeline.name
    collection = ctx.obj["COLLECTION"]
    collection_resource_dir = ctx.obj["COLLECTION_RESOURCE_DIR"]
    organisation_dir = ctx.obj["ORGANISATION_DIR"]
    specification = ctx.obj["SPECIFICATION"]
    specification_dir = ctx.obj["SPECIFICATION_DIR"]
    tmp_dir = ctx.obj["TMP_DIR"]

    entity_num_gen = EntityNumGen()
    entity_num_gen.load_entity_state(
        dataset=dataset_name,
        pipeline_dir=pipeline_dir,
        specification_dir=specification_dir,
    )

    print("")
    print("======================================================================")
    print("New Lookups")
    print("======================================================================")

    # generate the lookup entries for each new resource
    dataset_resource_map = collection.dataset_resource_map()
    for dataset in dataset_resource_map:
        for resource in dataset_resource_map[dataset]:
            resource_file_path = collection_resource_dir / resource

            resource_lookups = get_resource_unidentified_lookups(
                input_path=resource_file_path,
                dataset=dataset,
                organisations=collection.resource_organisations(resource),
                pipeline=pipeline,
                specification=specification,
                tmp_dir=tmp_dir.absolute(),
                org_csv_path=(organisation_dir / "organisation.csv").absolute(),
                entity_num_gen=entity_num_gen,
            )

            collection.new_lookups += resource_lookups

    # save new lookups to file
    lookup_csv_path = ctx.obj["PIPELINE_DIR"] / "lookup.csv"
    if lookup_csv_path.is_file():
        with open(lookup_csv_path, "a") as file:
            for line in collection.new_lookups:
                file.write(f"{line[0]}\n")


def task_postprocess(ctx):
    postprocess_collection_csvs(ctx)


# =====================================================================
# Supporting functions and classes
# =====================================================================
class CollectionLogStore(ItemStore):
    def load_item(self, path):
        item = super().load_item(path)
        item = item.copy()

        # migrate content-type and bytes fields
        h = item.get("response-headers", {})
        if "content-type" not in item:
            item["content-type"] = h.get("Content-Type", "")
        if "bytes" not in item:
            item["bytes"] = h.get("Content-Length", "")

        # migrate datetime to entry-date field
        if "datetime" in item:
            if "entry-date" not in item:
                item["entry-date"] = item["datetime"]
            del item["datetime"]

        # migrate url to endpoint-url field
        if "url" in item:
            if "endpoint-url" not in item:
                item["endpoint-url"] = item["url"]
            del item["url"]

        # default the endpoint value
        if "endpoint" not in item:
            item["endpoint"] = hash_value(item["endpoint-url"])
        self.check_item_path(item, path)

        return item

    def save_item(self, item, path):
        del item["endpoint"]
        return super().save_item(item)

    def check_item_path(self, item, in_path):
        # m = re.match(r"^.*\/([-\d]+)\/(\w+).json", path)
        # (date, endpoint) = m.groups()
        path = Path(in_path)
        date = path.parts[-2]
        endpoint = path.parts[-1].split(".")[0]

        if not item.get("entry-date", "").startswith(date):
            logging.warning(
                "incorrect date in path %s for entry-date %s"
                % (path, item["entry-date"])
            )

        # print(item["url"], hash_value(item["url"]))
        if endpoint != item["endpoint"]:
            logging.warning(
                "incorrect endpoint in path %s expected %s" % (path, item["endpoint"])
            )


class EntityNumGen:
    def __init__(self, entity_num_state: dict = None):
        if not entity_num_state:
            entity_num_state = {
                "range_min": 0,
                "range_max": 100,
                "current": 0,
            }

        self.entity_num_state = entity_num_state

    def next(self):
        current = self.entity_num_state["current"]
        new_current = current + 1

        if new_current > self.entity_num_state["range_max"]:
            new_current = self.entity_num_state["range_min"]

        if new_current < self.entity_num_state["range_min"]:
            new_current = self.entity_num_state["range_min"]

        self.entity_num_state["current"] = new_current

        return new_current

    def load_entity_state(
        self,
        dataset: str = None,
        pipeline_dir: Path = None,
        specification_dir: Path = None,
    ):
        """
        sets entity_state values from the dataset range outlined in the specification
        for the given dataset
        :param dataset:
        :param pipeline_dir:
        :param specification_dir:
        :return:
        """
        if not dataset:
            return
        if not pipeline_dir:
            return
        if not specification_dir:
            return

        # find entity range for dataset
        dataset_cols = [
            "dataset",
            "entity-minimum",
            "entity-maximum",
        ]
        dataset_path = specification_dir / "dataset.csv"
        dataset_df = pd.read_csv(
            dataset_path, header=0, index_col=False, usecols=dataset_cols
        )

        # find the highest entity used so far
        lookup_cols = [
            "prefix",
            "entity",
        ]
        lookup_path = pipeline_dir / "lookup.csv"
        lookup_df = pd.read_csv(
            lookup_path, header=0, index_col=False, usecols=lookup_cols
        )

        self.entity_num_state = {
            "range_min": dataset_df.loc[dataset_df["dataset"] == dataset].values[0][1],
            "range_max": dataset_df.loc[dataset_df["dataset"] == dataset].values[0][2],
            "current": lookup_df.loc[lookup_df["prefix"] == dataset].max()["entity"],
        }


# print all lookups that aren't found will need to read through all files
class PrintLookupPhase(Phase):
    def __init__(self, lookups={}, entity_num_gen=None):
        self.lookups = lookups
        self.entity_field = "entity"
        self.entity_num_gen = entity_num_gen
        self.new_lookup_entries = []

    def lookup(self, **kwargs):
        return self.lookups.get(key(**kwargs), "")

    def process(self, stream):
        for block in stream:
            row = block["row"]
            entry_number = block["entry-number"]
            prefix = row.get("prefix", "")
            reference = row.get("reference", "")
            if "," in reference:
                reference = f'"{reference}"'
            organisation = row.get("organisation", "")
            if prefix:
                if not row.get(self.entity_field, ""):
                    entity = (
                        # by the resource and row number
                        (
                            self.entity_field == "entity"
                            and self.lookup(prefix=prefix, entry_number=entry_number)
                        )
                        # TBD: fixup prefixes so this isn't needed ..
                        # or by the organisation and the reference
                        or self.lookup(
                            prefix=prefix,
                            organisation=organisation,
                            reference=reference,
                        )
                    )

            if not entity:
                if prefix and organisation and reference:
                    new_entry = f"{prefix},,{organisation},{reference},{self.entity_num_gen.next()}"
                    self.new_lookup_entries.append([new_entry])
                    print(new_entry)

            yield block


def postprocess_collection_csvs(ctx):
    """
    clean up files and folders that do not typically get checked-in to
    source control.
    NOTE: by-pass this routine if you wish to inspect all files used / generated
    by the process
    :param ctx:
    :return:
    """
    tmp_dir = ctx.obj["TMP_DIR"]
    collection_resource_dir = ctx.obj["COLLECTION_RESOURCE_DIR"]

    log_csv_path = ctx.obj["COLLECTION_DIR"] / "log.csv"
    resource_csv_path = ctx.obj["COLLECTION_DIR"] / "resource.csv"

    # clean up directories
    if tmp_dir.is_dir():
        shutil.rmtree(tmp_dir)
    if collection_resource_dir.is_dir():
        shutil.rmtree(collection_resource_dir)

    # merge existing source.csv and endpoint.csv entries with new ones
    try:
        os.remove(log_csv_path)
        os.remove(resource_csv_path)
    except OSError:
        pass


def get_resource_unidentified_lookups(
    input_path=None,
    dataset=None,
    organisations=[],
    pipeline=None,
    specification=None,
    tmp_dir=None,
    org_csv_path=None,
    entity_num_gen=None,
):
    if not (pipeline or specification or dataset or input_path):
        error_msg = "Failed to perform lookups for resource"
        raise Exception(error_msg)

    # convert phase inputs
    resource = resource_from_path(input_path)
    dataset_resource_log = DatasetResourceLog(dataset=dataset, resource=resource)
    custom_temp_dir = tmp_dir  # './var'

    print("")
    print("----------------------------------------------------------------------")
    print(f">>> organisations:{organisations}")
    print(f">>> resource:{resource}")
    print("----------------------------------------------------------------------")

    # normalise phase inputs
    skip_patterns = pipeline.skip_patterns(resource)
    null_path = None

    # concat field phase
    concats = pipeline.concatenations(resource)
    column_field_log = ColumnFieldLog(dataset=dataset, resource=resource)

    # map phase
    intermediate_fieldnames = specification.intermediate_fieldnames(pipeline)
    columns = pipeline.columns(resource)

    # patch phase
    patches = pipeline.patches(resource=resource)

    # harmonize phase
    issue_log = IssueLog(dataset=dataset, resource=resource)

    # default phase
    default_fields = pipeline.default_fields(resource=resource)
    default_values = pipeline.default_values(endpoints=[])

    if len(organisations) == 1:
        default_values["organisation"] = organisations[0]

    # migrate phase
    schema = specification.pipeline[pipeline.name]["schema"]

    # organisation phase
    organisation = Organisation(org_csv_path, Path(pipeline.path))

    # print lookups phase
    pipeline_lookups = pipeline.lookups()
    print_lookup_phase = PrintLookupPhase(
        lookups=pipeline_lookups, entity_num_gen=entity_num_gen
    )

    run_pipeline(
        ConvertPhase(
            path=input_path,
            dataset_resource_log=dataset_resource_log,
            custom_temp_dir=custom_temp_dir,
        ),
        NormalisePhase(skip_patterns=skip_patterns, null_path=null_path),
        ParsePhase(),
        ConcatFieldPhase(concats=concats, log=column_field_log),
        MapPhase(
            fieldnames=intermediate_fieldnames,
            columns=columns,
            log=column_field_log,
        ),
        FilterPhase(filters=pipeline.filters(resource)),
        PatchPhase(
            issues=issue_log,
            patches=patches,
        ),
        HarmonisePhase(
            specification=specification,
            issues=issue_log,
        ),
        DefaultPhase(
            default_fields=default_fields,
            default_values=default_values,
            issues=issue_log,
        ),
        # TBD: move migrating columns to fields to be immediately after map
        # this will simplify harmonisation and remove intermediate_fieldnames
        # but effects brownfield-land and other pipelines which operate on columns
        MigratePhase(
            fields=specification.schema_field[schema],
            migrations=pipeline.migrations(),
        ),
        OrganisationPhase(organisation=organisation),
        FieldPrunePhase(fields=specification.current_fieldnames(schema)),
        EntityReferencePhase(
            dataset=dataset,
            specification=specification,
        ),
        EntityPrefixPhase(dataset=dataset),
        print_lookup_phase,
    )

    return print_lookup_phase.new_lookup_entries
