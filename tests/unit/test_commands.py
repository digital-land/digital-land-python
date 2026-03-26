def test_validate_and_add_data_input_error_thrown_whhen_no_resource_downloaded(
    csv_file_path, collection_name, collection_dir, specification_dir, organisation_path
):
    expected_cols = [
        "pipelines",
        "organisation",
        "documentation-url",
        "endpoint-url",
        "start-date",
        "licence",
    ]

    specification = Specification(specification_dir)
    organisation = Organisation(organisation_path=organisation_path)

    collection = Collection(name=collection_name, directory=collection_dir)
    collection.load()
    # ===== FIRST VALIDATION BASED ON IMPORT.CSV INFO
    # - Check licence, url, date, organisation

    # read and process each record of the new endpoints csv at csv_file_path i.e import.csv

    with open(csv_file_path) as new_endpoints_file:
        reader = csv.DictReader(new_endpoints_file)
        csv_columns = reader.fieldnames

        # validate the columns in input .csv
        for expected_col in expected_cols:
            if expected_col not in csv_columns:
                raise Exception(f"required column ({expected_col}) not found in csv")

        for row in reader:
            # validate licence
            if row["licence"] == "":
                raise ValueError("Licence is blank")
            elif not specification.licence.get(row["licence"], None):
                raise ValueError(
                    f"Licence '{row['licence']}' is not a valid licence according to the specification."
                )
            # check if urls are not blank and valid urls
            is_endpoint_valid, endpoint_valid_error = is_url_valid(
                row["endpoint-url"], "endpoint_url"
            )
            is_documentation_valid, documentation_valid_error = is_url_valid(
                row["documentation-url"], "documentation_url"
            )
            if not is_endpoint_valid or not is_documentation_valid:
                raise ValueError(
                    f"{endpoint_valid_error} \n {documentation_valid_error}"
                )

            # if there is no start-date, do we want to populate it with today's date?
            if row["start-date"]:
                valid_date, error = is_date_valid(row["start-date"], "start-date")
                if not valid_date:
                    raise ValueError(error)

            # validate organisation
            if row["organisation"] == "":
                raise ValueError("The organisation must not be blank")
            elif not organisation.lookup(row["organisation"]):
                raise ValueError(
                    f"The given organisation '{row['organisation']}' is not in our valid organisations"
                )

            # validate pipeline(s) - do they exist and are they in the collection
            pipelines = row["pipelines"].split(";")
            for pipeline in pipelines:
                if not specification.dataset.get(pipeline, None):
                    raise ValueError(
                        f"'{pipeline}' is not a valid dataset in the specification"
                    )
                collection_in_specification = specification.dataset.get(
                    pipeline, None
                ).get("collection")
                if collection_name != collection_in_specification:
                    raise ValueError(
                        f"'{pipeline}' does not belong to provided collection {collection_name}"
                    )

    # VALIDATION DONE, NOW ADD TO COLLECTION
    print("======================================================================")
    print("Endpoint and source details")
    print("======================================================================")
    print("Endpoint URL: ", row["endpoint-url"])
    print("Endpoint Hash:", hash_value(row["endpoint-url"]))
    print("Documentation URL: ", row["documentation-url"])
    print()

    endpoints = []
    # if endpoint already exists, it will indicate it and quit function here
    if collection.add_source_endpoint(row):
        endpoint = {
            "endpoint-url": row["endpoint-url"],
            "endpoint": hash_value(row["endpoint-url"]),
            "end-date": row.get("end-date", ""),
            "plugin": row.get("plugin"),
            "licence": row["licence"],
        }
        endpoints.append(endpoint)
    else:
        # We rely on the add_source_endpoint function to log why it couldn't be added
        raise Exception(
            "Endpoint and source could not be added - is this a duplicate endpoint?"
        )

    # if successfully added we can now attempt to fetch from endpoint
    collector = Collector(
        resource_dir=str(Path(collection_dir) / "resource"),
        log_dir=str(Path(collection_dir) / "log"),
    )
    endpoint_resource_info = {}
    for endpoint in endpoints:
        fetch_status, log = collector.fetch(
            url=endpoint["endpoint-url"],
            endpoint=endpoint["endpoint"],
            end_date=endpoint["end-date"],
            plugin=endpoint["plugin"],
            refill_todays_logs=True,
        )
        try:
            # log is already returned from fetch, but read from file if needed for verification
            log_path = collector.log_path(datetime.utcnow(), endpoint["endpoint"])
            if os.path.isfile(log_path):
                with open(log_path, "r") as f:
                    log = json.load(f)
        except Exception as e:
            print(
                f"Error: The log file for {endpoint} could not be read from path {log_path}.\n{e}"
            )
            break
        log_status = log.get("status", None)
        exception = log.get("exception", None)
        if fetch_status not in [FetchStatus.OK, FetchStatus.ALREADY_FETCHED]:
            raise HTTPError(
                f"Failed to collect from URL. fetch status: {fetch_status}, log status: {log_status}, exception: {exception}"
            )
        # Raise exception if status is not 200
        if not log_status or log_status != "200":
            raise HTTPError(
                f"Failed to collect from URL with status: {log_status if log_status else exception}"
            )

        # Resource and path will only be printed if downloaded successfully but should only happen if status is 200
        resource = log.get("resource", None)
        if resource:
            resource_path = Path(collection_dir) / "resource" / resource
            print(
                "Resource collected: ",
                resource,
            )
            print(
                "Resource Path is: ",
                resource_path,
            )

        print(f"Log Status for {endpoint['endpoint']}: The status is {log_status}")
        endpoint_resource_info.update(
            {
                "endpoint": endpoint["endpoint"],
                "resource": log.get("resource"),
                "resource_path": resource_path,
                "pipelines": row["pipelines"].split(";"),
                "organisation": row["organisation"],
                "entry-date": row["entry-date"],
            }
        )

    return collection, endpoint_resource_info




def validate_and_add_data_input_non_200(
    csv_file_path, collection_name, collection_dir, specification_dir, organisation_path
):
    expected_cols = [
        "pipelines",
        "organisation",
        "documentation-url",
        "endpoint-url",
        "start-date",
        "licence",
    ]

    specification = Specification(specification_dir)
    organisation = Organisation(organisation_path=organisation_path)

    collection = Collection(name=collection_name, directory=collection_dir)
    collection.load()
    # ===== FIRST VALIDATION BASED ON IMPORT.CSV INFO
    # - Check licence, url, date, organisation

    # read and process each record of the new endpoints csv at csv_file_path i.e import.csv

    with open(csv_file_path) as new_endpoints_file:
        reader = csv.DictReader(new_endpoints_file)
        csv_columns = reader.fieldnames

        # validate the columns in input .csv
        for expected_col in expected_cols:
            if expected_col not in csv_columns:
                raise Exception(f"required column ({expected_col}) not found in csv")

        for row in reader:
            # validate licence
            if row["licence"] == "":
                raise ValueError("Licence is blank")
            elif not specification.licence.get(row["licence"], None):
                raise ValueError(
                    f"Licence '{row['licence']}' is not a valid licence according to the specification."
                )
            # check if urls are not blank and valid urls
            is_endpoint_valid, endpoint_valid_error = is_url_valid(
                row["endpoint-url"], "endpoint_url"
            )
            is_documentation_valid, documentation_valid_error = is_url_valid(
                row["documentation-url"], "documentation_url"
            )
            if not is_endpoint_valid or not is_documentation_valid:
                raise ValueError(
                    f"{endpoint_valid_error} \n {documentation_valid_error}"
                )

            # if there is no start-date, do we want to populate it with today's date?
            if row["start-date"]:
                valid_date, error = is_date_valid(row["start-date"], "start-date")
                if not valid_date:
                    raise ValueError(error)

            # validate organisation
            if row["organisation"] == "":
                raise ValueError("The organisation must not be blank")
            elif not organisation.lookup(row["organisation"]):
                raise ValueError(
                    f"The given organisation '{row['organisation']}' is not in our valid organisations"
                )

            # validate pipeline(s) - do they exist and are they in the collection
            pipelines = row["pipelines"].split(";")
            for pipeline in pipelines:
                if not specification.dataset.get(pipeline, None):
                    raise ValueError(
                        f"'{pipeline}' is not a valid dataset in the specification"
                    )
                collection_in_specification = specification.dataset.get(
                    pipeline, None
                ).get("collection")
                if collection_name != collection_in_specification:
                    raise ValueError(
                        f"'{pipeline}' does not belong to provided collection {collection_name}"
                    )

    # VALIDATION DONE, NOW ADD TO COLLECTION
    print("======================================================================")
    print("Endpoint and source details")
    print("======================================================================")
    print("Endpoint URL: ", row["endpoint-url"])
    print("Endpoint Hash:", hash_value(row["endpoint-url"]))
    print("Documentation URL: ", row["documentation-url"])
    print()

    endpoints = []
    # if endpoint already exists, it will indicate it and quit function here
    if collection.add_source_endpoint(row):
        endpoint = {
            "endpoint-url": row["endpoint-url"],
            "endpoint": hash_value(row["endpoint-url"]),
            "end-date": row.get("end-date", ""),
            "plugin": row.get("plugin"),
            "licence": row["licence"],
        }
        endpoints.append(endpoint)
    else:
        # We rely on the add_source_endpoint function to log why it couldn't be added
        raise Exception(
            "Endpoint and source could not be added - is this a duplicate endpoint?"
        )

    # if successfully added we can now attempt to fetch from endpoint
    collector = Collector(
        resource_dir=str(Path(collection_dir) / "resource"),
        log_dir=str(Path(collection_dir) / "log"),
    )
    endpoint_resource_info = {}
    for endpoint in endpoints:
        fetch_status, log = collector.fetch(
            url=endpoint["endpoint-url"],
            endpoint=endpoint["endpoint"],
            end_date=endpoint["end-date"],
            plugin=endpoint["plugin"],
            refill_todays_logs=True,
        )
        try:
            # log is already returned from fetch, but read from file if needed for verification
            log_path = collector.log_path(datetime.utcnow(), endpoint["endpoint"])
            if os.path.isfile(log_path):
                with open(log_path, "r") as f:
                    log = json.load(f)
        except Exception as e:
            print(
                f"Error: The log file for {endpoint} could not be read from path {log_path}.\n{e}"
            )
            break
        log_status = log.get("status", None)
        exception = log.get("exception", None)
        if fetch_status not in [FetchStatus.OK, FetchStatus.ALREADY_FETCHED]:
            raise HTTPError(
                f"Failed to collect from URL. fetch status: {fetch_status}, log status: {log_status}, exception: {exception}"
            )
        # Raise exception if status is not 200
        if not log_status or log_status != "200":
            raise HTTPError(
                f"Failed to collect from URL with status: {log_status if log_status else exception}"
            )

        # Resource and path will only be printed if downloaded successfully but should only happen if status is 200
        resource = log.get("resource", None)
        if resource:
            resource_path = Path(collection_dir) / "resource" / resource
            print(
                "Resource collected: ",
                resource,
            )
            print(
                "Resource Path is: ",
                resource_path,
            )

        print(f"Log Status for {endpoint['endpoint']}: The status is {log_status}")
        endpoint_resource_info.update(
            {
                "endpoint": endpoint["endpoint"],
                "resource": log.get("resource"),
                "resource_path": resource_path,
                "pipelines": row["pipelines"].split(";"),
                "organisation": row["organisation"],
                "entry-date": row["entry-date"],
            }
        )

    return collection, endpoint_resource_info