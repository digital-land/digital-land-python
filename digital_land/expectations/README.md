## Expectations Data Quality

This tool was developed to make easy to run Data Quality checks against our pipelines.

It's development environment was a docker container with the setup exactly as described here:
https://github.com/digital-land/digital-land-docker-pipeline-runner

At the moment unit tests cover expectations.py and most of the core.py, unit tests for some parts of core and main were left to be completed later.

Executing a yaml data quality file against a sqlite3 file is done the following way:

    digital-land expectations --results-path "expectations/results" --sqlite-dataset-path "/src/sharing_area/conservation-area-collection/dataset/conservation-area.sqlite3" --data-quality-yaml "/src/sharing_area/green-box-data-quality/expectations/conservation-area.yaml"

Note 1: in prod you will need to change the parameters to the ones of your pipeline
    --results-path 
    --sqlite-dataset-path
    --data-quality-yaml

Note 2: because the command is running from the digital-land CLI it needs to either be run in a folder that has the default subfolders for dataset, pipeline_dir and specification_dir OR it needs to be given these as additional parameters (even if the only parameters it uses are the 3 mentioned in Note 1)

## Writting you data-quality-yaml

An example of a yaml is provided in the file: expectations/example_expectations_set.yaml

The core structure needed in the yaml is as follows:

```yaml
expectations:
  - expectation_name: name_of_the_expectation_function_1
    expectation_severity: RaiseError
    argument_1: False
    argument_2: 
      - value_1
      - value_2
      - value_3
      - value_4
    (...)
    argument_x: value_x

  - expectation_name: name_of_the_expectation_function_2
    expectation_severity: LogWarning
    argument_1: False
    argument_2: 
      - value_1
      - value_2
      - value_3
      - value_4

    (... and so on)
```
### Important Notes:
- **name_of_the_expectation_function** should match their definition in expectations/expectations.py
- **expectation_severity** should be either *RaiseError* or *LogWarning*. If any expectation with *RaiseError* severity returns *False* main will exit with a *DataQualityError*, if only *LogWarning* expectations return *False* the script will raise a warning and will also save to the results folder as *Fail* but the scrit will not exit with *error*.
- all arguments names should match the expectation arguments in expectations/expectations.py
- the way the arguments are expressed in yaml should be consistent with their types in the function definition. For example, when providing an argument that is expected as a list in the function definition, you need to have the yaml equivalent of a list, even if you want to provide one single item in the list:

```yaml
    list_name:
      - item_1
```
