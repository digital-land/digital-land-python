import re
import subprocess
from pathlib import Path


def run(command):
    proc = subprocess.run(command, capture_output=True, text=True)
    try:
        proc.check_returncode()  # raise exception on nonz-ero return code
    except subprocess.CalledProcessError as e:
        print(f"\n---- STDERR ----\n{proc.stderr}")
        print(f"\n---- STDOUT ----\n{proc.stdout}")
        raise e

    return proc


parse_container_id = re.compile(r"^#[0-9]+ writing image ([^ ]*) .*done$", re.MULTILINE)
parse_container_id_alternate = re.compile(r"^Successfully built ([^ ]*)$", re.MULTILINE)
parse_name = re.compile(r"^#[0-9]+ naming to ([^ ]*) done", re.MULTILINE)
parse_name_alternate = re.compile(r"Successfully tagged ([^ ]*)", re.MULTILINE)


def build_container(datasets, base_tag, options):
    datasette_tag = f"{base_tag}_datasette"
    dl_tag = f"{base_tag}_digital_land"

    container_id, name = build_datasette_container(datasets, datasette_tag, options)
    container_id, name = build_digital_land_container(datasets, datasette_tag, dl_tag)
    return (container_id, dl_tag)


def parse_docker_output(proc):
    container_id_match = parse_container_id.search(proc.stderr)

    if container_id_match:
        name_match = parse_name.search(proc.stderr)
    else:
        container_id_match = parse_container_id_alternate.search(proc.stdout)
        name_match = parse_name_alternate.search(proc.stdout)

    if not container_id_match:
        print("----- STDOUT -----")
        print(proc.stdout or "EMPTY")
        print("----- STDERR -----")
        print(proc.stderr or "EMPTY")
        raise Exception("container_id not matched")

    return (container_id_match.group(1), name_match.group(1) if name_match else None)


def log_command(command):
    print(f"executing command:\n{' '.join(command)}")


def build_datasette_container(datasets, tag, options):
    command = [
        "datasette",
        "package",
        '--extra-options="--setting sql_time_limit_ms 8000"',
        "--spatialite",
    ]

    if options:
        extra_commands = re.split(" |,", options)
        command.extend(extra_commands)

    if tag:
        command.extend(["--tag", tag])

    command.extend(datasets)
    log_command(command)
    proc = run(command)
    return parse_docker_output(proc)


def build_digital_land_container(datasets, source_tag, dest_tag):
    command = [
        "docker",
        "build",
        "-t",
        dest_tag,
        "--build-arg",
        f"DATASETS={','.join([Path(d).name for d in datasets])}",
        "--build-arg",
        f"APP_IMAGE={source_tag}",
        ".",
    ]
    log_command(command)
    proc = run(command)
    return parse_docker_output(proc)
