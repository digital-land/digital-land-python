import subprocess


def get_version():
    try:
        return (
            subprocess.check_output("git describe --tags --match [0-9]*".split())
            .decode()
            .strip()
            .split("-", 1)[0]
        )
    except subprocess.CalledProcessError:
        print("Unable to get version number from git tags")
        exit(1)


def next_version():
    version = get_version().split(".")
    version[-1] = str(int(version[-1], 10) + 1)
    return ".".join(version)


if __name__ == "__main__":
    print(next_version())
