import subprocess
import hashlib
import difflib
from wasabi import color

def execute(command):
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    try:
        outs, errs = proc.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()

    return proc.returncode, outs.decode("utf-8"), errs.decode("utf-8")


def hash_digest(url):
    return hashlib.sha256(url.encode("utf-8")).hexdigest()

def print_diffs(fromfile, tofile):
    # helper function to print detailed diffs between two files
    file_a = open(fromfile).readlines()
    file_b = open(tofile).readlines()
    count = 0
    message = []
    for a, b in zip(file_a, file_b):
        count += 1
        if a == b:
            continue
        message.append(f"line {count} differs: ")
        matcher = difflib.SequenceMatcher(None, a, b)
        output = []
        for opcode, a0, a1, b0, b1 in matcher.get_opcodes():
            if opcode == "equal":
                output.append(a[a0:a1])
            elif opcode == "insert":
                output.append(color(b[b0:b1], fg=16, bg="green"))
            elif opcode == "delete":
                output.append(color(a[a0:a1], fg=16, bg="red"))
            elif opcode == "replace":
                output.append(color(b[b0:b1], fg=16, bg="green"))
                output.append(color(a[a0:a1], fg=16, bg="red"))
        message.append("".join(output))
    return "".join(message)