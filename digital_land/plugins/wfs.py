import io
import re

from digital_land.phase.convert import detect_encoding


# Black (22.1.0) doesn't recognize that raw strings containing " have to be enclosed in '
# fmt: off
strip_exps = [
    (re.compile(rb' ?timeStamp="[^"]*"'), rb""),
    (re.compile(rb' ?fid="[^"]*"'), rb""),
    (re.compile(rb'(gml:id="[^."]+)[^"]*'), rb"\1"),
]
# fmt: on


def strip_variable_content(content):
    for strip_exp, replacement in strip_exps:
        content = strip_exp.sub(replacement, content)
    return content


def get(collector, url, log={}, plugin="wfs"):
    log, content = collector.get(url=url, log=log, plugin=plugin)
    encoding = detect_encoding(io.BytesIO(content))
    if encoding:
        content = strip_variable_content(content)
    return log, content
