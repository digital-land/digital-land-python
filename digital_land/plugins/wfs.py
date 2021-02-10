import io
import re
import xml.sax

import lxml.etree as ET

from digital_land.load import detect_encoding

strip_exps = [
    (re.compile(br' ?timeStamp="[^"]*"'), br""),
    (re.compile(br' ?fid="[^"]*"'), br""),
    (re.compile(br'(gml:id="[^."]+)[^"]*'), br"\1"),
]


def strip_variable_content(content):
    for strip_exp, replacement in strip_exps:
        content = strip_exp.sub(replacement, content)
    return content


def is_xml(content):
    try:
        xml.sax.parseString(content, xml.sax.ContentHandler())
        return True
    except:  # SAX' exceptions are not public
        pass
    return False


def canonicalise_xml(content):
    et = ET.fromstring(content)
    root = et.getroottree()
    output = io.BytesIO()
    __import__("pdb").set_trace()
    root.write(output, method="c14n")
    return output.getvalue()


def get(collector, url, log={}, plugin="wfs"):
    log, content = collector.get(url=url, log=log, plugin=plugin)
    encoding = detect_encoding(io.BytesIO(content))
    if encoding:
        if is_xml(content) and False:
            content = canonicalise_xml(content)
        content = strip_variable_content(content)
    return log, content
