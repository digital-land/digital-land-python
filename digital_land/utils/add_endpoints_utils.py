import os
import shutil

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
