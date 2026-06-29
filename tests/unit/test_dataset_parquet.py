def test_close_conn_removes_duckdb_file(tmp_path):
    import duckdb
    from digital_land.package.dataset_parquet import DatasetParquetPackage

    duckdb_path = tmp_path / "overflow.duckdb"

    # build a minimal instance, bypassing the spec-driven __init__ so this stays
    # a true unit test of close_conn (which only needs .conn and .duckdb_path)
    pkg = DatasetParquetPackage.__new__(DatasetParquetPackage)
    pkg.duckdb_path = duckdb_path
    pkg.conn = duckdb.connect(str(duckdb_path))
    assert duckdb_path.exists()

    pkg.close_conn()

    assert not duckdb_path.exists()


def test_close_conn_handles_in_memory_db():
    import duckdb
    from digital_land.package.dataset_parquet import DatasetParquetPackage

    # in-memory mode never sets duckdb_path; close_conn must not raise
    pkg = DatasetParquetPackage.__new__(DatasetParquetPackage)
    pkg.conn = duckdb.connect()  # in-memory

    pkg.close_conn()  # should not raise AttributeError
