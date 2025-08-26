import polars as pl

from pathlib import Path

from digital_land.platform import Platform


class TestPlatform:
    def test_download_dataset_csv_download(self, tmp_path: Path, mocker):

        # create dependencies including a dataset
        # create and store dataset

        data = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 87},
            {"id": 3, "name": "Charlie", "score": 92},
        ]

        dataset_name = "test-dataset"
        files_url = tmp_path / "files"
        input_df = pl.DataFrame(data)
        input_path = files_url / "dataset" / f"{dataset_name}.csv"
        input_path.parent.mkdir(parents=True)
        input_df.write_csv(input_path)

        mocker.patch.object(
            Platform, "_get_dataset_file_url", return_value=str(input_path)
        )
        # set up
        platform = Platform()

        output_path = tmp_path / f"{dataset_name}.csv"

        # Simulate downloading a dataset
        print(str(output_path))
        platform.download_dataset(
            output_path=str(output_path),
            dataset=dataset_name,
        )

        # Check if the file was created
        assert output_path.exists()
        assert output_path.is_file()
