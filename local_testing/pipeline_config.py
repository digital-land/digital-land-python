"""
Pipeline configuration management for title-boundary dataset.

Handles creation and management of pipeline configuration CSV files
and downloading of required resources like organisation.csv.
"""

import urllib.request
from pathlib import Path


class PipelineConfig:
    """Manages pipeline configuration files and resources."""

    @staticmethod
    def ensure_pipeline_config(pipeline_dir: Path):
        """
        Ensure all required pipeline configuration CSV files exist.

        Creates default configuration files for:
        - column mapping
        - default values
        - patches, concatenations, combinations
        - filters, lookups, migrations, redirects

        Args:
            pipeline_dir: Directory where pipeline config files should be created
        """
        pipeline_dir.mkdir(parents=True, exist_ok=True)

        configs = {
            "column.csv": """dataset,resource,column,field
title-boundary,,reference,reference
title-boundary,,name,name
title-boundary,,geometry,geometry
title-boundary,,start-date,start-date
title-boundary,,entry-date,entry-date
title-boundary,,end-date,end-date
title-boundary,,prefix,prefix
title-boundary,,organisation,organisation
title-boundary,,notes,notes
title-boundary,,national-cadastral-reference,notes
""",
            "default.csv": "dataset,resource,field,default-field,entry-date\n",
            "patch.csv": "dataset,resource,field,pattern,value\n",
            "concat.csv": "dataset,resource,field,fields,separator\n",
            "combine.csv": "dataset,resource,field,fields,separator\n",
            "convert.csv": "dataset,resource,field,value,replacement\n",
            "filter.csv": "dataset,resource,field,pattern\n",
            "skip.csv": "dataset,resource,pattern\n",
            "lookup.csv": "prefix,resource,organisation,reference,entity\n",
            "migrate.csv": "dataset,old-field,new-field\n",
            "redirect.csv": "entity,status,redirect-entity\n",
        }

        for filename, content in configs.items():
            filepath = pipeline_dir / filename
            if not filepath.exists():
                filepath.write_text(content)

    @staticmethod
    def download_organisation_csv(cache_dir: Path) -> Path:
        """
        Download organisation.csv from digital-land repository if not present.

        Falls back to creating a minimal organisation.csv with Land Registry
        data if download fails.

        Args:
            cache_dir: Directory where organisation.csv should be cached

        Returns:
            Path to organisation.csv file
        """
        org_csv = cache_dir / "organisation.csv"

        if not org_csv.exists():
            print("  Downloading organisation.csv...")
            url = "https://raw.githubusercontent.com/digital-land/organisation-dataset/main/collection/organisation.csv"

            try:
                cache_dir.mkdir(parents=True, exist_ok=True)
                urllib.request.urlretrieve(url, org_csv)
                print(f"  Downloaded organisation.csv ({org_csv.stat().st_size} bytes)")
            except Exception as e:
                print(f"  Warning: Could not download ({e}), creating minimal file")
                org_csv.write_text(
                    "organisation,name,statistical-geography,opendatacommunities-uri\n"
                    "government-organisation:D2,Land Registry,E92000001,"
                    "http://opendatacommunities.org/id/government-organisation/land-registry\n"
                )

        return org_csv
