"""
GML extractor for title-boundary datasets.

Handles extraction of GML files from ZIP archives.
"""

import zipfile
from pathlib import Path


class GMLExtractor:
    """Extracts GML files from ZIP archives."""
    
    @staticmethod
    def extract_gml_from_zip(zip_path: Path, output_dir: Path) -> Path:
        """
        Extract GML file from ZIP archive.
        
        Args:
            zip_path: Path to ZIP file
            output_dir: Directory to extract GML file to
            
        Returns:
            Path to extracted GML file
            
        Raises:
            ValueError: If no GML file found in archive
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"  Extracting GML from {zip_path}")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Find GML file in archive
            gml_files = [f for f in zip_ref.namelist() if f.lower().endswith('.gml')]
            
            if not gml_files:
                raise ValueError(f"No GML file found in {zip_path}")
            
            gml_filename = gml_files[0]
            print(f"  Found: {gml_filename}")
            
            # Extract to output directory
            zip_ref.extract(gml_filename, output_dir)
            
            gml_path = output_dir / gml_filename
            size_mb = gml_path.stat().st_size / (1024 * 1024)
            print(f"  Extracted: {gml_path} ({size_mb:.1f} MB)")
            
            return gml_path
