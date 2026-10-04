"""
AHON - NOAH Data Extraction

Extracts the core NOAH hazard datasets:

    1. Flood 25-year
    2. Landslide
    3. Storm Surge SSA1-SSA4

Source:
    data/raw/noah/archives/

Output:
    data/raw/noah/datasets/

The original ZIP archives are preserved.

The extraction process is designed to be:
    - reproducible
    - storage-conscious
    - restartable
    - beginner-friendly

Important:
    This script only extracts the source data.

    It does NOT:
        - convert geometry to 2D
        - create GeoPackages
        - create Parquet
        - upload to Databricks

Those are separate downstream pipeline stages.
"""

from pathlib import Path
import io
import zipfile



# PROJECT PATHS


ARCHIVE_DIR = Path(
    "data/raw/noah/archives"
)

DATASET_DIR = Path(
    "data/raw/noah/datasets"
)



# SHAPEFILE COMPONENTS


# Only extract files that are useful for the ESRI Shapefile.
#
# Not every dataset will contain every extension.
#
# .shp = geometry
# .shx = geometry index
# .dbf = attribute table
# .prj = coordinate reference system
# .cpg = character encoding
# .qix = spatial index
# .sbn/.sbx = spatial index
# .qpj = QGIS projection information

ALLOWED_EXTENSIONS = {
    ".shp",
    ".shx",
    ".dbf",
    ".prj",
    ".cpg",
    ".qix",
    ".sbn",
    ".sbx",
    ".qpj",
}



# EXTRACTION SETTINGS

# Read/write large Shapefile components in 1 MB pieces.
#
# This avoids doing:
#
#     source.read()
#
# for an entire multi-hundred-MB file at once.

COPY_CHUNK_SIZE = 1024 * 1024



def extract_nested_shapefile(
    outer_zip: zipfile.ZipFile,
    nested_zip_name: str,
    output_dir: Path,
) -> bool:
    """
    Extract Shapefile components from a ZIP
    stored inside another ZIP.

    Parameters
    ----------
    outer_zip:
        The outer ZIP archive.

    nested_zip_name:
        Path/name of the nested ZIP inside
        the outer archive.

    output_dir:
        Directory where the Shapefile components
        will be extracted.

    Returns
    -------
    bool
        True if the dataset was newly extracted.
        False if the dataset already existed.
    """

    
    # Make sure the output directory exists.
    

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    
    # Determine the expected Shapefile name.
    #
    # Example:
    #
    #     25yr/Abra_Flood_25year.zip
    #
    # becomes:
    #
    #     Abra_Flood_25year.shp
    

    nested_name = Path(
        nested_zip_name
    ).stem

    expected_shp = (
        output_dir
        / f"{nested_name}.shp"
    )

    
    # Storage-friendly restart behavior
    #
    # If the main .shp file already exists,
    # assume this dataset has already been extracted.
    

    if expected_shp.exists():

        print(
            f"    SKIP: "
            f"{expected_shp.name} "
            f"already exists."
        )

        return False

    
    # Read the nested ZIP from the outer ZIP.
    #
    # We do not write the nested ZIP to disk.
    #
    # This avoids creating another large temporary
    # ZIP file on the filesystem.
    

    nested_zip_data = outer_zip.read(
        nested_zip_name
    )

    extracted = False

    
    # Open the nested ZIP from memory.
    

    with zipfile.ZipFile(
        io.BytesIO(nested_zip_data)
    ) as nested_zip:

        for file_name in nested_zip.namelist():

            file_path = Path(
                file_name
            )

            extension = (
                file_path.suffix.lower()
            )

          
            # Ignore files that are not Shapefile components.
          

            if extension not in ALLOWED_EXTENSIONS:
                continue

            output_path = (
                output_dir
                / file_path.name
            )

          
            # Don't overwrite an existing component.
          

            if output_path.exists():

                print(
                    f"      SKIP COMPONENT: "
                    f"{output_path.name}"
                )

                continue

            print(
                f"      Extracting: "
                f"{file_path.name}"
            )

          
            # Stream the component in 1 MB pieces.
          

            with (
                nested_zip.open(file_name) as source,
                open(
                    output_path,
                    "wb",
                ) as destination,
            ):

                while True:

                    chunk = source.read(
                        COPY_CHUNK_SIZE
                    )

                    if not chunk:
                        break

                    destination.write(
                        chunk
                    )

            extracted = True

    return extracted



# FLOOD EXTRACTION


def extract_flood() -> None:
    """
    Extract NOAH 25-year Flood province ZIPs.

    Expected archive pattern:

        Flood_25year_*.zip

    Expected nested ZIP structure:

        25yr/
            Province.zip
    """

    output_dir = (
        DATASET_DIR
        / "NOAH-Flood-Hazard"
        / "25-year"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    
    # Find all Flood 25-year archives.
    

    archive_files = sorted(
        ARCHIVE_DIR.glob(
            "Flood_25year_*.zip"
        )
    )

    if not archive_files:

        raise FileNotFoundError(
            "No Flood_25year_*.zip files found in "
            f"{ARCHIVE_DIR}"
        )

    extracted = 0
    skipped = 0

    
    # Process each outer archive.
    

    for archive_path in archive_files:

        print()
        print(
            f"Processing flood archive: "
            f"{archive_path.name}"
        )

        with zipfile.ZipFile(
            archive_path,
            "r",
        ) as outer_zip:

          
            # Find province ZIPs inside 25yr/.
          

            province_archives = [
                name
                for name in outer_zip.namelist()
                if (
                    name.startswith("25yr/")
                    and name.lower().endswith(
                        ".zip"
                    )
                )
            ]

            print(
                f"Found "
                f"{len(province_archives)} "
                f"province archives."
            )

          
            # Extract each province.
          

            for province_zip in province_archives:

                print(
                    f"  Dataset: "
                    f"{Path(province_zip).stem}"
                )

                was_extracted = (
                    extract_nested_shapefile(
                        outer_zip,
                        province_zip,
                        output_dir,
                    )
                )

                if was_extracted:

                    extracted += 1

                else:

                    skipped += 1

    
    # Summary
    

    print()
    print(
        "Flood extraction complete."
    )

    print(
        f"New datasets extracted: "
        f"{extracted}"
    )

    print(
        f"Existing datasets skipped: "
        f"{skipped}"
    )

    print(
        f"Output directory: "
        f"{output_dir}"
    )



# LANDSLIDE EXTRACTION


def extract_landslide() -> None:
    """
    Extract NOAH Landslide Hazard province ZIPs.

    Expected archive pattern:

        Landslide_*.zip

    Expected nested ZIP structure contains:

        LandslideHazards/
    """

    output_dir = (
        DATASET_DIR
        / "NOAH-Landslide-Hazard"
        / "LandslideHazards"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    
    # Find all Landslide archives.
    

    archive_files = sorted(
        ARCHIVE_DIR.glob(
            "Landslide_*.zip"
        )
    )

    if not archive_files:

        raise FileNotFoundError(
            "No Landslide_*.zip files found in "
            f"{ARCHIVE_DIR}"
        )

    extracted = 0
    skipped = 0

    
    # Process each outer archive.
    

    for archive_path in archive_files:

        print()
        print(
            f"Processing landslide archive: "
            f"{archive_path.name}"
        )

        with zipfile.ZipFile(
            archive_path,
            "r",
        ) as outer_zip:

          
            # Find province ZIPs.
          

            province_archives = [
                name
                for name in outer_zip.namelist()
                if (
                    "LandslideHazards/" in name
                    and name.lower().endswith(
                        ".zip"
                    )
                )
            ]

            print(
                f"Found "
                f"{len(province_archives)} "
                f"province archives."
            )

          
            # Extract each province.
          

            for province_zip in province_archives:

                print(
                    f"  Dataset: "
                    f"{Path(province_zip).stem}"
                )

                was_extracted = (
                    extract_nested_shapefile(
                        outer_zip,
                        province_zip,
                        output_dir,
                    )
                )

                if was_extracted:

                    extracted += 1

                else:

                    skipped += 1

    
    # Summary
    

    print()
    print(
        "Landslide extraction complete."
    )

    print(
        f"New datasets extracted: "
        f"{extracted}"
    )

    print(
        f"Existing datasets skipped: "
        f"{skipped}"
    )

    print(
        f"Output directory: "
        f"{output_dir}"
    )



# STORM SURGE EXTRACTION


def extract_storm_surge() -> None:
    """
    Extract NOAH Storm Surge datasets.

    Advisory levels:

        SSA1
        SSA2
        SSA3
        SSA4

    Expected source archive:

        Storm-Surge_01.zip
    """

    archive_path = (
        ARCHIVE_DIR
        / "Storm-Surge_01.zip"
    )

    output_base = (
        DATASET_DIR
        / "NOAH-Storm-Surge"
    )

    
    # Check archive.
    

    if not archive_path.exists():

        raise FileNotFoundError(
            "Storm Surge archive not found: "
            f"{archive_path}"
        )

    extracted = 0
    skipped = 0

    
    # Open Storm Surge archive.
    

    with zipfile.ZipFile(
        archive_path,
        "r",
    ) as outer_zip:
      
        # Find all nested province ZIPs.

        province_archives = [
            name
            for name in outer_zip.namelist()
            if (
                "StormSurgeAdvisory" in name
                and name.lower().endswith(
                    ".zip"
                )
            )
        ]

        print()
        print(
            "Storm Surge province archives "
            "found: "
            f"{len(province_archives)}"
        )

        # Process each nested ZIP

        for province_zip in province_archives:

            path_parts = Path(
                province_zip
            ).parts
            advisory_folder = next(
                (
                    part
                    for part in path_parts
                    if part.startswith(
                        "StormSurgeAdvisory"
                    )
                ),
                None,
            )

            if advisory_folder is None:

                print(
                    f"    WARNING: Could not "
                    f"determine advisory level "
                    f"for {province_zip}"
                )

                continue
              
            advisory_level = (
                advisory_folder.replace(
                    "StormSurgeAdvisory",
                    "",
                )
            )

            output_dir = (
                output_base
                / f"SSA{advisory_level}"
            )

            output_dir.mkdir(
                parents=True,
                exist_ok=True,
            )

            print()
            print(
                f"  Dataset: "
                f"SSA{advisory_level} / "
                f"{Path(province_zip).stem}"
            )

            was_extracted = (
                extract_nested_shapefile(
                    outer_zip,
                    province_zip,
                    output_dir,
                )
            )

            if was_extracted:

                extracted += 1

            else:

                skipped += 1

    
    # Summary
    print()
    print(
        "Storm Surge extraction complete."
    )

    print(
        f"New datasets extracted: "
        f"{extracted}"
    )

    print(
        f"Existing datasets skipped: "
        f"{skipped}"
    )

    print(
        f"Output directory: "
        f"{output_base}"
    )



# STORAGE SUMMARY
def show_storage_summary() -> None:
    """
    Calculate the size of the extracted NOAH datasets.
    """

    print()
    print("=" * 60)
    print("NOAH STORAGE SUMMARY")
    print("=" * 60)

    if not DATASET_DIR.exists():

        print(
            "No extracted datasets found."
        )

        return

    total_bytes = 0
    file_count = 0

    
    # Walk through all extracted files.
    

    for file_path in DATASET_DIR.rglob("*"):

        if not file_path.is_file():
            continue

        file_count += 1

        total_bytes += (
            file_path.stat().st_size
        )

    total_gb = (
        total_bytes
        / (1024 ** 3)
    )

    print(
        f"Files:       "
        f"{file_count:,}"
    )

    print(
        f"Total size:  "
        f"{total_gb:.2f} GB"
    )

    print(
        f"Location:    "
        f"{DATASET_DIR}"
    )



# MAIN
def main() -> None:
    """
    Run all NOAH extraction processes.
    """

    print()
    print("#" * 60)
    print("AHON — NOAH DATA EXTRACTION")
    print("#" * 60)

    print()
    print(
        f"Source archives: "
        f"{ARCHIVE_DIR}"
    )

    print(
        f"Extraction output: "
        f"{DATASET_DIR}"
    )

    
    # 1. Flood 25-year
    extract_flood()

    
    # 2. Landslide
    extract_landslide()

    
    # 3. Storm Surge
    extract_storm_surge()

    
    # Storage summary
    show_storage_summary()

    
    # Final message
    print()
    print("#" * 60)
    print("ALL NOAH EXTRACTION COMPLETE")
    print("#" * 60)



# ENTRY POINT


if __name__ == "__main__":
    main()
