"""
AHON - NOAH Parquet Export

Pipeline:

    NOAH ZIP Archives
          ↓
    extract_noah.py
          ↓
    Raw NOAH Shapefiles
          ↓
    ingest_noah.py
          ↓
    Local Bronze GeoPackages
          ↓
    extract_noah_parquet.py
          ↓
    Parquet
          ↓
    Databricks
          ↓
    Delta Bronze

Datasets exported:

    1. Flood 25-year
    2. Landslide
    3. Storm Surge

Design:

    Flood:
        One Parquet file

    Landslide:
        100-row Parquet chunks because the geometries
        are large and complex.

    Storm Surge:
        One Parquet file

Important:

    - Original Bronze GeoPackages are never modified.
    - A temporary 2D GeoPackage is created/reused.
    - EPSG:4326 is preserved.
    - Geometry is standardized to 2D for Parquet export.
    - Parquet uses Snappy compression.
    - Landslide chunks are independently validated.
"""


# IMPORTS


from pathlib import Path
import subprocess
import gc

import pyarrow.parquet as pq
import pyogrio



# PROJECT PATHS


# Project root:
#
# AHON-Pipeline/
# ├── src/
# │   └── ingestion/
# │       └── noah/
# │           └── extract_noah_parquet.py
#
# parents[3] brings us back to AHON-Pipeline.

PROJECT_ROOT = Path(__file__).resolve().parents[3]



# BRONZE GEOPACKAGES


# These GeoPackages are created by ingest_noah.py.

BRONZE_DIR = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "noah"
)



# EXPORT / TEMPORARY FILES


# Temporary 2D GeoPackages are stored here.

EXPORT_DIR = (
    PROJECT_ROOT
    / "data"
    / "export"
    / "noah"
)



# PARQUET OUTPUT LOCATION


# Large Parquet files are written to the WSL filesystem
# instead of Windows C: drive.
#
# This is important because the Windows C: drive was
# previously almost full.

WSL_PARQUET_DIR = (
    Path.home()
    / "ahon-parquet"
)



# DATASET CONFIGURATION


DATASETS = {

    
    # FLOOD
    

    "flood": {

        "name": "flood_25yr",

        "input": (
            BRONZE_DIR
            / "flood_25yr"
            / "flood_25yr.gpkg"
        ),

        "layer": "bronze_noah_flood",

        "output": (
            WSL_PARQUET_DIR
            / "flood_25yr.parquet"
        ),

        "temp_2d": (
            EXPORT_DIR
            / "_temp"
            / "flood_25yr_2d.gpkg"
        ),

        "mode": "single",
    },


    
    # LANDSLIDE
    

    "landslide": {

        "name": "landslide",

        "input": (
            BRONZE_DIR
            / "landslide"
            / "landslide.gpkg"
        ),

        "layer": "bronze_noah_landslide",

        "output_dir": (
            WSL_PARQUET_DIR
            / "landslide_chunks"
        ),

        "temp_2d": (
            EXPORT_DIR
            / "_temp"
            / "landslide_2d.gpkg"
        ),

        "mode": "chunks",
    },


    
    # STORM SURGE
    

    "storm_surge": {

        "name": "storm_surge",

        "input": (
            BRONZE_DIR
            / "storm_surge"
            / "storm_surge.gpkg"
        ),

        "layer": "bronze_noah_storm_surge",

        "output": (
            WSL_PARQUET_DIR
            / "storm_surge.parquet"
        ),

        "temp_2d": (
            EXPORT_DIR
            / "_temp"
            / "storm_surge_2d.gpkg"
        ),

        "mode": "single",
    },
}



# CONFIGURATION


# All Parquet geometry is standardized to 2D.

TARGET_CRS = "EPSG:4326"


# Landslide is split into 100-row files because
# its geometries are much larger and more complex.

LANDSLIDE_CHUNK_SIZE = 100


# Small Arrow batches reduce memory usage.

ARROW_BATCH_SIZE = 25



# HELPER: RUN OGR2OGR


def run_ogr2ogr(
    command: list[str],
) -> None:
    """
    Run an ogr2ogr command.

    If ogr2ogr fails, the Python script also fails.
    """

    print()
    print("Running ogr2ogr:")
    print(" ".join(command))
    print()

    subprocess.run(
        command,
        check=True,
    )



# HELPER: CREATE DIRECTORY


def ensure_parent_directory(
    path: Path,
) -> None:
    """
    Create the parent directory of a file.
    """

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )



# HELPER: VALIDATE SOURCE GEOPACKAGE


def get_source_info(
    input_path: Path,
    layer: str,
) -> dict:
    """
    Read and validate the Bronze GeoPackage metadata.

    Returns:
        pyogrio dataset information.
    """

    
    # Check that the GeoPackage exists
    

    if not input_path.exists():

        raise FileNotFoundError(
            "\nBronze GeoPackage not found:\n"
            f"{input_path}\n\n"
            "Run ingest_noah.py first."
        )


    
    # Read GeoPackage metadata
    

    info = pyogrio.read_info(
        input_path,
        layer=layer,
    )


    
    # Display information
    

    print()
    print("=" * 70)
    print("BRONZE GEOPACKAGE")
    print("=" * 70)

    print(
        f"Path:      {input_path}"
    )

    print(
        f"Layer:     {layer}"
    )

    print(
        f"Features:  {info['features']:,}"
    )

    print(
        f"Geometry:  {info['geometry_type']}"
    )

    print(
        f"CRS:       {info['crs']}"
    )


    
    # CRS validation
    

    if info["crs"] != TARGET_CRS:

        raise RuntimeError(
            f"\nCRS validation failed.\n"
            f"Expected: {TARGET_CRS}\n"
            f"Found:    {info['crs']}"
        )


    return info



# CREATE 2D GEOPACKAGE


def create_2d_geopackage(
    source_path: Path,
    source_layer: str,
    temp_path: Path,
) -> None:
    """
    Create a temporary 2D GeoPackage.

    The original Bronze GeoPackage is preserved.

    -dim XY removes Z coordinates.
    -t_srs keeps the data in EPSG:4326.
    """

    
    # Reuse existing 2D GeoPackage
    

    if temp_path.exists():

        print()
        print(
            "Existing 2D GeoPackage found:"
        )

        print(
            temp_path
        )

        print(
            "Reusing existing 2D GeoPackage."
        )

        return


    
    # Create parent directory
    

    temp_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )


    
    # Convert to 2D
    

    print()
    print("=" * 70)
    print("CREATING 2D GEOPACKAGE")
    print("=" * 70)

    command = [

        "ogr2ogr",

        "-f",
        "GPKG",

        str(temp_path),

        str(source_path),

        source_layer,

        "-dim",
        "XY",

        "-t_srs",
        TARGET_CRS,

        "-nln",
        source_layer,

        "-overwrite",
    ]


    run_ogr2ogr(
        command
    )


    print()
    print(
        "2D GeoPackage created:"
    )

    print(
        temp_path
    )



# VALIDATE 2D GEOPACKAGE


def validate_2d_geopackage(
    temp_path: Path,
    layer: str,
    expected_features: int,
) -> dict:
    """
    Validate the temporary 2D GeoPackage.

    Checks:

        - feature count
        - CRS
        - geometry metadata
    """

    info = pyogrio.read_info(
        temp_path,
        layer=layer,
    )


    print()
    print("=" * 70)
    print("2D GEOPACKAGE VALIDATION")
    print("=" * 70)

    print(
        f"Expected features: "
        f"{expected_features:,}"
    )

    print(
        f"Actual features:   "
        f"{info['features']:,}"
    )

    print(
        f"Geometry type:     "
        f"{info['geometry_type']}"
    )

    print(
        f"CRS:                "
        f"{info['crs']}"
    )


    
    # Feature count validation
    

    if info["features"] != expected_features:

        raise RuntimeError(
            "\n2D GeoPackage feature count mismatch.\n"
            f"Expected: {expected_features:,}\n"
            f"Found:    {info['features']:,}"
        )


    
    # CRS validation
    

    if info["crs"] != TARGET_CRS:

        raise RuntimeError(
            "\n2D GeoPackage CRS mismatch.\n"
            f"Expected: {TARGET_CRS}\n"
            f"Found:    {info['crs']}"
        )


    print()
    print(
        "2D GeoPackage validation: PASS"
    )


    return info



# EXPORT ONE DATASET TO ONE PARQUET FILE


def export_single_parquet(
    input_path: Path,
    layer: str,
    output_path: Path,
) -> int:
    """
    Stream a GeoPackage into one Parquet file.

    Used for:

        - Flood
        - Storm Surge
    """

    
    # Create output directory
    

    ensure_parent_directory(
        output_path
    )


    
    # Remove existing Parquet
    

    if output_path.exists():

        print()
        print(
            "Removing existing Parquet:"
        )

        print(
            output_path
        )

        output_path.unlink()


    rows_written = 0
    writer = None


    print()
    print("=" * 70)
    print("STREAMING GEOPACKAGE → PARQUET")
    print("=" * 70)

    print(
        f"Input:  {input_path}"
    )

    print(
        f"Layer:  {layer}"
    )

    print(
        f"Output: {output_path}"
    )


    try:

       
        # Open GeoPackage using Arrow streaming
      

        with pyogrio.open_arrow(

            input_path,

            layer=layer,

            batch_size=ARROW_BATCH_SIZE,

            use_pyarrow=True,

        ) as (
            metadata,
            reader,
        ):

            print()
            print(
                "Arrow reader created."
            )

            print(
                f"Geometry type: "
                f"{metadata['geometry_type']}"
            )


           
            # Read batches
           

            while True:

                try:

                    batch = (
                        reader
                        .read_next_batch()
                    )

                except StopIteration:

                    break


               
                # Ignore empty batches
               

                if batch.num_rows == 0:

                    del batch

                    continue


               
                # Create Parquet writer from first batch
               

                if writer is None:

                    writer = pq.ParquetWriter(

                        output_path,

                        batch.schema,

                        compression="snappy",
                    )


               
                # Write batch
               

                writer.write_batch(
                    batch
                )


                rows_written += (
                    batch.num_rows
                )


                print(
                    f"Written: "
                    f"{rows_written:,} rows",
                    flush=True,
                )


               
                # Release batch memory
               

                del batch

                gc.collect()


    finally:

       
        # Always close Parquet writer
       

        if writer is not None:

            writer.close()

            writer = None
          
    # VALIDATE PARQUET

    print()
    print(
        "Validating Parquet..."
    )


    parquet_file = pq.ParquetFile(
        output_path
    )


    actual_rows = (
        parquet_file
        .metadata
        .num_rows
    )


    print(
        f"Rows written:  "
        f"{rows_written:,}"
    )

    print(
        f"Rows in file:  "
        f"{actual_rows:,}"
    )


    if actual_rows != rows_written:

        raise RuntimeError(
            "\nParquet validation failed.\n"
            f"Expected: {rows_written:,}\n"
            f"Found:    {actual_rows:,}"
        )


    print()
    print(
        "Parquet validation: PASS"
    )


    return actual_rows



# WRITE LANDSLIDE PARQUET CHUNK


def write_landslide_chunk(
    batches,
    chunk_number: int,
    chunk_dir: Path,
) -> int:
    """
    Write one Landslide Parquet chunk.

    Each chunk is independently finalized
    and validated.
    """

    output_path = (
        chunk_dir
        / (
            f"landslide_part_"
            f"{chunk_number:03d}.parquet"
        )
    )


    print()
    print("-" * 70)

    print(
        f"Writing Landslide chunk "
        f"{chunk_number:03d}"
    )

    print("-" * 70)


    writer = None
    rows_in_chunk = 0


    try:

      
        # Write batches
      

        for batch in batches:

            if batch.num_rows == 0:

                del batch

                continue


           
            # Create writer using first batch schema
           

            if writer is None:

                writer = pq.ParquetWriter(

                    output_path,

                    batch.schema,

                    compression="snappy",
                )


           
            # Write batch
           

            writer.write_batch(
                batch
            )


            rows_in_chunk += (
                batch.num_rows
            )


            del batch


    finally:

       
        # Finalize Parquet footer


        if writer is not None:

            writer.close()

            writer = None


    # VALIDATE CHUNK

    print()
    print(
        "Validating chunk..."
    )


    parquet_file = pq.ParquetFile(
        output_path
    )


    actual_rows = (
        parquet_file
        .metadata
        .num_rows
    )


    print(
        f"Expected rows: "
        f"{rows_in_chunk:,}"
    )

    print(
        f"Actual rows:   "
        f"{actual_rows:,}"
    )

    print(
        f"File:          "
        f"{output_path}"
    )


    if actual_rows != rows_in_chunk:

        raise RuntimeError(
            f"\nLandslide chunk "
            f"{chunk_number} validation failed.\n"
            f"Expected: {rows_in_chunk:,}\n"
            f"Found:    {actual_rows:,}"
        )


    print(
        "Chunk validation: PASS"
    )


    return actual_rows


# EXPORT FLOOD

def export_flood() -> None:
    """
    Export Flood 25-year Bronze GeoPackage
    into one Parquet file.
    """

    dataset = DATASETS["flood"]


    print()
    print("#" * 70)
    print(
        "NOAH FLOOD 25-YEAR → PARQUET"
    )
    print("#" * 70)


    # Read Bronze metadata

    source_info = get_source_info(

        dataset["input"],

        dataset["layer"],
    )


    expected_features = (
        source_info["features"]
    )

    # Create/reuse 2D GeoPackage

    create_2d_geopackage(

        dataset["input"],

        dataset["layer"],

        dataset["temp_2d"],
    )


    # Validate 2D GeoPackage

    validate_2d_geopackage(

        dataset["temp_2d"],

        dataset["layer"],

        expected_features,
    )


    # Export to Parquet

    actual_rows = export_single_parquet(

        dataset["temp_2d"],

        dataset["layer"],

        dataset["output"],
    )


    # Final validation

    if actual_rows != expected_features:

        raise RuntimeError(
            "\nFlood export failed.\n"
            f"Expected: {expected_features:,}\n"
            f"Found:    {actual_rows:,}"
        )


    print()
    print("=" * 70)
    print(
        "FLOOD PARQUET EXPORT: PASS"
    )
    print("=" * 70)

    print(
        f"Rows:   {actual_rows:,}"
    )

    print(
        f"Output: {dataset['output']}"
    )


# EXPORT LANDSLIDE

def export_landslide() -> None:
    """
    Export Landslide Bronze GeoPackage
    into 100-row Parquet chunks.
    """

    dataset = DATASETS["landslide"]


    input_path = dataset["input"]

    layer = dataset["layer"]

    temp_path = dataset["temp_2d"]

    chunk_dir = dataset["output_dir"]


    print()
    print("#" * 70)
    print(
        "NOAH LANDSLIDE → CHUNKED PARQUET"
    )
    print("#" * 70)


    # Read Bronze metadata

    source_info = get_source_info(

        input_path,

        layer,
    )


    expected_features = (
        source_info["features"]
    )


    # Create directories

    WSL_PARQUET_DIR.mkdir(

        parents=True,

        exist_ok=True,
    )


    chunk_dir.mkdir(

        parents=True,

        exist_ok=True,
    )

    # Create/reuse 2D GeoPackage
    create_2d_geopackage(

        input_path,

        layer,

        temp_path,
    )


    # Validate 2D GeoPackage

    validate_2d_geopackage(

        temp_path,

        layer,

        expected_features,
    )


    # Remove previous chunks

    existing_chunks = sorted(

        chunk_dir.glob(
            "landslide_part_*.parquet"
        )
    )


    if existing_chunks:

        print()
        print(
            "Removing previous Landslide "
            "Parquet chunks..."
        )


        for chunk in existing_chunks:

            print(
                f"Removing: "
                f"{chunk.name}"
            )

            chunk.unlink()


    # Start streaming

    print()
    print("=" * 70)
    print(
        "STREAMING LANDSLIDE 2D GEOPACKAGE"
    )
    print("=" * 70)


    rows_processed = 0

    chunk_number = 0

    chunk_batches = []

    chunk_rows = 0


    with pyogrio.open_arrow(

        temp_path,

        layer=layer,

        batch_size=ARROW_BATCH_SIZE,

        use_pyarrow=True,

    ) as (
        metadata,
        reader,
    ):

        print()
        print(
            f"Geometry type: "
            f"{metadata['geometry_type']}"
        )

        print(
            "Arrow reader created."
        )



        # Read Arrow batches


        while True:

            try:

                batch = (
                    reader
                    .read_next_batch()
                )

            except StopIteration:

                break


            if batch.num_rows == 0:

                del batch

                continue



            # Add batch to current chunk
    

            chunk_batches.append(
                batch
            )


            chunk_rows += (
                batch.num_rows
            )


            rows_processed += (
                batch.num_rows
            )


            print(

                f"Read: "
                f"{rows_processed:,}/"
                f"{expected_features:,} "
                f"| Current chunk: "
                f"{chunk_rows:,}/"
                f"{LANDSLIDE_CHUNK_SIZE}",

                flush=True,
            )


    
            # Write a 100-row chunk
        

            if (
                chunk_rows
                >= LANDSLIDE_CHUNK_SIZE
            ):

                chunk_number += 1


                write_landslide_chunk(

                    chunk_batches,

                    chunk_number,

                    chunk_dir,
                )

                # Release memory
            

                chunk_batches = []

                chunk_rows = 0

                gc.collect()


    # WRITE FINAL PARTIAL CHUNK

    if chunk_batches:

        chunk_number += 1


        write_landslide_chunk(

            chunk_batches,

            chunk_number,

            chunk_dir,
        )


        chunk_batches = []

        chunk_rows = 0

        gc.collect()


    # FINAL LANDSLIDE VALIDATION

    print()
    print("=" * 70)
    print(
        "LANDSLIDE FINAL VALIDATION"
    )
    print("=" * 70)


    chunk_files = sorted(

        chunk_dir.glob(
            "landslide_part_*.parquet"
        )
    )


    total_rows = 0


    for chunk_file in chunk_files:

        parquet_file = pq.ParquetFile(
            chunk_file
        )


        rows = (
            parquet_file
            .metadata
            .num_rows
        )


        print(

            f"{chunk_file.name}: "
            f"{rows:,} rows"
        )


        total_rows += rows


    print()
    print(
        f"Chunks created: "
        f"{len(chunk_files)}"
    )


    print(
        f"Total rows:     "
        f"{total_rows:,}"
    )


    print(
        f"Expected rows:  "
        f"{expected_features:,}"
    )


    # Validate total row count

    if total_rows != expected_features:

        raise RuntimeError(

            "\nLandslide Parquet export failed.\n"

            f"Expected: "
            f"{expected_features:,}\n"

            f"Found: "
            f"{total_rows:,}"
        )


    print()
    print("=" * 70)
    print(
        "LANDSLIDE PARQUET EXPORT: PASS"
    )
    print("=" * 70)


    print(
        f"Total rows: "
        f"{total_rows:,}"
    )


    print(
        f"Chunks: "
        f"{len(chunk_files)}"
    )


    print(
        f"Location: "
        f"{chunk_dir}"
    )

# EXPORT STORM SURGE

def export_storm_surge() -> None:
    """
    Export Storm Surge Bronze GeoPackage
    into one Parquet file.
    """

    dataset = DATASETS[
        "storm_surge"
    ]


    print()
    print("#" * 70)
    print(
        "NOAH STORM SURGE → PARQUET"
    )
    print("#" * 70)

    # Read Bronze metadata

    source_info = get_source_info(

        dataset["input"],

        dataset["layer"],
    )


    expected_features = (
        source_info["features"]
    )

    # Create/reuse 2D GeoPackage

    create_2d_geopackage(

        dataset["input"],

        dataset["layer"],

        dataset["temp_2d"],
    )

    # Validate 2D GeoPackage

    validate_2d_geopackage(

        dataset["temp_2d"],

        dataset["layer"],

        expected_features,
    )



    # Export to Parquet

    actual_rows = export_single_parquet(

        dataset["temp_2d"],

        dataset["layer"],

        dataset["output"],
    )


    # Final validation

    if actual_rows != expected_features:

        raise RuntimeError(

            "\nStorm Surge export failed.\n"

            f"Expected: "
            f"{expected_features:,}\n"

            f"Found: "
            f"{actual_rows:,}"
        )


    print()
    print("=" * 70)
    print(
        "STORM SURGE PARQUET EXPORT: PASS"
    )
    print("=" * 70)


    print(
        f"Rows:   "
        f"{actual_rows:,}"
    )


    print(
        f"Output: "
        f"{dataset['output']}"
    )


# FINAL SUMMARY

def print_final_summary() -> None:
    """
    Print the final Parquet output locations.
    """

    print()
    print()
    print("#" * 70)
    print(
        "ALL NOAH PARQUET EXPORTS COMPLETE"
    )
    print("#" * 70)


    print()
    print("Flood 25-year:")
    print(
        f"  {DATASETS['flood']['output']}"
    )


    print()
    print("Landslide:")
    print(
        f"  {DATASETS['landslide']['output_dir']}"
    )


    print()
    print("Storm Surge:")
    print(
        f"  {DATASETS['storm_surge']['output']}"
    )


    print()
    print("Expected source feature counts:")

    print(
        "  Flood 25-year: 227"
    )

    print(
        "  Landslide:     2,692"
    )

    print(
        "  Storm Surge:   804"
    )


    print()
    print(
        "Next step:"
    )

    print(
        "Upload the Parquet files to "
        "Databricks for Delta Bronze ingestion."
    )


# MAIN
def main() -> None:
    """
    Run all NOAH Parquet exports.
    """

    print()
    print("=" * 70)
    print(
        "AHON - NOAH PARQUET EXPORT"
    )
    print("=" * 70)


    # 1. FLOOD

    export_flood()

    # 2. LANDSLIDE

    export_landslide()



    # 3. STORM SURGE

    export_storm_surge()


    # FINAL SUMMARY

    print_final_summary()


# SCRIPT ENTRY POINT

if __name__ == "__main__":

    main()
