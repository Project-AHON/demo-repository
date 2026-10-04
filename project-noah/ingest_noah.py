from pathlib import Path
from datetime import datetime, timezone
import subprocess

# PATHS

DATASET_DIR = Path("data/raw/noah/datasets")
BRONZE_DIR = Path("data/bronze/noah")

# SOURCE DATASETS

FLOOD_DIR = (
    DATASET_DIR
    / "NOAH-Flood-Hazard"
    / "25-year"
)

LANDSLIDE_DIR = (
    DATASET_DIR
    / "NOAH-Landslide-Hazard"
    / "LandslideHazards"
)

STORM_SURGE_DIR = (
    DATASET_DIR
    / "NOAH-Storm-Surge"
)

# BRONZE OUTPUTS

FLOOD_OUTPUT = (
    BRONZE_DIR
    / "flood_25yr"
    / "flood_25yr.gpkg"
)

LANDSLIDE_OUTPUT = (
    BRONZE_DIR
    / "landslide"
    / "landslide.gpkg"
)

STORM_SURGE_OUTPUT = (
    BRONZE_DIR
    / "storm_surge"
    / "storm_surge.gpkg"
)

# BRONZE LAYER NAMES

FLOOD_LAYER = "bronze_noah_flood"

LANDSLIDE_LAYER = "bronze_noah_landslide"

STORM_SURGE_LAYER = "bronze_noah_storm_surge"


# COMMON SETTINGS

TARGET_CRS = "EPSG:4326"

# Make all polygon geometries MultiPolygon in Bronze.
# This allows Polygon and MultiPolygon source files
# to be stored consistently in the same GeoPackage layer.
GEOMETRY_TYPE = "MULTIPOLYGON25D"

# HELPERS

def run_ogr2ogr(
    command: list[str],
) -> None:
    """Run an ogr2ogr command."""

    subprocess.run(
        command,
        check=True,
    )


def ensure_output_directory(
    output_path: Path,
) -> None:
    """Create the output directory if it does not exist."""

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )


def get_ingest_timestamp() -> str:
    """Return the UTC ingestion timestamp."""

    return datetime.now(
        timezone.utc
    ).isoformat()

# FLOOD

def ingest_flood() -> None:
    """
    Ingest 25-year Flood Shapefiles into Bronze.

    Source field:
        Var

    Bronze field:
        var

    Hazard values:
        1 = Low
        2 = Medium
        3 = High
    """

    print("\n" + "=" * 80)
    print("INGESTING FLOOD DATA")
    print("=" * 80)

    shapefiles = sorted(
        FLOOD_DIR.glob("*.shp")
    )

    if not shapefiles:
        raise FileNotFoundError(
            f"No Flood Shapefiles found in {FLOOD_DIR}"
        )

    ensure_output_directory(
        FLOOD_OUTPUT
    )

    first_file = True

    ingest_timestamp = get_ingest_timestamp()

    for shapefile in shapefiles:

        print(
            f"Flood: {shapefile.name}"
        )

        if first_file:

            command = [
                "ogr2ogr",
                "-f",
                "GPKG",
                "-nln",
                FLOOD_LAYER,
                "-nlt",
                GEOMETRY_TYPE,
                str(FLOOD_OUTPUT),
                str(shapefile),
                "-t_srs",
                TARGET_CRS,
                "-sql",
                (
                    "SELECT "
                    "Var AS var, "
                    f"'{shapefile.name}' AS source_file, "
                    "'FLOOD' AS source_dataset, "
                    "'25YEAR' AS scenario, "
                    "'Var' AS source_field, "
                    f"'{TARGET_CRS}' AS source_crs, "
                    f"'{ingest_timestamp}' AS ingest_timestamp "
                    "FROM "
                    f'"{shapefile.stem}"'
                ),
            ]

        else:

            command = [
                "ogr2ogr",
                "-update",
                "-append",
                "-nln",
                FLOOD_LAYER,
                "-nlt",
                GEOMETRY_TYPE,
                str(FLOOD_OUTPUT),
                str(shapefile),
                "-t_srs",
                TARGET_CRS,
                "-sql",
                (
                    "SELECT "
                    "Var AS var, "
                    f"'{shapefile.name}' AS source_file, "
                    "'FLOOD' AS source_dataset, "
                    "'25YEAR' AS scenario, "
                    "'Var' AS source_field, "
                    f"'{TARGET_CRS}' AS source_crs, "
                    f"'{ingest_timestamp}' AS ingest_timestamp "
                    "FROM "
                    f'"{shapefile.stem}"'
                ),
            ]

        run_ogr2ogr(command)

        first_file = False

    print(
        f"\nFlood Bronze complete: "
        f"{FLOOD_OUTPUT}"
    )

# LANDSLIDE

def ingest_landslide() -> None:
    """
    Ingest Landslide Shapefiles into Bronze.

    Actual source field:
        LH

    Documentation refers to:
        HAZ

    We preserve the actual source field LH.

    Hazard values:
        1 = Low
        2 = Medium
        3 = High
    """

    print("\n" + "=" * 80)
    print("INGESTING LANDSLIDE DATA")
    print("=" * 80)

    shapefiles = sorted(
        LANDSLIDE_DIR.glob("*.shp")
    )

    if not shapefiles:
        raise FileNotFoundError(
            "No Landslide Shapefiles found in "
            f"{LANDSLIDE_DIR}"
        )

    ensure_output_directory(
        LANDSLIDE_OUTPUT
    )

    first_file = True

    ingest_timestamp = get_ingest_timestamp()

    for shapefile in shapefiles:

        print(
            f"Landslide: {shapefile.name}"
        )

        if first_file:

            command = [
                "ogr2ogr",
                "-f",
                "GPKG",
                "-nln",
                LANDSLIDE_LAYER,
                "-nlt",
                GEOMETRY_TYPE,
                str(LANDSLIDE_OUTPUT),
                str(shapefile),
                "-t_srs",
                TARGET_CRS,
                "-sql",
                (
                    "SELECT "
                    "LH AS lh, "
                    f"'{shapefile.name}' AS source_file, "
                    "'LANDSLIDE' AS source_dataset, "
                    "'NONE' AS scenario, "
                    "'LH' AS source_field, "
                    f"'{TARGET_CRS}' AS source_crs, "
                    f"'{ingest_timestamp}' AS ingest_timestamp "
                    "FROM "
                    f'"{shapefile.stem}"'
                ),
            ]

        else:

            command = [
                "ogr2ogr",
                "-update",
                "-append",
                "-nln",
                LANDSLIDE_LAYER,
                "-nlt",
                GEOMETRY_TYPE,
                str(LANDSLIDE_OUTPUT),
                str(shapefile),
                "-t_srs",
                TARGET_CRS,
                "-sql",
                (
                    "SELECT "
                    "LH AS lh, "
                    f"'{shapefile.name}' AS source_file, "
                    "'LANDSLIDE' AS source_dataset, "
                    "'NONE' AS scenario, "
                    "'LH' AS source_field, "
                    f"'{TARGET_CRS}' AS source_crs, "
                    f"'{ingest_timestamp}' AS ingest_timestamp "
                    "FROM "
                    f'"{shapefile.stem}"'
                ),
            ]

        run_ogr2ogr(command)

        first_file = False

    print(
        f"\nLandslide Bronze complete: "
        f"{LANDSLIDE_OUTPUT}"
    )

# STORM SURGE

def ingest_storm_surge() -> None:
    """
    Ingest Storm Surge Shapefiles into Bronze.

    Source field:
        HAZ

    Advisory scenarios:
        SSA1
        SSA2
        SSA3
        SSA4

    HAZ and advisory_level are kept separate because
    they represent different concepts.
    """

    print("\n" + "=" * 80)
    print("INGESTING STORM SURGE DATA")
    print("=" * 80)

    shapefiles = []

    for advisory_dir in sorted(
        STORM_SURGE_DIR.glob("SSA*")
    ):

        advisory_level = advisory_dir.name

        for shapefile in sorted(
            advisory_dir.glob("*.shp")
        ):

            shapefiles.append(
                (
                    advisory_level,
                    shapefile,
                )
            )

    if not shapefiles:
        raise FileNotFoundError(
            "No Storm Surge Shapefiles found in "
            f"{STORM_SURGE_DIR}"
        )

    ensure_output_directory(
        STORM_SURGE_OUTPUT
    )

    first_file = True

    ingest_timestamp = get_ingest_timestamp()

    for advisory_level, shapefile in shapefiles:

        print(
            f"Storm Surge: "
            f"{advisory_level} - "
            f"{shapefile.name}"
        )

        if first_file:

            command = [
                "ogr2ogr",
                "-f",
                "GPKG",
                "-nln",
                STORM_SURGE_LAYER,
                "-nlt",
                GEOMETRY_TYPE,
                str(STORM_SURGE_OUTPUT),
                str(shapefile),
                "-t_srs",
                TARGET_CRS,
                "-sql",
                (
                    "SELECT "
                    "HAZ AS haz, "
                    f"'{advisory_level}' AS advisory_level, "
                    f"'{shapefile.name}' AS source_file, "
                    "'STORM_SURGE' AS source_dataset, "
                    "'HAZ' AS source_field, "
                    f"'{TARGET_CRS}' AS source_crs, "
                    f"'{ingest_timestamp}' AS ingest_timestamp "
                    "FROM "
                    f'"{shapefile.stem}"'
                ),
            ]

        else:

            command = [
                "ogr2ogr",
                "-update",
                "-append",
                "-nln",
                STORM_SURGE_LAYER,
                "-nlt",
                GEOMETRY_TYPE,
                str(STORM_SURGE_OUTPUT),
                str(shapefile),
                "-t_srs",
                TARGET_CRS,
                "-sql",
                (
                    "SELECT "
                    "HAZ AS haz, "
                    f"'{advisory_level}' AS advisory_level, "
                    f"'{shapefile.name}' AS source_file, "
                    "'STORM_SURGE' AS source_dataset, "
                    "'HAZ' AS source_field, "
                    f"'{TARGET_CRS}' AS source_crs, "
                    f"'{ingest_timestamp}' AS ingest_timestamp "
                    "FROM "
                    f'"{shapefile.stem}"'
                ),
            ]

        run_ogr2ogr(command)

        first_file = False

    print(
        f"\nStorm Surge Bronze complete: "
        f"{STORM_SURGE_OUTPUT}"
    )

# MAIN

def main() -> None:
    """Run all NOAH Bronze ingestion pipelines."""

    print("=" * 80)
    print("AHON - NOAH BRONZE INGESTION")
    print("=" * 80)

    ingest_flood()

    ingest_landslide()

    ingest_storm_surge()

    print("\n" + "=" * 80)
    print("ALL NOAH BRONZE INGESTION COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
