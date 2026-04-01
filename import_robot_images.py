from __future__ import annotations

import sys
import re
from pathlib import Path

import pandas as pd
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    Text,
    ForeignKey,
    create_engine,
    select,
    text,
)
from sqlalchemy.engine import Engine

from config import DATABASE_URL


def normalize_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^\w]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "unnamed"
    if name[0].isdigit():
        name = f"col_{name}"
    return name


def create_robot_images_table(engine: Engine) -> Table:
    metadata = MetaData()

    robot_embodiment = Table(
        "robot_embodiment",
        metadata,
        autoload_with=engine,
    )

    robot_images = Table(
        "robot_images",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column(
            "robot_id",
            Integer,
            ForeignKey(robot_embodiment.c.robot_id, ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        Column("thumbnail_file", Text, nullable=True),
        Column("image_file", Text, nullable=True),
        Column("description", Text, nullable=True),
    )

    metadata.create_all(engine, tables=[robot_images], checkfirst=True)
    return robot_images


def robot_id_exists(conn, robot_id: int) -> bool:
    metadata = MetaData()
    robot_embodiment = Table("robot_embodiment", metadata, autoload_with=conn)

    stmt = select(robot_embodiment.c.robot_id).where(
        robot_embodiment.c.robot_id == robot_id
    )
    row = conn.execute(stmt).fetchone()
    return row is not None


def import_robot_images(engine: Engine, manifest_csv: Path) -> None:
    df = pd.read_csv(manifest_csv)
    df.columns = [normalize_name(col) for col in df.columns]

    required_cols = {"robot_id", "thumbnail_file", "image_file"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Manifest missing required columns: {sorted(missing)}")

    robot_images = create_robot_images_table(engine)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE robot_images RESTART IDENTITY"))

        for _, row in df.iterrows():
            robot_id = int(row["robot_id"])
            thumbnail_file = str(row["thumbnail_file"]).strip() if pd.notna(row["thumbnail_file"]) else None
            image_file = str(row["image_file"]).strip() if pd.notna(row["image_file"]) else None
            description = str(row["description"]).strip() if "description" in df.columns and pd.notna(row["description"]) else None

            if not robot_id_exists(conn, robot_id):
                print(f"Skipping robot_id {robot_id}: not found in robot_embodiment")
                continue

            conn.execute(
                robot_images.insert().values(
                    robot_id=robot_id,
                    thumbnail_file=thumbnail_file or None,
                    image_file=image_file or None,
                    description=description,
                )
            )

            print(f"Imported robot_id={robot_id} thumb={thumbnail_file} image={image_file}")


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python import_robot_images.py /path/to/robot_image_manifest.csv")
        sys.exit(1)

    manifest_csv = Path(sys.argv[1]).expanduser().resolve()

    if not manifest_csv.exists():
        print(f"Manifest CSV not found: {manifest_csv}")
        sys.exit(1)

    engine = create_engine(DATABASE_URL, future=True)

    create_robot_images_table(engine)
    import_robot_images(engine, manifest_csv)
    print("Done.")


if __name__ == "__main__":
    main()