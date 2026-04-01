from flask import Flask, jsonify, render_template, request, Response
from sqlalchemy import inspect, text
from db import engine
from ui_config import TABLE_CONFIG

app = Flask(__name__)

NA_STRINGS = {"", "na", "n/a", "null", "none"}

def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def is_effectively_na(v) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    return s == "" or s.lower() in NA_STRINGS

def list_tables():
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema="public")

    HIDDEN_TABLES = {
        "robot_embodiment",
        "robot_manifest",
        "robot_images",
    }

    return sorted([t for t in tables if t not in HIDDEN_TABLES])

def get_columns(table: str):
    inspector = inspect(engine)
    return inspector.get_columns(table, schema="public")

@app.get("/")
def home():
    return render_template("index.html")

@app.get("/api/tables")
def api_tables():
    return jsonify(tables=list_tables())

# --- NEW: serve UI config for a given table ---
@app.get("/api/ui_config")
def api_ui_config():
    table = request.args.get("table", "").strip()
    if not table:
        return jsonify(error="table is required"), 400

    config = TABLE_CONFIG.get(table, {})

    return jsonify(
        table=table,
        card_fields=config.get("card_fields", []),
        modal_groups=config.get("modal_groups", []),
    )

@app.get("/api/filter_options")
def api_filter_options():
    table = request.args.get("table", "").strip()
    if not table:
        return jsonify(error="table is required"), 400

    if table not in list_tables():
        return jsonify(error=f"Unknown table '{table}'"), 400

    cols = get_columns(table)
    filters = []

    with engine.connect() as conn:
        for col in cols:
            name = col["name"]
            if name == "id":
                continue

            type_name = str(col["type"]).lower()

            if any(t in type_name for t in ["integer", "numeric", "float", "double", "real"]):
                row = conn.execute(text(f"""
                    SELECT MIN({qident(name)}) AS min_val,
                           MAX({qident(name)}) AS max_val
                    FROM {qident(table)}
                    WHERE {qident(name)} IS NOT NULL
                """)).mappings().first()

                if row and row["min_val"] is not None and row["max_val"] is not None:
                    filters.append({
                        "column": name,
                        "kind": "numeric",
                        "min": row["min_val"],
                        "max": row["max_val"]
                    })
            else:
                vals = conn.execute(text(f"""
                    SELECT DISTINCT {qident(name)} AS v
                    FROM {qident(table)}
                    WHERE {qident(name)} IS NOT NULL
                    ORDER BY {qident(name)}
                    LIMIT 50
                """)).scalars().all()

                vals = [v for v in vals if not is_effectively_na(v)]

                if 1 < len(vals) <= 25:
                    filters.append({
                        "column": name,
                        "kind": "categorical",
                        "options": vals
                    })

    return jsonify(table=table, filters=filters)

@app.post("/api/query")
def api_query():
    payload = request.get_json(silent=True) or {}
    table = (payload.get("table") or "").strip()
    search = (payload.get("search") or "").strip()
    filters = payload.get("filters") or {}
    ranges = payload.get("ranges") or {}

    if not table:
        return jsonify(error="table is required"), 400
    if table not in list_tables():
        return jsonify(error=f"Unknown table '{table}'"), 400

    cols = [c["name"] for c in get_columns(table)]
    show_cols = [c for c in cols if c != "id"]

    where_parts = []
    params = {}

    if search:
        search_parts = []
        for i, col in enumerate(show_cols):
            key = f"search_{i}"
            search_parts.append(f"CAST({qident(col)} AS TEXT) ILIKE :{key}")
            params[key] = f"%{search}%"
        if search_parts:
            where_parts.append("(" + " OR ".join(search_parts) + ")")

    for col, values in filters.items():
        if col not in cols or not values:
            continue
        placeholders = []
        for i, val in enumerate(values):
            key = f"{col}_v{i}"
            placeholders.append(f":{key}")
            params[key] = val
        where_parts.append(f"{qident(col)} IN ({', '.join(placeholders)})")

    for col, bounds in ranges.items():
        if col not in cols or not isinstance(bounds, dict):
            continue
        min_val = bounds.get("min")
        max_val = bounds.get("max")
        if min_val is not None:
            key = f"{col}_min"
            where_parts.append(f"{qident(col)} >= :{key}")
            params[key] = min_val
        if max_val is not None:
            key = f"{col}_max"
            where_parts.append(f"{qident(col)} <= :{key}")
            params[key] = max_val

    sql = f"SELECT * FROM {qident(table)}"
    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)
    sql += " LIMIT 500"

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    rows = [dict(r) for r in rows]
    trimmed_rows = [{c: r.get(c, "") for c in show_cols} for r in rows]
    
    # Add images for any table that has robot_id
    if "robot_id" in show_cols:

        robot_ids = [
            row.get("robot_id")
            for row in trimmed_rows
            if row.get("robot_id") is not None and str(row.get("robot_id")).strip() != ""
        ]

        image_map = {}

        if robot_ids:
            placeholders = ", ".join([f":rid_{i}" for i in range(len(robot_ids))])
            params_img = {f"rid_{i}": rid for i, rid in enumerate(robot_ids)}

            sql_img = text(f"""
                SELECT robot_id, thumbnail_file, image_file
                FROM robot_images
                WHERE robot_id IN ({placeholders})
            """)

            with engine.connect() as conn:
                img_rows = conn.execute(sql_img, params_img).mappings().all()

            image_map = {
                row["robot_id"]: {
                    "thumbnail_file": row["thumbnail_file"],
                    "image_file": row["image_file"],
                }
                for row in img_rows
            }

        for row in trimmed_rows:
            robot_id = row.get("robot_id")
            img = image_map.get(robot_id)

            if img and img.get("thumbnail_file"):
                row["image_url"] = f"/static/robot_thumbnails/{img['thumbnail_file']}"
            else:
                row["image_url"] = None

            if img and img.get("image_file"):
                row["full_image_url"] = f"/static/robot_images/{img['image_file']}"
            else:
                row["full_image_url"] = None

    return jsonify(columns=show_cols, rows=trimmed_rows)

if __name__ == "__main__":
    app.run(debug=True)
