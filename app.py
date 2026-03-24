from flask import Flask, jsonify, render_template, request
from sqlalchemy import inspect, text
from db import engine

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
    return sorted(inspector.get_table_names(schema="public"))

def get_columns(table: str):
    inspector = inspect(engine)
    return inspector.get_columns(table, schema="public")

@app.get("/")
def home():
    return render_template("index.html")

@app.get("/api/tables")
def api_tables():
    return jsonify(tables=list_tables())

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

    searchable_cols = [c for c in show_cols]

    if search:
        search_parts = []
        for i, col in enumerate(searchable_cols):
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

    return jsonify(columns=show_cols, rows=trimmed_rows)

if __name__ == "__main__":
    app.run(debug=True)