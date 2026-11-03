"""Read-only queries against Neon (role etl_reader), returned as JSON Lines."""

from __future__ import annotations

import json

import psycopg2

from hospedaplus.lake import neon_url


def dump(sql: str, params: tuple = ()) -> tuple[bytes, int]:
    with psycopg2.connect(neon_url()) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c.name for c in cur.description]
        rows = cur.fetchall()
    lines = [json.dumps(dict(zip(cols, r)), default=str, ensure_ascii=False) for r in rows]
    return ("\n".join(lines) + ("\n" if lines else "")).encode(), len(rows)
