"""
Print per-year non-null counts for key Gold metrics from the web-exported JSON.

This is a quick data-quality sanity check for the frontend dataset.

Run:
  ``PYTHONPATH=. python app/scripts/gold_metric_nonnull_by_year.py``
"""

from __future__ import annotations

import json
from pathlib import Path


def main() -> None:
    """
    Load the exported JSON and report how many UFs have non-null values per year.

    Also prints whether a GeoJSON file exists (used by the web map).
    """
    p = Path("exports/web/gold_pbf_estados_df_geo.json")
    rows = json.loads(p.read_text(encoding="utf-8"))
    years = sorted({r["Ano"] for r in rows})

    def non_null_count(year: int, key: str) -> int:
        return sum(1 for r in rows if r["Ano"] == year and r.get(key) is not None)

    print("Years:", years)
    for key in ["valor_2021", "pbfPerBenef", "pbfPerCapita"]:
        print("\nNon-null counts for", key)
        for y in years:
            print(y, non_null_count(y, key))

    geo = Path("exports/web/brazil-states.geojson")
    print("\nGeoJSON exists:", geo.exists(), "-", geo)


if __name__ == "__main__":
    main()
