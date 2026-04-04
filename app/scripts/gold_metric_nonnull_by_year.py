from __future__ import annotations

"""
Print per-year non-null counts for key gold metrics from the web-exported JSON.

Run:
  python app/scripts/gold_metric_nonnull_by_year.py
"""

import json
from pathlib import Path


def main() -> None:
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
