"""A small end-to-end check for the native file-based pipeline."""
import csv
import importlib
import json
import os
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from scripts.run_local import transform


class LocalModeTest(unittest.TestCase):
    def test_builds_gold_files_and_quality_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            month = root / "bronze" / "taxi_trips" / "month=2023-01"
            month.mkdir(parents=True)
            with (month / "page=00001.csv").open("w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=[
                    "trip_id", "trip_start_timestamp", "trip_end_timestamp", "trip_seconds",
                    "trip_miles", "pickup_community_area", "fare", "tips", "trip_total", "payment_type",
                ])
                writer.writeheader()
                writer.writerow({"trip_id": "one", "trip_start_timestamp": "2023-01-02T12:00:00.000",
                                 "trip_end_timestamp": "2023-01-02T12:10:00.000", "trip_seconds": 600,
                                 "trip_miles": 2, "pickup_community_area": 1, "fare": 10, "tips": 2,
                                 "trip_total": 12, "payment_type": "Credit Card"})
                writer.writerow({"trip_id": "bad", "trip_start_timestamp": "2023-01-02T12:00:00.000",
                                 "trip_end_timestamp": "2023-01-02T12:10:00.000", "trip_seconds": 0,
                                 "trip_miles": 2, "pickup_community_area": 1, "fare": 10, "tips": 2,
                                 "trip_total": 12, "payment_type": "Credit Card"})
            zones = root / "bronze" / "reference" / "community_areas"
            zones.mkdir(parents=True)
            zones.joinpath("community_areas.csv").write_text("community_area,community_name\n1,Rogers Park\n")
            from datetime import date
            transform(root, date(2023, 1, 1), date(2023, 2, 1), 0.6)
            report = json.loads((root / "silver" / "_quality" / "latest.json").read_text())
            self.assertEqual((report["rows_in"], report["rows_clean"]), (2, 1))
            for table in ("daily_kpis", "top_pickup_zones", "hourly_demand", "payment_mix"):
                self.assertTrue((root / "gold" / table / "data.parquet").exists())
            with mock.patch.dict(os.environ, {"STORAGE_BACKEND": "local", "DATA_DIR": str(root)}):
                from api import main
                importlib.reload(main)
                self.assertTrue(main.health()["ready"])
                self.assertEqual(main.tables()["daily_kpis"], 1)
                self.assertEqual(main.kpis("daily_kpis")[0]["trips"], 1)
                self.assertEqual(main.quality()["rows_clean"], 1)


if __name__ == "__main__":
    unittest.main()
