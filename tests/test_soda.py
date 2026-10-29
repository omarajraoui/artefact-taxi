from datetime import date

from pipeline.ingest.soda import csv_rows, month_windows, soda_params


def test_month_windows_cover_the_period_half_open():
    assert month_windows(date(2023, 1, 1), date(2023, 4, 1)) == [
        (date(2023, 1, 1), date(2023, 2, 1)),
        (date(2023, 2, 1), date(2023, 3, 1)),
        (date(2023, 3, 1), date(2023, 4, 1)),
    ]
    assert month_windows(date(2023, 12, 15), date(2024, 1, 10)) == [(date(2023, 12, 15), date(2024, 1, 1)), (date(2024, 1, 1), date(2024, 1, 10))]
    assert month_windows(date(2023, 1, 1), date(2023, 1, 1)) == []


def test_soda_page_is_filtered_and_stably_ordered():
    p = soda_params(date(2023, 1, 1), date(2023, 2, 1), 50000, 100000)
    assert p["$where"] == "trip_start_timestamp >= '2023-01-01T00:00:00' AND trip_start_timestamp < '2023-02-01T00:00:00'"
    assert p["$order"] == ":id"
    assert (p["$limit"], p["$offset"]) == ("50000", "100000")


def test_csv_rows_counts_data_rows_and_quoted_newlines():
    assert csv_rows(b'"a","b"\n"1","x"\n"2","multi\nline"\n') == 2
    assert csv_rows(b'"a","b"\n') == 0
