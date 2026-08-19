from datetime import datetime

from src.pipeline import calculate_daily_summary, clean_data, identify_anomalies


def test_duplicate_records_are_removed(spark):
    rows = [
        (datetime(2022, 3, 1, 0), 1, 10.0, 100, 2.0),
        (datetime(2022, 3, 1, 0), 1, 10.0, 100, 2.0),
        (datetime(2022, 3, 1, 1), 1, 11.0, 110, 2.2),
    ]
    df = spark.createDataFrame(
        rows,
        ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"],
    )

    assert clean_data(df).count() == 2


def test_missing_and_invalid_measurements_are_imputed(spark):
    rows = [
        (datetime(2022, 3, 1, 0), 1, 10.0, 100, 2.0),
        (datetime(2022, 3, 1, 1), 1, 11.0, 110, 2.2),
        (datetime(2022, 3, 1, 2), 1, None, 120, -1.0),
    ]
    df = spark.createDataFrame(
        rows,
        ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"],
    )

    cleaned = clean_data(df).orderBy("timestamp").collect()
    final = cleaned[-1]

    assert final.wind_speed is not None
    assert final.power_output is not None
    assert final.power_output >= 0


def test_daily_summary_and_missing_readings(spark):
    rows = [
        (datetime(2022, 3, 1, 0), 1, 10.0, 100, 2.0),
        (datetime(2022, 3, 1, 1), 1, 10.0, 100, 3.0),
        (datetime(2022, 3, 1, 2), 1, 10.0, 100, 4.0),
    ]
    df = spark.createDataFrame(
        rows,
        ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"],
    )

    result = calculate_daily_summary(df, expected_readings=4).first()

    assert result.min_power_output == 2.0
    assert result.max_power_output == 4.0
    assert result.avg_power_output == 3.0
    assert result.reading_count == 3
    assert result.missing_readings == 1
    assert result.is_complete_period is False


def test_high_and_low_turbines_are_flagged_as_anomalies(spark):
    # 10 normal turbine averages around 3 MW and two extreme turbines.
    rows = []
    normal_values = [2.9, 3.0, 3.1, 3.0, 2.95, 3.05, 3.0, 2.98, 3.02, 3.0]
    for turbine_id, value in enumerate(normal_values, start=1):
        rows.append(("2022-03-01", turbine_id, value, 24))
    rows.extend(
        [
            ("2022-03-01", 11, 0.5, 24),
            ("2022-03-01", 12, 5.5, 24),
        ]
    )

    summary = spark.createDataFrame(
        rows, ["date", "turbine_id", "avg_power_output", "reading_count"]
    ).selectExpr(
        "to_date(date) as date",
        "turbine_id",
        "avg_power_output",
        "reading_count",
        "0.0 as min_power_output",
        "0.0 as max_power_output",
        "0.0 as stddev_power_output",
        "0 as missing_readings",
        "true as is_complete_period",
    )

    flagged = {
        row.turbine_id
        for row in identify_anomalies(summary).filter("is_anomaly").collect()
    }

    assert 11 in flagged
    assert 12 in flagged
