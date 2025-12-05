import pandas as pd
from pathlib import Path

DATA_PATH = Path(__file__).parents[1] / "data" / "processed" / "oecd_energy_fact.csv"


# DATA LOADING


def load_df():
    df = pd.read_csv(DATA_PATH)
    return df


# TEST 1 — BASIC SANITY


def test_file_loads():
    df = load_df()
    assert len(df) > 10000

def test_core_columns_exist():
    df = load_df()
    required_cols = [
        "Country", "Time", "Balance", "Product",
        "product_clean", "fuel_group", "is_atomic_fuel", "Value"
    ]

    for col in required_cols:
        assert col in df.columns


# Test 3 - 
def test_clean_column_names_basic():
    import pandas as pd
    from capstone_etl.transform.transform import clean_column_names

    df = pd.DataFrame({
        " Country Name ": [1],
        "Display-Order": [2],
        "Year To Date": [3]
    })

    cleaned = clean_column_names(df)

    expected = {"country_name", "display_order", "year_to_date"}

    assert set(cleaned.columns) == expected


def test_standardise_dataset_2_transforms_fields_correctly():
    import pandas as pd
    from capstone_etl.transform.transform import standardise_dataset_2

    raw = pd.DataFrame({
        "Country": ["France"],
        "Time": ["Aug-25"],
        "Balance": ["Net Electricity Production"],
        "Product": ["Hydro"],
        "Value": ["123.45"],
        "Unit": ["GWh"]
    })

    cleaned = standardise_dataset_2(raw)

    # Check date parsing
    assert cleaned.loc[0, "year"] == 2025
    assert cleaned.loc[0, "month"] == 8
    assert pd.api.types.is_datetime64_any_dtype(cleaned["time"])

    # Check column cleanup
    assert "unit" not in cleaned.columns

    # Check type coercion
    assert cleaned.loc[0, "value"] == 123.45
    assert type(cleaned.loc[0, "value"]) is float



# # TEST 2.1 — OECD FILTERING


# def test_only_oecd_countries_present():
#     df = load_df()

#     disallowed = ["Argentina", "Brazil", "China", "India"]

#     for c in disallowed:
#         assert c not in df["Country"].unique()


# # TEST 3 — FUEL GROUPS VALID


# def test_valid_fuel_groups():
#     df = load_df()

#     valid_groups = {"LOW_CARBON", "NUCLEAR", "FOSSIL", "OTHER"}

#     assert set(df["fuel_group"].unique()).issubset(valid_groups)


# # TEST 4 — KPI MATH SANITY


# def test_energy_mix_percentages_sum_to_100():
#     df = load_df()
#     atomic = df[df["is_atomic_fuel"]]

#     totals = atomic.groupby(["Country", "Time"])["Value"].sum().reset_index()

#     mix = (
#         atomic.groupby(["Country", "Time", "fuel_group"])["Value"]
#         .sum()
#         .reset_index()
#         .pivot(index=["Country", "Time"], columns="fuel_group", values="Value")
#         .fillna(0)
#     )

#     mix["sum_groups"] = mix.sum(axis=1)

#     check = totals.merge(
#         mix["sum_groups"],
#         left_on=["Country","Time"],
#         right_index=True
#     )

#     pct_diff = abs(check["Value"] - check["sum_groups"]) / check["Value"]

#     # allow up to 1% rounding tolerance
#     assert (pct_diff < 0.01).all()


# # TEST 5 — RECONCILIATION


# def test_iea_electricity_totals_reasonable():
#     df = load_df()

#     atomic = df[df["is_atomic_fuel"]]
#     totals_atomic = (
#         atomic.groupby(["Country", "Time"])["Value"]
#         .sum()
#         .reset_index(name="atomic_sum")
#     )

#     electricity_rows = df[df["product_clean"] == "Electricity"]

#     recon = totals_atomic.merge(
#         electricity_rows,
#         on=["Country", "Time"],
#         how="inner"
#     )

#     delta_pct = abs(recon["atomic_sum"] - recon["Value"]) / recon["Value"]

#     # accept small data inconsistencies
#     assert (delta_pct < 0.02).all()
