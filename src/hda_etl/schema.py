import polars as pl

MIX_SOURCES = {
    "nuclear",
    "geothermal",
    "biomass",
    "coal",
    "wind",
    "solar",
    "hydro",
    "gas",
    "oil",
    "unknown",
}

COMMON_EXPECTED_DTYPES = {
    "zone": pl.String,
    "datetime": pl.Datetime(time_zone="UTC"),
    "updatedAt": pl.Datetime(time_zone="UTC"),
    "ingestion_timestamp": pl.Datetime(time_zone="UTC"),
    "temporalGranularity": pl.String,
    "year": pl.Int32,
    "month": pl.Int8,
    "day": pl.Int8,
}

EXPECTED_DTYPES_MIX = {
    "isEstimated": pl.Boolean,
    "estimationMethod": pl.String,
    "breakdownType": pl.String,
    **{src: pl.Float64 for src in MIX_SOURCES},
    "hydro_storage_charge": pl.Float64,
    "hydro_storage_discharge": pl.Float64,
    "battery_storage_charge": pl.Float64,
    "battery_storage_discharge": pl.Float64,
    "flows_imports": pl.Float64,
    "flows_exports": pl.Float64,
    **COMMON_EXPECTED_DTYPES,
}

EXPECTED_DTYPES_FLOWS = {
    "direction": pl.String,
    "flow_zone": pl.String,
    "power_mw": pl.Float64,
    **COMMON_EXPECTED_DTYPES,
}
