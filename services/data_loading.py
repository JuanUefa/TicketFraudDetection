# services/data_loading.py
 
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import col, lower, upper, trim
from utils.logging_config import get_logger
 
logger = get_logger("data_loading")
 
def data_loading(session: Session, ds_lottery: str, n_points: int, run_id: str) -> tuple[DataFrame, DataFrame]:
    logger.info(f"[{run_id}] START - data_loading")
 
    df_country_iso = (
        session.table("COUNTRY_CODE_MAPPING")
        .filter(col("COUNTRY_NAME").is_not_null() & col("ISOCODE").is_not_null())
        .select(
            trim(lower(col("COUNTRY_NAME"))).alias("country_name"),
            trim(upper(col("ISOCODE"))).alias("isocode")
        )
    )
 
    df_input = session.table(ds_lottery).sample(n=n_points)
 
    logger.info(f"[{run_id}] OK - Loaded input table: {ds_lottery}")
    logger.info(f"[{run_id}] OK - {df_input.count()} input rows, {df_country_iso.count()} countries")
 
    return df_input, df_country_iso