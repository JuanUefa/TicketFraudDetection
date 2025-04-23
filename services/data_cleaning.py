# services/data_cleaning.py
 
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import (
    col, lower, upper, trim, to_timestamp, regexp_replace, when,
    lit, split, call_udf
)
from snowflake.snowpark.types import StringType
from utils.logging_config import get_logger
 
logger = get_logger("data_cleaning")
 
def data_cleaning(session: Session, df_input: DataFrame, df_country_iso: DataFrame, ds_lottery: str, run_id: str) -> DataFrame:
    logger.info(f"[{run_id}] START - data_cleaning")
 
    df = subset_dataframe(df_input, ds_lottery)
    logger.info(f"[{run_id}] OK - subset_dataframe")
 
    df = rename_vars(df, ds_lottery, run_id)
    df = missing_values_imputation(df)
    df = variables_cleaning(session, df, df_country_iso, ds_lottery)
 
    logger.info(f"[{run_id}] OK - data_cleaning: {df.count()} rows cleaned")
    return df
 
def subset_dataframe(df: DataFrame, ds_lottery: str) -> DataFrame:
    if ds_lottery == 'DS_LOTTERY_AI_DATA_CLEANSING_UECLF24':
        return df.select([
            col('"CLEANSED OUT (COMBINATION ALL CHECKS)"'),
            col('"ID_NUMBER"'), col('"Contact Number"'), col('"First name"'),
            col('"Last name"'), col('"Full name"'), col('"Birthday"'),
            col('"Email"'), col('"Mobile"'), col('"City"'), col('"Country"'),
            col('"Postcode"'), col('"I am a fan of - UECLF 2024 (extended export)"'),
            col("""'Increase your chances - UELF 2024'"""),
            col("""'I want to take part in the draw - UELF 2024'"""),
            col('"Tenant ID"'), col('"Application for UECLF?"'),
            col('"Application for UELF?"'), col('"Application for UCLF?"'),
            col('"Risk Score"'), col('"Provider Country"'), col('"Provider City"'),
            col('"Provider"'), col('"Browser language"'), col('"Browser"'),
            col('"Order Date"'), col('"Order Number"'), col('"IP short"'),
            col('"Combined Address"')
        ])
    elif ds_lottery == 'DS_LOTTERY_AI_DATA_CLEANSING_CLUBFINALS25':
        return df.select([
            col('"Application Id"'), col('passport_id'), col('increase_chances'),
            col('"Card Holder Name"'), col('"Application submission time"'),
            col('"Application Submission IP"'), col('"Email"'),
            col('"Mobile phone number"'), col('"Zip code"'), col('"City"'),
            col('"Country"'), col('"Firstname"'), col('"Lastname"'),
            col('"Birthdate"'), col('"BROWSER"'), col('"BROWSER_LANGUAGE"'),
            col('"PROVIDER"'), col('"PROVIDER_CITY"'), col('"PROVIDER_COUNTRY"'),
            col('cleansed')
        ])
    else:
        raise ValueError(f"Unknown dataset identifier: {ds_lottery}")
 
def rename_vars(df: DataFrame, ds_lottery: str, run_id: str) -> DataFrame:
    logger.info(f"[{run_id}] Renaming variables for dataset: {ds_lottery}")
 
    renames = {}
    if ds_lottery == 'DS_LOTTERY_AI_DATA_CLEANSING_UECLF24':
        renames = {
            '"CLEANSED OUT (COMBINATION ALL CHECKS)"': "cleansed",
            '"ID_NUMBER"': "passport_id", '"Contact Number"': "contact_number",
            '"First name"': "first_name", '"Last name"': "last_name",
            '"Full name"': "full_name", '"Birthday"': "birthday", '"Email"': "email",
            '"Mobile"': "mobile", '"City"': "city", '"Country"': "country",
            '"Postcode"': "postcode", '"I am a fan of - UECLF 2024 (extended export)"': "ueclf_24_fan_of",
            """'Increase your chances - UELF 2024'""": "uelf_24_app_increase_chances",
            """'I want to take part in the draw - UELF 2024'""": "uelf_24_app",
            '"Tenant ID"': "competition_id", '"Application for UECLF?"': "ueclf_24_app",
            '"Application for UELF?"': "uelf_25_app", '"Application for UCLF?"': "uclf_24_app",
            '"Risk Score"': "risk_ident_score", '"Provider Country"': "provider_country",
            '"Provider City"': "provider_city", '"Provider"': "provider",
            '"Browser language"': "browser_language", '"Browser"': "browser",
            '"Order Date"': "app_date", '"Order Number"': "app_id",
            '"IP short"': "ip_short", '"Combined Address"': "combined_address"
        }
    elif ds_lottery == 'DS_LOTTERY_AI_DATA_CLEANSING_CLUBFINALS25':
        renames = {
            '"Application Id"': "app_id", '"Card Holder Name"': "full_name",
            '"Application submission time"': "app_date", '"Application Submission IP"': "ip_short",
            '"Email"': "email", '"Mobile phone number"': "contact_number",
            '"Zip code"': "postcode", '"City"': "city", '"Country"': "country",
            '"Firstname"': "first_name", '"Lastname"': "last_name",
            '"Birthdate"': "birthday", '"BROWSER"': "browser",
            '"BROWSER_LANGUAGE"': "browser_language", '"PROVIDER"': "provider",
            '"PROVIDER_CITY"': "provider_city", '"PROVIDER_COUNTRY"': "provider_country"
        }
 
    for original, new in renames.items():
        df = df.with_column_renamed(original, new)
 
    return df
 
def missing_values_imputation(df: DataFrame) -> DataFrame:
    df = df.with_column(
        "app_date",
        when((col("app_date").is_null()) | (col("app_date") == lit("0")), lit("01/01/1970 00:00:00"))
        .otherwise(col("app_date"))
    )
 
    df = df.with_column(
        "app_date",
        regexp_replace(
            col("app_date").cast(StringType()),
            r"(\d{2})/(\d{2})/(\d{4})",
            r"\3-\2-\1"
        )
    )
 
    df = df.with_column("app_date", to_timestamp(col("app_date")))
    df = df.fillna("0")
    return df
 
def variables_cleaning(session: Session, df: DataFrame, df_country_iso: DataFrame, ds_lottery: str) -> DataFrame:
    if ds_lottery == 'DS_LOTTERY_AI_DATA_CLEANSING_UECLF24':
        df = df.with_column(
            "uelf_24_app_increase_chances",
            when(col("uelf_24_app_increase_chances") == "I accept tickets in another category", "1").otherwise("0")
        )
 
        df = df.with_column(
            "cleansed",
            when(col("cleansed") == "CLEANSED", "1")
            .when(col("cleansed") == "OK", "0")
            .otherwise("0")
        )
 
        df = df.with_column(
            "uelf_24_app",
            when(col("uelf_24_app") == "I want to apply for tickets in any case", "1")
            .when(col("uelf_24_app") == "I want to apply for tickets only if my team qualifies", "0")
            .otherwise("0")
        )
 
        for flag in ["ueclf_24_app", "uclf_24_app", "uelf_25_app"]:
            df = df.with_column(
                flag,
                when(col(flag) == "yes", "1").when(col(flag) == "no", "0").otherwise("0")
            )
 
    df = df.with_column("full_name", split(lower(col("full_name")), lit(","))) \
           .with_column("first_name", call_udf("clean_names", col("first_name"))) \
           .with_column("last_name", call_udf("clean_names", col("last_name"))) \
           .with_column("email_components", call_udf("extract_email_components", col("email"))) \
           .with_column("email_username", col("email_components")[0]) \
           .with_column("email_domain", col("email_components")[1]) \
           .with_column("email_tld", col("email_components")[2]) \
           .with_column("browser_language", call_udf("clean_browser_language", col("browser_language"))) \
           .with_column("country", upper(col("country"))) \
           .join(df_country_iso, col("country") == df_country_iso["country_name"], "left") \
           .drop("country") \
           .with_column_renamed("isocode", "country") \
           .with_column("city", upper(col("city"))) \
           .with_column("city", call_udf("clean_city", col("city")))
 
    return df