# dev/main.py
 
import sys
from pathlib import Path
from uuid import uuid4
 
sys.path.append(str(Path(__file__).resolve().parents[1]))
 
from snow_utils.session import get_snowpark_session
from services.data_loading import data_loading
from utils.logging_config import get_logger

from snow_utils.udf_loader import register_all_udfs
 
logger = get_logger("dev_main")
 
def main():
    session = get_snowpark_session()
    run_id = str(uuid4())  
 
    logger.info(f"Starting full pipeline test with run_id: {run_id}")
 
    # Load the data
    df_input, df_country_iso = data_loading(
        session=session,
        ds_lottery="DS_LOTTERY_AI_DATA_CLEANSING_CLUBFINALS25",  # Replace with actual table
        n_points=100,
        run_id=run_id
    )
 
    logger.info("Sample input:")
    df_input.show(10)
 
    logger.info("Country ISO map:")
    df_country_iso.show(10)

    # Registro de UDFs existentes votando errores
    # Register UDFs and print summary
    #registered_udfs = register_all_udfs(session)
    #print("Registered UDFs:", ", ".join(registered_udfs))
 
    logger.info("Pipeline test complete!")
 
if __name__ == "__main__":
    main()