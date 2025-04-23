# udfs/clean_names.py
 
from snowflake.snowpark.types import StringType
import unicodedata
import re
 
def clean_names(name: str) -> str:
    if name is None:
        return ""
    name = str(name).lower()
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("utf-8")
    return re.sub(r"[^a-z\s]", "", name)
 
# This is the standard export the loader will expect
udf_definitions = [
    {
        "func": clean_names,
        "name": "clean_names",
        "return_type": StringType(),
        "input_types": [StringType()]
    }
]