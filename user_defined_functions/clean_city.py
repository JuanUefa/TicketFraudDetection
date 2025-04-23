from snowflake.snowpark.types import StringType
import re
 
def clean_city(city: str) -> str:
    if city is None or city.strip() == "":
        return "unknown"
    city = city.lower().strip()
    city = re.sub(r'[^a-z\s]', '', city)
    return re.sub(r'\s+', ' ', city)
 
udf_definitions = [
    {
        "func": clean_city,
        "name": "clean_city",
        "return_type": StringType(),
        "input_types": [StringType()]
    }
]