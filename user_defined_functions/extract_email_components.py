from snowflake.snowpark.types import StringType, ArrayType
import re
 
def extract_email_components(email: str) -> list[str]:
    if email is None:
        return ["", "", ""]
    match = re.match(r'([^@]+)@([^.]+)\.(.+)', email)
    if match:
        return [match.group(1), match.group(2), match.group(3)]
    return ["", "", ""]
 
udf_definitions = [
    {
        "func": extract_email_components,
        "name": "extract_email_components",
        "return_type": ArrayType(StringType()),
        "input_types": [StringType()]
    }
]