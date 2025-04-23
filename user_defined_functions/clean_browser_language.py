from snowflake.snowpark.types import StringType
import re
 
def clean_browser_language(lang: str) -> str:
    if lang is None or lang.strip() == "":
        return "unknown"
    lang_list = lang.lower().split(',')
    cleaned_langs = [
        re.sub(r'^([a-z]{2})-([a-z]{2})$', lambda m: f"{m.group(1)}-{m.group(2).upper()}", l)
        for l in lang_list if re.fullmatch(r'^[a-z]{2}-[a-z]{2}$', l)
    ]
    return ','.join(cleaned_langs) if cleaned_langs else "unknown"
 
udf_definitions = [
    {
        "func": clean_browser_language,
        "name": "clean_browser_language",
        "return_type": StringType(),
        "input_types": [StringType()]
    }
]