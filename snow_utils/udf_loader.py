# snow_utils/udf_loader.py
 
import importlib.util
import pathlib
from snowflake.snowpark import Session
 
def register_all_udfs(
    session: Session,
    udfs_path: str = "user_defined_functions",
    permanent: bool = False,
    stage: str = None
) -> list[str]:
    registered = []
    udfs_dir = pathlib.Path(udfs_path)
 
    print(f"Scanning for UDFs in: {udfs_dir.resolve()}")
 
    for file in udfs_dir.glob("*.py"):
        module_name = file.stem
        spec = importlib.util.spec_from_file_location(module_name, file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
 
        if hasattr(module, "udf_definitions"):
            for udf_def in module.udf_definitions:
                name = udf_def["name"]
                input_types = udf_def["input_types"]
                input_signature = ",".join([t.simpleString().upper() for t in input_types])
 
                if permanent:
                    try:
                        drop_stmt = f"DROP FUNCTION IF EXISTS {name}({input_signature})"
                        session.sql(drop_stmt).collect()
                        print(f"Dropped existing function: {name}({input_signature})")
                    except Exception as e:
                        print(f"Could not drop function {name}: {e}")
 
                session.udf.register(
                    func=udf_def["func"],
                    name=name,
                    return_type=udf_def["return_type"],
                    input_types=input_types,
                    is_permanent=permanent,
                    stage_location=stage if permanent else None,
                    replace=not permanent  # True only if temporary
                )
                print(f"Registered: {name}")
                registered.append(name)
        else:
            print(f"Skipped {file.name}: no 'udf_definitions' found")
 
    return registered