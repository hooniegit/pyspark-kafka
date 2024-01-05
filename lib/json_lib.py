
def infer_schema(json_data:dict):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
    
    def infer_field(name, value):
        if isinstance(value, bool):
            return StructField(name, BooleanType(), True)
        elif isinstance(value, int):
            return StructField(name, IntegerType(), True)
        elif isinstance(value, list):
            element_type = infer_field(name, value[0]).dataType
            return StructField(name, ArrayType(element_type), True)
        elif isinstance(value, dict):
            nested_fields = [infer_field(sub_name, sub_value) for sub_name, sub_value in value.items()]
            return StructField(name, StructType(nested_fields), True)
        else:
            return StructField(name, StringType(), True)
    
    fields = [infer_field(name, value) for name, value in json_data.items()]
    return StructType(fields)
