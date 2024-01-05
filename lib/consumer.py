
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

def receive_json(batchDF, batchId, spark):
    from pyspark.sql.functions import col, from_json
    import json
    
    try:
        df = batchDF \
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \

        json_data = df.select(col("value")).first()[0]
        schema = infer_schema(json.loads(json_data))
        print(schema)
        
        df.select(from_json(col("value").cast("string"), schema).alias("value")) \
            .write \
            .format("console") \
            .save()                     

        df.select(from_json(col("value").cast("string"), schema).alias("value")) \
            .selectExpr("value.message", "value.date") \
            .write \
            .format("console") \
            .save()  
        
    except Exception as E:
        print(f">>>>>>>> {E}")
        batchDF \
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .write \
            .format("console") \
            .save()         
