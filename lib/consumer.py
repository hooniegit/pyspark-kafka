
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
    from pyspark.sql.functions import from_json
    import json
    
    try:
        # Create Schema
        json_data = batchDF \
            .selectExpr(
                "CAST(value AS STRING)"
            ) \
            .select("value").first()[0]
        schema = infer_schema(json.loads(json_data))
        print(schema)
        
        # Change Datatype
        df = batchDF \
            .selectExpr(
                "CAST(key AS STRING)",
                "CAST(value AS STRING)"
            ) \
            .withColumn("value", from_json("value", schema))
        
        # Test
        df.write \
            .format("console") \
            .save()                     

        # Test
        df \
            .selectExpr(
                "key",
                "value.message", 
                "value.date"
            ) \
            .write \
            .format("console") \
            .save()  
        
    except Exception as E:
        print(f">>>>>>>> {E}")
        batchDF \
            .selectExpr(
                "CAST(key AS STRING)", 
                "CAST(value AS STRING)"
            ) \
            .write \
            .format("console") \
            .save()         
