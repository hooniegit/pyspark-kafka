
def demo_function(batchDF, batchId, spark):
    batchDF \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .write \
        .format("console") \
        .save()
