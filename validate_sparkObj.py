
def validate_obj(spark):
    df = spark.sql('select current_date() as today')
    df.show()