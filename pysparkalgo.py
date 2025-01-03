import pyspark
from pyspark.sql import SparkSession
import data_processing as dp

import env_vars as env
from validate_sparkObj import validate_obj

import excelOutput as ex

def main():
    print("Inside Main: \n")
    spark = SparkSession.builder.appName(env.env+'App').getOrCreate()
    # validate_obj(spark)
    citydf = spark.read.parquet('C:\\Users\\KIIT\\Desktop\\Nov@2024\\source\\us_cities_dimension.parquet',header=env.header)
    presdf = spark.read.csv('C:\\Users\\KIIT\Desktop\\Nov@2024\\source\\USA_Presc_Medicare_Data_12021.csv',header=env.header,\
                         inferSchema=env.inferschema)
    df_city,df_presc = dp.data_clean(citydf,presdf) # After basic cleaning and pre-processing
    # df_city.printSchema()
    # df_presc.printSchema()
    # df_city.show(5)
    # df_presc.show(5)
    df_city_transf,df_presc_transf,df_joined = dp.transf(df_city,df_presc,spark)
    # dp.windowPartition(df_city,df_presc,spark)
    df_rep = dp.reporting(df_joined)
    ex.excelWriting(df_rep)


if __name__ == '__main__':
    main()


# df.show(truncate=0)