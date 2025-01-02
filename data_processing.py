from pyspark.sql.functions import *
from pyspark.sql.window import Window

def data_clean(df1,df2):
    
    df_city = df1
    # df_city.select(['county_name','state_name']).show(5)
    print('Converting Some columns to Upper case: \n')
    df_city = df_city.withColumn('state_name',upper(col('state_name')))
    df_city = df_city.withColumn('city',upper(col('city')))
    # df_city.select(['county_name','state_name']).show(5)

    df_presc = df2
    # print("Null Count for both dfs: ")
    # null_pres_df = [sum(col(c).isNull().cast('int')).alias(c) for c in df_pres.columns]
    # null_pres_df = df_pres.select(null_pres_df)
    # null_pres_df.show()
    df_presc = df2.select([col('npi').alias('presc_id'),col('nppes_provider_last_org_name').alias('presc_lname'),\
                           col('nppes_provider_first_name').alias('presc_fname'),col('nppes_provider_city').alias('presc_city')\
                           ,col('nppes_provider_state').alias('presc_state'),col('specialty_description').alias('presc_spec_desc'),\
                            col('drug_name'),col('total_claim_count').alias('tx_count'),col('total_day_supply'),\
                                col('total_drug_cost'),col('years_of_exp')])

    df_presc = df_presc.withColumn('country_name',lit('USA'))
    df_presc = df_presc.withColumn('years_of_exp',regexp_replace(col('years_of_exp'),'=','').cast('int'))
    df_presc = df_presc.withColumn('presc_fullname',concat(col('presc_fname'),lit(' '),col('presc_lname')))
    print("Scema ->>")
    # df_presc.printSchema()
    print("Null Count for both dfs: ")


    df_presc = df_presc.dropna(subset = ['presc_id','drug_name'])
    # null_pres_df = [sum(col(c).isNull().cast('int')).alias(c) for c in df_presc.columns]
    # null_pres_df = df_presc.select(null_pres_df)
    # null_pres_df.show()

    # df_presc.show(5)
    return df_city,df_presc

def transf(df1,df2,spark):
    #Calculate no. of zips per city 
    df_intrm = df1.withColumn('zipslen',size(split(col('zips'),' '))) 
    df_intrm.show(5)

    #distinct prescb. and tx_cnt according to state and city 
    df2.createOrReplaceTempView('cte')
    df2_rep = spark.sql('Select presc_state,presc_city, count( distinct presc_id) as dist_presc_id, sum(tx_count) as \
                        tot_tx from cte group by presc_state,presc_city')
    df2_rep.show(5)

    df_joined = df_intrm.join(df2_rep,(df_intrm['state_id']==df2_rep['presc_state']) & (df_intrm['city']==df2_rep['presc_city']),'inner')
    df_joined.select(['city','state_id','state_name','zipslen','dist_presc_id','tot_tx']).show(5)
    df_joined.createOrReplaceTempView('cte')
    print(df_joined.count())
    # df_city = df1.groupby('city').
    # df_city.show()

def windowPartition(df1,df2,spark):

    #creating over by clause
    spec = Window.partitionBy('presc_state').orderBy(desc(col('tx_count')))

    #Before filter count
    print(df2.count())

    # Filttering presc by years_of_exp
    df2 = df2.filter((df2['years_of_exp'] > 20) & (df2['years_of_exp'] < 50))

    #After Filter count
    print(df2.count())

    df2_rep2 = df2.withColumn('rank',dense_rank().over(spec))
    df2_rep2 = df2_rep2.join(df1,(df1['state_id']==df2_rep2['presc_state']) & (df1['city']==df2_rep2['presc_city']), 'inner')
    df2_rep2 = df2_rep2.select(['presc_fullname','city','presc_state','years_of_exp','tx_count','rank'])
    df2_rep2.filter('rank<=5').show(20)



