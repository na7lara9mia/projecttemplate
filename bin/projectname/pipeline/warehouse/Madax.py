# Imports
import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime


def writeToHDFS(df, countryColumn, brandColumn, path):
    print("Writing in HDFS ...")
    df \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .partitionBy(countryColumn, brandColumn, "year", "month") \
        .parquet(path)


def madax_construct(spark_session, dateFrom, dateTo, writeHDFS="False", country=None, brand=None):
    # Read the configurations
    with open(r'../conf/madax.yaml') as file:
        conf = yaml.load(file, Loader=yaml.FullLoader)

    # Loading the conf from HDFS
    df_rbvqtcdc = spark_session.read.load(conf["hdfsFileReadPath"] + conf["cdc"])
    df_rbvqttxm_arc = spark_session.read.load(conf["hdfsFileReadPath"] + conf["txm_arc"])
    df_rbvqtfll = spark_session.read.load(conf["hdfsFileReadPath"] + conf["fll"])
    df_rbvqttff = spark_session.read.load(conf["hdfsFileReadPath"] + conf["tff"])
    df_rbvqtlm1 = spark_session.read.load(conf["hdfsFileReadPath"] + conf["lm1"])
    df_rbvqtveh = spark_session.read.load(conf["hdfsFileReadPath"] + conf["veh"])
    df_rbvqttut = spark_session.read.load(conf["hdfsFileReadPath"] + conf["tut"])
    df_rbvqtcco = spark_session.read.load(conf["hdfsFileReadPath"] + conf["cco"])
    df_rbvqtcaf = spark_session.read.load(conf["hdfsFileReadPath"] + conf["caf"])
    df_rbvqtm10 = spark_session.read.load(conf["hdfsFileReadPath"] + conf["m10"])
    df_rbvqtctp = spark_session.read.load(conf["hdfsFileReadPath"] + conf["ctp"])
    df_rbvqtfam = spark_session.read.load(conf["hdfsFileReadPath"] + conf["fam"])

    # Creating the temporary views
    df_rbvqtcdc.createOrReplaceTempView("df_rbvqtcdc")
    df_rbvqttxm_arc.createOrReplaceTempView("df_rbvqttxm_arc")
    df_rbvqtfll.createOrReplaceTempView("df_rbvqtfll")
    df_rbvqttff.createOrReplaceTempView("df_rbvqttff")
    df_rbvqtlm1.createOrReplaceTempView("df_rbvqtlm1")
    df_rbvqtveh.createOrReplaceTempView("df_rbvqtveh")
    df_rbvqttut.createOrReplaceTempView("df_rbvqttut")
    df_rbvqtcco.createOrReplaceTempView("df_rbvqtcco")
    df_rbvqtcaf.createOrReplaceTempView("df_rbvqtcaf")
    df_rbvqtm10.createOrReplaceTempView("df_rbvqtm10")
    df_rbvqtctp.createOrReplaceTempView("df_rbvqtctp")
    df_rbvqtfam.createOrReplaceTempView("df_rbvqtfam")

    if (country is not None) & (brand is not None):
        query = conf["madaxSQLPathP"]
    else:
        query = conf["madaxSQLPath"]

    # Reading the SQL query from file
    queryMADAX = open(query, mode='r', encoding='utf-8-sig').read().format(dateFrom, dateTo, country, brand)

    # Reading the data using the query above
    dfMADAX = spark_session.sql(queryMADAX) \
        .withColumn("year", F.year(F.col("DATVENT"))) \
        .withColumn("month", F.month(F.col("DATVENT"))) \
        .withColumn("day", F.dayofmonth(F.col("DATVENT"))) \
        .withColumn('COUNTRY', F.col('LIBELLE_FRANCAIS'))

    # Treatments
    # Promo Codes Mapping
    for i in range(1, 6):
        codop = "CODOP{0}".format(str(i))
        dfMADAX = dfMADAX.withColumn(codop + '_LIBELLE',
                                     F.when((F.substring(codop, 1, 1) == 'C') | (F.substring(codop, 1, 1) == 'D'),
                                            F.col(codop).substr(F.lit(4), F.lit(F.length(F.col(codop)) - 5)))
                                     .otherwise(F.col(codop).substr(F.lit(3), F.lit(F.length(F.col(codop)) - 4))))

    # Mapping VP, VU variables from French abb. to English abb.
    column_name_VpVu = {'VP': 'PC', 'VU': 'CV'}

    dfMADAXFinal = dfMADAX \
        .replace(to_replace=column_name_VpVu, subset=['VPVU']) \
        .withColumn('REMIPOUR', F.when(F.col('REMIPOUR') == '0', F.lit(None)).otherwise(F.col('REMIPOUR'))) \
        .withColumn('CODPROM', F.when(F.col('CODPROM') == '0', F.lit(None)).otherwise(F.col('CODPROM')))

    print(dfMADAXFinal.select("COUNTRY", "MARQUE").distinct().show())

    # Writing the result in HDFS under partitions
    if writeHDFS == "True":
        writeToHDFS(dfMADAXFinal, "COUNTRY", "MARQUE", conf["hdfsFileWritePath"])

    if country is not None:
        # Stopping the Spark Session
        spark_session.stop()
        print(datetime.now().strftime("%y/%m/%d %H:%M:%S") + " INFO Session successfully closed.")
