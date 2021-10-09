# Imports
import yaml
from functools import reduce
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


def samara_construct(spark_session, create, dateFrom, dateTo, writeHDFS="False", country=None, brand=None):
    # Read the configurations
    with open(r'../conf/samara.yaml') as file:
        conf = yaml.load(file, Loader=yaml.FullLoader)

    # Loading the conf from HDFS
    df_sinqtvin = spark_session.read.load(conf["hdfsFileReadPath"] + conf["vin"])
    df_sinqtcli = spark_session.read.load(conf["hdfsFileReadPath"] + conf["cli"])
    df_sinqtver = spark_session.read.load(conf["hdfsFileReadPath"] + conf["ver"])
    df_sinqtfv4 = spark_session.read.load(conf["hdfsFileReadPath"] + conf["fv4"])
    df_sinqtcmp = spark_session.read.load(conf["hdfsFileReadPath"] + conf["cmp"])
    df_sinqtcnd = spark_session.read.load(conf["hdfsFileReadPath"] + conf["cnd"])
    df_sinqtseg = spark_session.read.load(conf["hdfsFileReadPath"] + conf["seg"])
    df_sinqtzds = spark_session.read.load(conf["hdfsFileReadPath"] + conf["zds"])
    df_sinqtsfa = spark_session.read.load(conf["hdfsFileReadPath"] + conf["sfa"])
    df_sinqtfam = spark_session.read.load(conf["hdfsFileReadPath"] + conf["fam"])
    df_sinqtrub = spark_session.read.load(conf["hdfsFileReadPath"] + conf["rub"])
    df_sinqtopc = spark_session.read.load(conf["hdfsFileReadPath"] + conf["opc"])
    df_sinqtcma = spark_session.read.load(conf["hdfsFileReadPath"] + conf["cma"])
    df_sinqtcmi = spark_session.read.load(conf["hdfsFileReadPath"] + conf["cmi"])
    df_sinqtcyr = spark_session.read.load(conf["hdfsFileReadPath"] + conf["cyr"])
    df_sinqtmrq = spark_session.read.load(conf["hdfsFileReadPath"] + conf["mrq"])
    df_sinqtbas = spark_session.read.load(conf["hdfsFileReadPath"] + conf["bas"])

    # Creating the temporary views
    df_sinqtvin.createOrReplaceTempView("df_sinqtvin")
    df_sinqtver.createOrReplaceTempView("df_sinqtver")
    df_sinqtfv4.createOrReplaceTempView("df_sinqtfv4")
    df_sinqtcmp.createOrReplaceTempView("df_sinqtcmp")
    df_sinqtcnd.createOrReplaceTempView("df_sinqtcnd")
    df_sinqtcli.createOrReplaceTempView("df_sinqtcli")
    df_sinqtseg.createOrReplaceTempView("df_sinqtseg")
    df_sinqtzds.createOrReplaceTempView("df_sinqtzds")
    df_sinqtsfa.createOrReplaceTempView("df_sinqtsfa")
    df_sinqtfam.createOrReplaceTempView("df_sinqtfam")
    df_sinqtrub.createOrReplaceTempView("df_sinqtrub")
    df_sinqtopc.createOrReplaceTempView("df_sinqtopc")
    df_sinqtcma.createOrReplaceTempView("df_sinqtcma")
    df_sinqtcmi.createOrReplaceTempView("df_sinqtcmi")
    df_sinqtcyr.createOrReplaceTempView("df_sinqtcyr")
    df_sinqtmrq.createOrReplaceTempView("df_sinqtmrq")
    df_sinqtbas.createOrReplaceTempView("df_sinqtbas")

    if country is not None:
        query = conf["samaraSQLPathP"]
        if brand is None:
            brand = ""
    else:
        query = conf["samaraSQLPath"]

    # Reading the SQL query from file
    querySAMARA = open(query, mode='r', encoding='utf-8-sig').read().format(dateFrom, dateTo, country, brand)

    # Reading the data using the query above
    dfSAMARA = spark_session.sql(querySAMARA) \
        .withColumn("year", F.year(F.col("DT_VD"))) \
        .withColumn("month", F.month(F.col("DT_VD"))) \
        .withColumn("day", F.dayofmonth(F.col("DT_VD"))) \
        .withColumn('SINQTZDS__CODE', F.substring(F.col("SINQTZDS__CODE"), 3, 2)) \
        .withColumnRenamed('SINQTZDS__CODE', "COUNTRY") \
        .replace(to_replace=conf["column_values_ctry"], subset=['COUNTRY'])

    if create == "vin":
        dfResult = samara_vin(dfSAMARA)
        print(dfResult.select("COUNTRY", "LIB_MRQ").distinct().show())
        if writeHDFS == "True":
            writeToHDFS(dfResult, "COUNTRY", "LIB_MRQ", conf["hdfsFileWritePathVin"])
    if create == "vinpromo":
        dfResult = samara_vinpromo(dfSAMARA)
        print(dfResult.select("COUNTRY", "CODE_MRQ").distinct().show())
        if writeHDFS == "True":
            writeToHDFS(dfResult, "COUNTRY", "CODE_MRQ", conf["hdfsFileWritePathVinPromo"])

    if country is not None:
        # Stopping the Spark Session
        spark_session.stop()
        print(datetime.now().strftime("%y/%m/%d %H:%M:%S") + " INFO Session successfully closed.")

def samara_vinpromo(dfSAMARA):
    # dropping the duplicate lines based on selected columns
    dfVINXPROMO = dfSAMARA \
        .select("SINQTVIN__CODE", "COUNTRY", "SINQTMRQ_2__CODE", "SINQTOPC_LIB", "SINQTRUB__CODE", "SINQTRUB__LIB",
                "SINQTOPC__CODE", "MCX_VARIABLES", "year", "month", "day") \
        .drop_duplicates() \
        .withColumnRenamed("SINQTVIN__CODE", "CODE_VIN") \
        .withColumnRenamed("SINQTOPC_LIB", "LIB_OPC") \
        .withColumnRenamed("SINQTRUB__CODE", "CODE_RUB") \
        .withColumnRenamed("SINQTRUB__LIB", "LIB_RUB") \
        .withColumnRenamed("SINQTOPC__CODE", "CODE_OPC") \
        .withColumnRenamed("SINQTMRQ_2__CODE", "CODE_MRQ")

    # renaming columns
    column_values_rub = {'PROVISION SUR PERTES FUTURES': 'PSA - Buy Backs',
                         'PRIMES VENTES AUX SOCIETES': 'PSA - B2B promotions',
                         'PRIME QUALITE': 'PSA - Bonus for quality',
                         'PRIMES A LA PERFORMANCE RESEAU': 'PSA - Bonus for dealer performance',
                         'PROMOTIONS CLIENT FINAL': 'PSA - B2C promotions'}
    dfVINXPROMO = dfVINXPROMO.replace(to_replace=column_values_rub, subset=['LIB_RUB'])

    dfVINXPROMO = dfVINXPROMO.filter((F.col('MCX_VARIABLES') != 0))

    # value modifications based on a condition
    dfVINXPROMO = dfVINXPROMO \
        .withColumn('LIB_OPC', F.concat(F.col('LIB_OPC'), F.lit(' - '), F.when(
        F.col('LIB_RUB').isin('PSA - Bonus for dealer performance', 'PSA - Bonus for quality'),
        'Network Remuneration').otherwise('Client Promotions')))

    return dfVINXPROMO


def samara_vin(dfSAMARA):

    # Samara (VIN) construction phase
    dfSAMARA_VIN = dfSAMARA.drop('SINQTRUB__CODE', 'SINQTRUB__LIB', 'SINQTOPC__CODE', 'SINQTOPC_LIB', 'MCX_VARIABLES')

    # Creating the temporary views
    dfSAMARA_VIN.createOrReplaceTempView("dfSAMARA_VIN")

    d = dfSAMARA_VIN.select('SINQTVIN__CODE',
                            'VOLUME_AJ',
                            'PRIX_VENTE',
                            'MACOM_CONSO',
                            'MACOM_CONSO_VERSION',
                            'MACOM_ENTITE',
                            'MACOM_ENTITE_AJ',
                            'MACOM_ENTITE_OPTION',
                            'PRIX_VENTE_AJ',
                            'PV_VERSION',
                            'MACOM_CONSO_AJ',
                            'MACOM_CONSO_OPTION',
                            'MACOM_ENTITE_VERSION',
                            'RBCV_AJ',
                            'PV_OPTIONS').groupby('SINQTVIN__CODE').sum()

    dfSAMARA_VIN = dfSAMARA_VIN.drop('VOLUME_AJ',
                                     'PRIX_VENTE',
                                     'MACOM_CONSO',
                                     'MACOM_CONSO_VERSION',
                                     'MACOM_ENTITE',
                                     'MACOM_ENTITE_AJ',
                                     'MACOM_ENTITE_OPTION',
                                     'PRIX_VENTE_AJ',
                                     'PV_VERSION',
                                     'MACOM_CONSO_AJ',
                                     'MACOM_CONSO_OPTION',
                                     'MACOM_ENTITE_VERSION',
                                     'RBCV_AJ',
                                     'PV_OPTIONS')

    dfSAMARA_VIN = dfSAMARA_VIN.drop_duplicates()  # to check before that and after that

    dfs = [dfSAMARA_VIN, d]
    dfSAMARA_VIN = reduce(lambda left, right: left.join(right, on='SINQTVIN__CODE'), dfs)

    # Renaming columns and replacing 0 vcalues with None
    dfSAMARA_VIN = dfSAMARA_VIN \
        .withColumnRenamed("SINQTVIN__CODE", "CODE_VIN") \
        .withColumnRenamed("SINQTCLI_2__CODE", "CODE_CLI_2") \
        .withColumnRenamed("SINQTVER__CODE", "CODE_VER") \
        .withColumnRenamed("SINQTVER__LIB", "LIB_VER") \
        .withColumnRenamed("SINQTCMP__CODE", "CODE_CMP") \
        .withColumnRenamed('SINQTCMP__LIB', 'LIB_CMP') \
        .withColumnRenamed('SINQTCND__CODE', 'CODE_CND') \
        .withColumnRenamed('SINQTCND__LIB', 'LIB_CND') \
        .withColumnRenamed('SINQTCLI__CODE', 'CODE_CLI') \
        .withColumnRenamed('SINQTCLI__LIB', 'LIB_CLI') \
        .withColumnRenamed('SINQTSEG__CODE', 'CODE_SEG') \
        .withColumnRenamed('SINQTSEG__LIB', 'LIB_SEG') \
        .withColumnRenamed('SINQTZDS__LIB', 'LIB_ZDS') \
        .withColumnRenamed('SINQTSFA__CODE', 'CODE_SFA') \
        .withColumnRenamed('SINQTSFA__LIB', 'LIB_SFA') \
        .withColumnRenamed('SINQTFAM__CODE', 'CODE_FAM') \
        .withColumnRenamed('SINQTFAM__LIB', 'LIB_FAM') \
        .withColumnRenamed('SINQTCMA__CODE', 'CODE_CMA') \
        .withColumnRenamed('SINQTCMA__LIB', 'LIB_CMA') \
        .withColumnRenamed('SINQTCMI__CODE', 'CODE_CMI') \
        .withColumnRenamed('SINQTCMI__LIB', 'LIB_CMI') \
        .withColumnRenamed('SINQTMRQ_2__CODE', 'LIB_MRQ') \
        .withColumnRenamed("sum(PRIX_VENTE_AJ)", 'PRIX_VENTE_AJ') \
        .withColumnRenamed("sum(PV_OPTIONS)", 'PV_OPTIONS') \
        .withColumnRenamed("sum(PV_VERSION)", 'PV_VERSION') \
        .withColumnRenamed("sum(RBCV_AJ)", 'RBCV_AJ') \
        .withColumnRenamed("sum(MACOM_CONSO_AJ)", 'MACOM_CONSO_AJ') \
        .withColumnRenamed("sum(MACOM_CONSO_OPTION)", 'MACOM_CONSO_OPTION') \
        .withColumnRenamed("sum(MACOM_ENTITE_VERSION)", 'MACOM_ENTITE_VERSION') \
        .withColumnRenamed("sum(VOLUME_AJ)", 'VOLUME_AJ') \
        .withColumnRenamed("sum(PRIX_VENTE)", 'PRIX_VENTE') \
        .withColumnRenamed("sum(MACOM_CONSO)", 'MACOM_CONSO') \
        .withColumnRenamed("sum(MACOM_CONSO_VERSION)", 'MACOM_CONSO_VERSION') \
        .withColumnRenamed("sum(MACOM_ENTITE)", 'MACOM_ENTITE') \
        .withColumnRenamed("sum(MACOM_ENTITE_AJ)", 'MACOM_ENTITE_AJ') \
        .withColumnRenamed("sum(MACOM_ENTITE_OPTION)", 'MACOM_ENTITE_OPTION') \
        .withColumn('PRIX_VENTE_AJ',
                    F.when(F.col('PRIX_VENTE_AJ') == '0', F.lit(None)).otherwise(F.col('PRIX_VENTE_AJ'))) \
        .withColumn('PV_OPTIONS', F.when(F.col('PV_OPTIONS') == '0', F.lit(None)).otherwise(F.col('PV_OPTIONS'))) \
        .withColumn('PV_VERSION', F.when(F.col('PV_VERSION') == '0', F.lit(None)).otherwise(F.col('PV_VERSION'))) \
        .withColumn('MACOM_CONSO_AJ',
                    F.when(F.col('MACOM_CONSO_AJ') == '0', F.lit(None)).otherwise(F.col('MACOM_CONSO_AJ'))) \
        .withColumn('MACOM_CONSO_OPTION',
                    F.when(F.col('MACOM_CONSO_OPTION') == '0', F.lit(None)).otherwise(F.col('MACOM_CONSO_OPTION'))) \
        .withColumn('MACOM_ENTITE_VERSION',
                    F.when(F.col('MACOM_ENTITE_VERSION') == '0', F.lit(None)).otherwise(F.col('MACOM_ENTITE_VERSION'))) \
        .withColumn('VOLUME_AJ', F.when(F.col('VOLUME_AJ') == '0', F.lit(None)).otherwise(F.col('VOLUME_AJ'))) \
        .withColumn('PRIX_VENTE', F.when(F.col('PRIX_VENTE') == '0', F.lit(None)).otherwise(F.col('PRIX_VENTE'))) \
        .withColumn('MACOM_CONSO', F.when(F.col('MACOM_CONSO') == '0', F.lit(None)).otherwise(F.col('MACOM_CONSO'))) \
        .withColumn('MACOM_CONSO_VERSION',
                    F.when(F.col('MACOM_CONSO_VERSION') == '0', F.lit(None)).otherwise(F.col('MACOM_CONSO_VERSION'))) \
        .withColumn('MACOM_ENTITE', F.when(F.col('MACOM_ENTITE') == '0', F.lit(None)).otherwise(F.col('MACOM_ENTITE'))) \
        .withColumn('MACOM_ENTITE_AJ',
                    F.when(F.col('MACOM_ENTITE_AJ') == '0', F.lit(None)).otherwise(F.col('MACOM_ENTITE_AJ'))) \
        .withColumn('MACOM_ENTITE_OPTION',
                    F.when(F.col('MACOM_ENTITE_OPTION') == '0', F.lit(None)).otherwise(F.col('MACOM_ENTITE_OPTION'))) \
        .select('CODE_VIN',
                'CODE_CLI_2',
                'CODE_VER',
                'LIB_VER',
                'DT_FACT',
                'DT_VD',
                'DT_COMM_CLI_FIN_VD',
                'DATIMM',
                'CODE_CMP',
                'LIB_CMP',
                'CODE_CND',
                'LIB_CND',
                'CODE_CLI',
                'LIB_CLI',
                'CODE_SEG',
                'LIB_SEG',
                'LIB_ZDS',
                'CODE_SFA',
                'LIB_SFA',
                'CODE_FAM',
                'LIB_FAM',
                'CODE_CMA',
                'LIB_CMA',
                'CODE_CMI',
                'LIB_CMI',
                'TYPE_FLOTTE_VD',
                'TYPE_OPE_ESSOR',
                'TYP_UTIL_VD',
                'CODE_PROFESSION_VD',
                'CODE_PROMO',
                'CODE_PROMO2',
                'VOLUME_AJ',
                'PRIX_VENTE',
                'MACOM_CONSO',
                'MACOM_CONSO_VERSION',
                'MACOM_ENTITE',
                'MACOM_ENTITE_AJ',
                'MACOM_ENTITE_OPTION',
                'LIB_MRQ',
                'PRIX_VENTE_AJ',
                'PV_VERSION',
                'MACOM_CONSO_AJ',
                'MACOM_CONSO_OPTION',
                'MACOM_ENTITE_VERSION',
                'RBCV_AJ',
                'PV_OPTIONS',
                'COUNTRY',
                'year',
                'month',
                'day')

    return dfSAMARA_VIN