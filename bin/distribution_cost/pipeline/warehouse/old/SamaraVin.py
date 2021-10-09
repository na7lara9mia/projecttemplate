# Imports
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.types import *


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
