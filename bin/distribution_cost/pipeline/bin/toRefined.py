from pyspark.sql import functions as F
from pyspark.sql.types import *
import functools
from itertools import chain
import pandas as pd
import calendar
from datetime import datetime
import os

from commonFunctions import load_config, copy_tables, create_hdfs_folder, mapping_countries_eng, log, timestamp, mapping_ctry, brands,countries, mapping_rub, write_to_hdfs, read_hdfs_file


def to_refined(session, data_of, fromDate, toDate, country=None, brand=None, isHistorical=None):
    log("info", "Beginning of --- REFINED ZONE --- Data Processes")
    log("info", "   Table: " + str(data_of))
    log("info", "   fromDate: " + str(fromDate))
    log("info", "   toDate: " + str(toDate))
    log("info", "   country: " + str(country))
    log("info", "   brand: " + str(brand))

    def samara_processing():
        log("info", "Samara processing starting.")
        # Read the configurations
        conf = load_config("samara")
        confCHAN = load_config("channel_type")
        confFAM = load_config("family")
        path = conf['hdfsFileStandardPath']

        if country is not None:
            path = path + 'COUNTRY=' + mapping_ctry[country] + '/'

        if brand is not None:
            path = path + 'BRAND=' + brand + '/'

        dfSAMARA = session.read.parquet(path)

        if country is not None:
            dfSAMARA = dfSAMARA.withColumn('COUNTRY', F.lit(mapping_ctry[country]))

        if brand is not None:
            dfSAMARA = dfSAMARA.withColumn('BRAND', F.lit(brand))

        # map channel column
        df_channel_type = session.read.parquet(conf['dfCHAN'])\
            .filter(F.col(confCHAN['ColumnToMapSamara']).isNotNull())
        column_values_channel_type = {row[confCHAN['ColumnToMapSamara']]: row[confCHAN['ColumnChannel']] for row in df_channel_type.collect()}
        dfSAMARA_VIN = dfSAMARA.replace(to_replace=column_values_channel_type, subset=['LIB_CMA'])

        # map family column
        df_family = session.read.parquet(conf['dfFAM'])
        column_value_fam = {row[confFAM['ColumnToMap']]: row[confFAM['ColumnFamily']] for row in df_family.collect()}
        dfSAMARA_VIN = dfSAMARA_VIN.replace(to_replace=column_value_fam, subset=['LIB_FAM'])

        # sum amounts per vin
        dfSAMARA_VIN=dfSAMARA_VIN.drop('SINQTOPC_LIB','SINQTOPC__CODE','SINQTRUB__LIB', 'SINQTRUB__CODE','MCX_VARIABLES')\
            .drop_duplicates()

        dfSAMARA_SUM = dfSAMARA_VIN.select(conf['ColumnToSelectSUM']).groupby('VIN', 'COUNTRY').sum()
        dfSAMARA_VIN = dfSAMARA_VIN.orderBy('DT_VD')\
            .drop_duplicates(subset=['VIN', 'COUNTRY'])

        # join both dataframes
        dfs = [dfSAMARA_VIN, dfSAMARA_SUM]
        dfSAMARA_VIN = functools.reduce(lambda left, right: left.join(right, on=['VIN', 'COUNTRY']), dfs)

        # Renaming columns and replacing 0 vcalues with None
        dfSAMARA_VIN = dfSAMARA_VIN \
            .withColumn('PRIX_VENTE_AJ',
                        F.when(F.col('PRIX_VENTE_AJ') == '0', F.lit(None)).otherwise(F.col('PRIX_VENTE_AJ'))) \
            .withColumn('PV_OPTIONS', F.when(F.col('PV_OPTIONS') == '0', F.lit(None)).otherwise(F.col('PV_OPTIONS'))) \
            .withColumn('PV_VERSION', F.when(F.col('PV_VERSION') == '0', F.lit(None)).otherwise(F.col('PV_VERSION'))) \
            .withColumn('MACOM_CONSO_AJ',
                        F.when(F.col('MACOM_CONSO_AJ') == '0', F.lit(None)).otherwise(F.col('MACOM_CONSO_AJ'))) \
            .withColumn('MACOM_CONSO_OPTION',
                        F.when(F.col('MACOM_CONSO_OPTION') == '0', F.lit(None)).otherwise(F.col('MACOM_CONSO_OPTION'))) \
            .withColumn('MACOM_ENTITE_VERSION', F.when(F.col('MACOM_ENTITE_VERSION') == '0', F.lit(None)).otherwise(
            F.col('MACOM_ENTITE_VERSION'))) \
            .withColumn('RBCV_AJ', F.when(F.col('RBCV_AJ') == '0', F.lit(None)).otherwise(F.col('RBCV_AJ'))) \
            .withColumn('VOLUME_AJ', F.when(F.col('VOLUME_AJ') == '0', F.lit(None)).otherwise(F.col('VOLUME_AJ'))) \
            .withColumn('LIB_SEG', F.regexp_replace(F.col('LIB_SEG'), 'SEGMENT ', '')) \
            .select(conf['ColumnToSelect'])

        dfMARGES = session.read.parquet(conf['dfMarges'])
        column_values_marges = {row['KEY_MARGIN']: row['MARGE_U'] for row in dfMARGES.collect()}
        mapping_expr = F.create_map([F.lit(x) for x in chain(*column_values_marges.items())])

        dfSAMARA_VIN = dfSAMARA_VIN\
            .withColumn('FIX_MARGIN', mapping_expr[dfSAMARA_VIN['FIX_MARGIN']])\
            .withColumn('LIST_PRICE',F.col('PRIX_VENTE_AJ') * (F.lit(1) + F.col('FIX_MARGIN').cast('float')))

        log("info", "Samara processing is finished.")
        return dfSAMARA_VIN

    def mxusm_processing(write_hdfs=True):
        log("info", "MXUSM processing starting.")
        # Read the configurations
        conf = load_config("mxusm")
        pathMadax = conf['dfMADAX']
        pathOPV = conf['dfOPV']

        if country is not None:
            pathMadax = pathMadax + 'COUNTRY=' + mapping_ctry[country] + '/'
            pathOPV = pathOPV + 'COUNTRY=' + mapping_ctry[country] + '/'
        if brand is not None:
            pathMadax = pathMadax + 'BRAND=' + brand + '/'
            pathOPV = pathOPV + 'BRAND=' + brand + '/'

        dfMADAX = session.read.parquet(pathMadax)
        dfOPV = session.read.parquet(pathOPV)
        dfSAMARAVIN = samara_processing()
        dfTAX = session.read.parquet(conf['dfTAX'])

        if country is not None:
            dfMADAX = dfMADAX.withColumn('COUNTRY', F.lit(mapping_ctry[country]))
            dfOPV = dfOPV.withColumn('COUNTRY', F.lit(mapping_ctry[country]))
        if brand is not None:
            dfMADAX = dfMADAX.withColumn('BRAND', F.lit(brand))
            dfOPV = dfOPV.withColumn('BRAND', F.lit(brand))

        # Renaming VIN column in madax, samara and opv and dropping year, month and day
        dfSAMARAVIN = dfSAMARAVIN.drop("year", "month", "day")
        dfMADAX = dfMADAX.drop("year", "month","day", "BRAND")
        dfOPV = dfOPV.drop("year", "month", "day","BRAND")

        # Make the outer join between MADAX and SAMARA on VIN and Country
        dfMXUSM = dfMADAX.join(dfSAMARAVIN, on=['COUNTRY', 'VIN'], how='right_outer')
        # Make the left join between OPV and MXSM on VIN and Country
        dfMXUSM = dfMXUSM.join(dfOPV, on=['COUNTRY', 'VIN'], how='left_outer')
        # Make the left join between tax rate and MXSM on VIN and Country
        dfMXUSM = dfMXUSM.join(dfTAX, on=['COUNTRY'], how='left_outer')

        # Merge columns in common between MADAX and SAMARA
        dfMXUSM = dfMXUSM\
            .withColumn('DATE_SALE',F.when(F.col('DATVENT').isNull(), F.col('DT_VD')).otherwise(F.col('DATVENT')))\
            .withColumn('DATE_ORDER', F.when(F.col('DATCCLT').isNull(),F.col('DATE_COMANDE')).otherwise(F.col('DATCCLT')))\
            .withColumn('LIB_TYPUTIL', F.when(F.col('TYPUTIL').isNull(),F.col('TYP_UTIL_VD')).otherwise(F.col('TYPUTIL')))\

        dfMXUSM = dfMXUSM.replace(to_replace=mapping_countries_eng, subset=['COUNTRY'])

        # Variable calculations
        dfMXUSM = dfMXUSM \
        .withColumn('TOT_ADV_AFT_TAX', (F.abs(F.when(F.col('TOTAL_REMISE').isNull(), 0).otherwise(F.col('TOTAL_REMISE'))) + F.when( F.col('NCL_VO_IMPAYUDAREC').isNull(), 0).otherwise(F.col('NCL_VO_IMPAYUDAREC'))))\
        .withColumn('TOT_ADV_PRE_TAX', F.col('TOT_ADV_AFT_TAX') / ( 1 + F.col('TAX_RATE').cast('float')))\
        .withColumn('TRADE_IN_AID_PRE', F.when(F.col('NCL_VO_IMPAYUDAREC').isNull(), 0).otherwise( F.col('NCL_VO_IMPAYUDAREC')) / (1 + F.col('TAX_RATE').cast('float')))\
        .withColumn('TRADE_IN_AID_PRE', F.when(F.col('TRADE_IN_AID_PRE') == '0',F.lit(None)).otherwise(F.col('TRADE_IN_AID_PRE')))\
        .withColumn('TOT_ADV_PRE_TAX', F.when(F.col('TOT_ADV_PRE_TAX') == '0',F.lit(None)).otherwise( F.col('TOT_ADV_PRE_TAX')))\
        .withColumn('TOT_ADV_AFT_TAX', F.when(F.col('TOT_ADV_AFT_TAX') == '0',F.lit(None)).otherwise(F.col('TOT_ADV_AFT_TAX')))\
        .withColumn('DISC_PRE', F.when(F.col('COUNTRY') == 'Spain', 0).otherwise(F.when(((F.col('NCL_VO_IMPAYUDAREC') > 0) | (F.col('NCL_VO_VALON') > 0)), 0).otherwise(F.when(F.abs(F.col('TOTAL_REMISE_PRE')) > 0, 1).otherwise(0))))\
        .withColumn('DISC_AFT', F.when(F.col('COUNTRY') == 'Spain', 0).otherwise(F.when(((F.abs(F.col('TOTAL_REMISE_PRE')) > 0) & (F.col('NCL_VO_IMPAYUDAREC') > 0) & (F.col('VEHICULE_REPENDRE_PRE') > 0)), 1).otherwise(0)))\
        .withColumn('VIN_DM', F.when(F.col('COUNTRY') == 'Spain', 1).otherwise(F.col('DISC_AFT') + F.col('DISC_PRE')))\
        .withColumn('VEH_AGE', F.datediff(F.col('DATE_ORDER'), F.col('DATPROD')))\
        .withColumn('STOCK_AGE',F.when(F.col('VEH_AGE') > 0, F.col('VEH_AGE')).otherwise(F.lit(None)))\
        .withColumn('VEH_AGE_GROUP',F.when(F.col('VEH_AGE') < 0, "< 0").otherwise(F.when((F.col('VEH_AGE') >= 0) & (F.col('VEH_AGE') < 30),"[0;30[").otherwise(F.when((F.col('VEH_AGE') >= 30) & (F.col('VEH_AGE') < 90),"[30;90[").otherwise(F.when((F.col('VEH_AGE') >= 90) & (F.col('VEH_AGE') <= 180),"[90;180[").otherwise(F.when(F.col('VEH_AGE') >= 180, ">= 180").otherwise(F.lit(None)))))))\
        .withColumn('DELIVERY_TIME',F.datediff(F.col('DATE_ORDER'), F.col('DATE_SALE')) * (-1))

        # Select columns from MX-SM-OPV
        dfMXUSM = dfMXUSM.drop('COD_OFR', 'NCL_CODCRE', 'DEDUCCION_IMPUESTOS',
                                                     'TRANSFORMATIONS_PRE', 'PRIX_FINAL_PRE', 'TARIF_VEHICULE',
                                                     'TARIF&OPTIONs', 'TARIF&OPTIONs_PRE', 'CRE_MARCA', 'DATE_COMANDE',
                                                     'NCL_PD_SUBTOTAL2', 'FINITION', 'FIN_LFINITIONAO', 'ACC_PRE',
                                                     'NCL_VO_IMPAYUDAREC', 'NCL_VO_IMPAYUDAREC_PRE', 'FRAIS_ANEXXES',
                                                     'TOTAL_REMISE_PRE', 'TOTAL_REMISE', 'PRIX_FINAL',
                                                     'TARIF_VEHICULE_PRE', 'OPCIONES_PRE', 'NCL_VO_PRIMACONVERSION',
                                                     'NCL_VO_PRIMACONVERSION_PRE', 'ACC', 'CONTRAT_SERVICE',
                                                     'BONUS_MALUS', 'NCL_VO_VALON', 'TRANSFORMATIONS',
                                                     'TRANSFORMATIONS_PRE', 'OPCIONES', 'OPCIONES_PRE',
                                                     'TAXE_PARAFISCALE', 'day', 'TAX_RATE', 'VEHICULE_REPENDRE_PRE',
                                                     'DIV_MARGIN')\
            .drop_duplicates()

        if write_hdfs:
            write_to_hdfs(dfMXUSM, conf["HdfsFileRefinedPath"],("COUNTRY", "BRAND"))

        log("info", "MXUSM processing is finished.")

    def vinpromo_processing(write_hdfs=True):
        log("info", "Samara processing starting.")
        # Read the configurations
        confSamara = load_config("samara")
        conf = load_config("vinpromo")
        path = confSamara['hdfsFileStandardPath']

        if country is not None:
            path = path + 'COUNTRY=' + mapping_ctry[country] + '/'

        if brand is not None:
            path = path + 'BRAND=' + brand + '/'

        dfSAMARA = session.read.parquet(path)

        if country is not None:
            dfSAMARA = dfSAMARA.withColumn('COUNTRY', F.lit(mapping_ctry[country]))

        if brand is not None:
            dfSAMARA = dfSAMARA.withColumn('BRAND', F.lit(brand))


        # dropping the duplicate lines based on selected columns
        dfVINPROMO = dfSAMARA \
            .select(conf['ColumnToSelect']) \
            .drop_duplicates()\
            .replace(to_replace=mapping_rub, subset=['LIB_RUB'])
        dfSAMARAVIN = samara_processing()

        # cleaning and dropping duplicates
        dfVINPROMO = dfVINPROMO\
            .filter((F.col('MCX_VARIABLES') != 0))\
            .drop('year','month')\
            .drop_duplicates()

        # if opc or rub null, fill with Unknown value
        dfVINPROMO = dfVINPROMO\
            .withColumn('CODE_OPC',F.when(F.col('CODE_OPC').isNull(), 'Unknown').otherwise(F.col('CODE_OPC')))\
            .withColumn('LIB_OPC',F.when(F.col('LIB_OPC').isNull(), 'Unknown').otherwise(F.col('LIB_OPC')))

        dfCT_STOCKAGE = dfSAMARAVIN.select("VIN", "COUNTRY", "BRAND", "CT_STOCKAGE") \
            .drop_duplicates() \
            .withColumn("LIB_OPC", F.lit('Financing Costs')) \
            .withColumn('LIB_RUB', F.lit('Financing Costs')) \
            .withColumn('CODE_OPC', F.lit('Financing Costs')) \
            .withColumn('CODE_RUB', F.lit('Financing Costs'))
        dfCT_STOCKAGE = dfCT_STOCKAGE.select('VIN', 'COUNTRY', 'BRAND', 'LIB_OPC', 'CODE_RUB', 'LIB_RUB',
                                             'CODE_OPC', 'CT_STOCKAGE')

        dfFIXED_MARGIN = dfSAMARAVIN.select("VIN", "COUNTRY", "BRAND", "List_Price", "FIX_MARGIN") \
            .drop_duplicates() \
            .withColumn("LIB_OPC", F.lit('Fixed Margin')) \
            .withColumn('LIB_RUB', F.lit('Fixed Margin')) \
            .withColumn('CODE_OPC', F.lit('Fixed Margin')) \
            .withColumn('CODE_RUB', F.lit('Fixed Margin')) \
            .withColumn('FIXED_MARGIN', F.col("LIST_PRICE") * F.col("FIX_MARGIN"))
        dfFIXED_MARGIN = dfFIXED_MARGIN.select('VIN', 'COUNTRY', 'BRAND', 'LIB_OPC', 'CODE_RUB', 'LIB_RUB',
                                               'CODE_OPC', 'FIX_MARGIN')

        dfVINPROMO = dfVINPROMO.union(dfCT_STOCKAGE)
        dfVINPROMO = dfVINPROMO.union(dfFIXED_MARGIN)

        dfVINPROMO = dfVINPROMO \
            .withColumn('LIB_OPC', F.concat(F.col('LIB_OPC'), F.lit(' - '), F.when(
            F.col('LIB_RUB').isin('PSA - Bonus for dealer performance', 'PSA - Bonus for quality'),
            'Network Remuneration').otherwise('Client Promotions')))

        if write_hdfs:
            write_to_hdfs(dfVINPROMO, conf["hdfsFileRefinedPath"], ["COUNTRY", "BRAND"])

    #list amounts + string to put in conf file
    def samarex_processing(write_hdfs=True):
        log("info", "Samarex construction begins.")
        # Read the configurations
        conf = load_config("samarex")
        confCHAN = load_config('channel_type')
        path = conf['HdfsFileStandardPath']

        if country is not None:
            path = path + 'COUNTRY=' + column_values_ctry[country] + '/'

        if brand is not None:
            path = path + 'BRAND=' + brand + '/'

        dfSAMAREX = session.read.parquet(path)
        dfCHAN = session.read.parquet(conf['dfCHAN']) \
            .filter(F.col(confCHAN['ColumnToMapSamarex']).isNotNull())

        dfSAMAREX = dfSAMAREX.drop_duplicates()
        dfSAMAREX = dfSAMAREX.withColumn('YEAR', F.substring(F.col('CYCLE'), -4, 4))
        dfSAMAREX = dfSAMAREX.withColumn('DATE',F.concat(F.lit('01/' ), F.col('MONTH').cast('string'), F.lit('/'), F.col('YEAR')))
        dfSAMAREX = dfSAMAREX.withColumn('RM035_TOT',F.col('RM035').cast('float')*F.col('VOLUME_AJ').cast('float'))
        dfSAMAREX = dfSAMAREX.withColumn('RM036_TOT',F.col('RM036').cast('float')*F.col('VOLUME_AJ').cast('float'))
        dfSAMAREX = dfSAMAREX.withColumn('RM037_TOT',F.col('RM037').cast('float')*F.col('VOLUME_AJ').cast('float'))
        dfSAMAREX = dfSAMAREX.withColumn('RM039_TOT',F.col('RM039').cast('float')*F.col('VOLUME_AJ').cast('float'))
        dfSAMAREX = dfSAMAREX.withColumn('RM280_TOT',F.col('RM280').cast('float')*F.col('VOLUME_AJ').cast('float'))
        dfSAMAREX = dfSAMAREX.withColumn('RBCV_AJ_TOT',F.col('RBCV_AJ').cast('float')*F.col('VOLUME_AJ').cast('float'))
        dfSAMAREX = dfSAMAREX.withColumn('LIST_PRICE_U_TOT',F.col('LIST_PRICE_U').cast('float')*F.col('VOLUME_AJ').cast('float'))
        dfSAMAREX = dfSAMAREX.withColumn('MACOM_CONSO_AJ_TOT',F.col('MACOM_CONSO_AJ').cast('float')*F.col('VOLUME_AJ').cast('float'))
        dfSAMAREX = dfSAMAREX.withColumn('PV_VERSION_TOT',F.col('PV_VERSION').cast('float')*F.col('VOLUME_AJ').cast('float'))
        dfSAMAREX = dfSAMAREX.withColumn('PV_OPTIONS_TOT',F.col('PV_OPTIONS').cast('float')*F.col('VOLUME_AJ').cast('float'))

        dfSAMAREX = dfSAMAREX.withColumn('VOLUME_AJ', F.col('VOLUME_AJ').cast('float'))
        dfSAMAREX=dfSAMAREX.withColumn('PRIX_VENTE_AJ',F.col('PRIX_VENTE_AJ').cast('float'))
        dfSAMAREX=dfSAMAREX.withColumn('PRIX_VENTE_AJ_DEV',F.col('PRIX_VENTE_AJ_DEV').cast('float'))
        dfSAMAREX=dfSAMAREX.withColumn('MACOM_CONSO_AJ',F.col('MACOM_CONSO_AJ').cast('float'))
        dfSAMAREX=dfSAMAREX.withColumn('PV_VERSION',F.col('PV_VERSION').cast('float'))
        dfSAMAREX=dfSAMAREX.withColumn('PV_OPTIONS',F.col('PV_OPTIONS').cast('float'))
        dfSAMAREX=dfSAMAREX.withColumn('MACOM_CONSO_OPTION',F.col('MACOM_CONSO_OPTION').cast('float'))
        dfSAMAREX=dfSAMAREX.withColumn('MCX_VARIABLES_AJ',F.col('MCX_VARIABLES_AJ').cast('float'))
        dfSAMAREX=dfSAMAREX.withColumn('RM035',F.col('RM035').cast('float'))
        dfSAMAREX=dfSAMAREX.withColumn('RM036',F.col('RM036').cast('float'))
        dfSAMAREX=dfSAMAREX.withColumn('RM037',F.col('RM037').cast('float'))
        dfSAMAREX=dfSAMAREX.withColumn('RM039',F.col('RM039').cast('float'))
        dfSAMAREX=dfSAMAREX.withColumn('RM280',F.col('RM280').cast('float'))
        dfSAMAREX=dfSAMAREX.withColumn('RBCV_AJ',F.col('RBCV_AJ').cast('float'))
        dfSAMAREX=dfSAMAREX.withColumn('LIST_PRICE_U',F.col('LIST_PRICE_U').cast('float'))
        dfSAMAREX=dfSAMAREX.withColumn('YEAR',F.col('YEAR').cast('int'))
        dfSAMAREX=dfSAMAREX.withColumn('DATE', F.to_date(F.col('DATE'), 'dd/MM/yyyy'))


        column_values_channel_type = {row['LIB_SAMAREX']:row['LIB_CHAN'] for row in dfCHAN.collect()}
        dfSAMAREX = dfSAMAREX.replace(to_replace=column_values_channel_type, subset=['CMA'])


        dfSAMAREX = dfSAMAREX.withColumn('CHANNEL', F.regexp_replace(F.col('CHANNEL'), "\s", ""))
        dfSAMAREX = dfSAMAREX.filter((F.col('CHANNEL') == 'B2C')|(F.col('CHANNEL') == 'B2B'))
        dfSAMAREX = dfSAMAREX.drop('CHANNEL')

        if write_hdfs:
            write_to_hdfs(dfSAMAREX,  conf["HdfsFileRefinedPath"], ("COUNTRY", "BRAND"))
        log("info", "Samarex construction is finished.")


    #wait for vinpromo dataset on refined
    def promo_processing(write_hdfs=True):
        log("info", "Promo construction at run.")

        # Read the configurations
        conf = load_config("promo")
        path = conf['HdfsFileStandardPath']
        pathVINPROMO = conf['dfVINPROMO']

        if country is not None:
            path = path + 'COUNTRY=' + mapping_ctry[country] + '/'
            #column_values_ctry[country] + '/'
            pathVINPROMO = pathVINPROMO + 'COUNTRY=' + mapping_ctry[country] + '/'
            #column_values_ctry[country] + '/'
        if brand is not None:
            path = path + 'BRAND=' + brand + '/'
            pathVINPROMO = pathVINPROMO + 'BRAND=' + brand + '/'

        dfPROMO = session.read.parquet(path)
        dfVINPROMO = session.read.parquet(pathVINPROMO)
        dfCLASS = session.read.parquet(conf['dfCLASS'])
        dfCLASS.show()

        listC = countries
        if country is not None:
            dfPROMO = dfPROMO.withColumn('COUNTRY', F.lit(mapping_ctry[country]))
            dfVINPROMO = dfVINPROMO.withColumn('COUNTRY', F.lit(mapping_ctry[country]))
            listC = [country]

        listB = brands
        if brand is not None:
            dfPROMO = dfPROMO.withColumn('BRAND', F.lit(brand))
            dfVINPROMO = dfVINPROMO.withColumn('BRAND', F.lit(brand))
            listB = [brand]


        dfPROMO=dfPROMO.drop_duplicates()

        dfCliPro = dfCLASS.filter(F.col('LIB_CLASS')=='Client Promotions').select('VME_CAT')
        dfCliPro.show()
        list_cli_prom = [i['VME_CAT'] for i in dfCliPro.collect()]
        dfNetRem = dfCLASS.filter(F.col('LIB_CLASS')=='Network Remuneration').select('VME_CAT')
        dfNetRem.show()
        list_net_rem = [i['VME_CAT'] for i in dfNetRem.collect()]

        dicCategories = {}
        for c in listC:
            for b in listB:
                dataCategories = [('Financing Costs','Fixed Margin'), ('FINANCING COSTS','FIXED MARGIN'),(c,c),(b,b) ]
                dicCategories = {dfPROMO.columns[i]: dataCategories[i] for i in range(len(dfPROMO.columns))}

        dfCAT_pandas = pd.DataFrame(dicCategories, columns=dfPROMO.columns)
        # This is a temporary solution for session.createDataFrame(df_pandas) that brings error
        # get path of the project
        pwd = os.getcwd()
        path = pwd[6:-3]

        dfCAT_pandas.to_csv("/gpfs/" + path + "data/catTemp.csv", index=False)

        # convert pandas dataframe into spark dataframe
        dfCAT = session.read.format("csv").option("header", "true").load("hdfs:///" + path + "data/catTemp.csv")
        
        dfPROMO = dfPROMO.union(dfCAT)


        dfPROMO = dfPROMO.withColumn('LIB_CLASS', F.when(F.col('LIB_CAT').isin(list_cli_prom), 'Client Promotions').otherwise(F.when(F.col('LIB_CAT').isin(list_net_rem),'Network Remuneration').otherwise(F.col('LIB_OPC'))))
        dfPROMO = dfPROMO.withColumn('LIB_OPC',F.concat(F.col('LIB_OPC'),F.lit(' - '), F.col('LIB_CLASS')))
        #dfPROMO.show()
        dfPROMO = dfPROMO.drop('LIB_CLASS')
        #dfPROMO.show()
        # add Unknow categorie if lib opc is unknown
        dicUnknownCategories = {}
        for c in listC:
            for b in listB:
                dataCategories = [('Unknown - Client Promotions', 'Unknown - Network Remuneration'), ('Unknown - Client Promotions', 'Unknown - Network Remuneration'), (c, c),
                                  (b, b)]
                dicUnknownCategories = {dfPROMO.columns[i]: dataCategories[i] for i in range(len(dfPROMO.columns))}

        dfCATUnknown_pandas = pd.DataFrame(dicUnknownCategories, columns=dfPROMO.columns)


        # This is a temporary solution for session.createDataFrame(df_pandas) that brings error
        # get path of the project
        dfCATUnknown_pandas.to_csv("/gpfs/" + path + "data/catUnknownTemp.csv", index=False)

        # convert pandas dataframe into spark dataframe
        dfCATUnkown = session.read.format("csv").option("header", "true").load("hdfs:///" + path + "data/catUnknownTemp.csv")
        #dfCATUnkown.show()
        dfPROMO = dfPROMO.union(dfCATUnkown)
        dfPROMO.show()
        dfVINPROMO = dfVINPROMO.drop('year','month','day')
        df_toFlagged = dfVINPROMO.join(dfPROMO.drop('BRAND'), on=['LIB_OPC', 'COUNTRY'],how='left_anti')
        #df_toFlagged.show()
        df_toFlagged = df_toFlagged.select('LIB_OPC', 'LIB_RUB', 'COUNTRY','BRAND')\
            .withColumn('LIB_CAT', F.when((F.col('LIB_RUB')!='')|(F.col('LIB_RUB').isNotNull()),F.concat(F.lit('To be flagged'),F.lit(' - '), F.when(F.col('LIB_RUB').isin('PSA - Bonus for dealer performance','PSA - Bonus for quality'), 'Network Remuneration').otherwise('Client Promotions'))).otherwise(F.lit('To be flagged')))\
            .drop_duplicates()
        df_toFlagged = df_toFlagged.drop('LIB_RUB')\
            .drop_duplicates()
        df_toFlagged = df_toFlagged.select('LIB_OPC', 'LIB_CAT', 'COUNTRY','BRAND')
        dfPROMO = dfPROMO.union(df_toFlagged)
        dfPROMO = dfPROMO.drop_duplicates()
        #dfPROMO.show()

        if write_hdfs:
           write_to_hdfs(dfPROMO,conf["HdfsFileRefinedPath"])
        log("info", "Promo construction is finished.")


    #list amounts + string to put in conf file
    def opv_processing(write_hdfs=True):
        log("info", "OPV processing starting.")
        # Read the configurations
        conf = load_config("opv")
        path = conf['HdfsFileStandardPath']

        if country is not None:
            path = path + 'COUNTRY=' + mapping_ctry[country] + '/'

        if brand is not None:
            path = path + 'BRAND=' + brand + '/'

        dfOPV = session.read.parquet(path)

        if country is not None:
            dfOPV = dfOPV.withColumn('COUNTRY', F.lit(mapping_ctry[country]))

        if brand is not None:
            dfOPV = dfOPV.withColumn('BRAND', F.lit(brand))

        dfTAX = session.read.parquet(conf['dfTAX'])
        dfOPV = dfOPV.withColumn('DATE_COMANDE',F.when(F.to_date(F.col("DATE_COMANDE"),"yyyy-MM-dd").isNotNull(),F.to_date(F.col("DATE_COMANDE"),"yyyy-MM-dd"))
            .otherwise(F.when(F.to_date(F.col("DATE_COMANDE"),"yyyy MM dd").isNotNull(),F.to_date(F.col("DATE_COMANDE"),"yyyy MM dd"))
            .otherwise(F.when(F.to_date(F.col("DATE_COMANDE"),"dd/MM/yyyy").isNotNull(),F.to_date(F.col("DATE_COMANDE"),"dd/MM/yyyy"))
            .otherwise(None))))

        dfOPV = dfOPV.filter(~(F.col('VIN')=='                 '))
        dfOPV = dfOPV.drop_duplicates()
        dfOPV = dfOPV.orderBy('DATE_COMANDE', ascending=False).coalesce(1).dropDuplicates(subset=['VIN'])

        list_amounts = ['ACC','ACC_PRE', 'NCL_VO_IMPAYUDAREC', 'FRAIS_ANEXXES', 'TOTAL_REMISE_PRE', 'TOTAL_REMISE', 'PRIX_FINAL', 'TARIF_VEHICULE_PRE', 'TARIF_VEHICULE', 'OPCIONES', 'OPCIONES_PRE','NCL_VO_PRIMACONVERSION','CONTRAT_SERVICE','BONUS_MALUS','NCL_VO_VALON', 'TRANSFORMATIONS_PRE','TAXE_PARAFISCALE','VEHICULE_REPENDRE_PRE', 'DEDUCCION_IMPUESTOS']

        for a in list_amounts:
            dfOPV= dfOPV.withColumn(a, F.when(F.col(a)=='0', F.lit(None)).otherwise(F.col(a)))
            dfOPV = dfOPV.withColumn(a, F.regexp_replace(F.col(a),'\\,','.'))
            dfOPV = dfOPV.withColumn(a, F.regexp_replace(F.col(a),'[^0-9\\-\\.]',''))
            dfOPV = dfOPV.withColumn(a,F.col(a).cast("float"))

        # convert string columns into string format\n",
        list_strings = ['VIN', 'COD_OFR', 'BRAND', 'NCL_CODCRE', 'FINITION', 'COUNTRY', 'FIN_LFINITIONAO']

        for s in list_strings:
            dfOPV = dfOPV.withColumn(s,F.col(s).cast("string"))

        dfOPV = dfOPV.join(dfTAX, on=['COUNTRY'], how='left_outer')
        dfOPV = dfOPV.withColumn('TARIF&OPTIONs_PRE', F.col('TARIF_VEHICULE_PRE') + F.col('OPCIONES_PRE'))
        dfOPV = dfOPV.withColumn('TARIF&OPTIONs', F.col('TARIF_VEHICULE') + F.col('OPCIONES'))
        dfOPV = dfOPV.withColumn('PRIX_FINAL_PRE', F.col('PRIX_FINAL')/(1+F.col('TAX_RATE').cast('float')))
        dfOPV = dfOPV.withColumn('NCL_VO_PRIMACONVERSION_PRE', F.col('NCL_VO_PRIMACONVERSION')/(1+F.col('TAX_RATE').cast('float')))
        dfOPV = dfOPV.withColumn('NCL_VO_IMPAYUDAREC_PRE', F.col('NCL_VO_IMPAYUDAREC')/(1+F.col('TAX_RATE').cast('float')))
        dfOPV = dfOPV.withColumn('TOTAL_REMISE', F.abs(F.col('TOTAL_REMISE')))
        dfOPV = dfOPV.withColumn('TOTAL_REMISE_PRE', F.abs(F.col('TOTAL_REMISE_PRE')))
        dfOPV = dfOPV.withColumn('DIV_MARGIN', F.col('TOTAL_REMISE_PRE') + (F.col('NCL_VO_IMPAYUDAREC') + F.col('NCL_VO_PRIMACONVERSION') ) /(1+F.col('TAX_RATE').cast('float')))
        dfOPV = dfOPV.withColumn('DEDUCCION_IMPUESTOS', F.lit(None).cast('float'))
        dfOPV = dfOPV.drop('TAX_RATE')
        dfCountriesInserted = dfOPV.select("COUNTRY")
        dfCountriesInserted.drop_duplicates()
        dfCountriesInserted.show()
        if write_hdfs:
            write_to_hdfs(dfOPV, conf["HdfsFileRefinedPath"],("COUNTRY", "BRAND"))

        log("info", "OPV processing is finished.")


    def goal__processing(write_hdfs=True):
        log("info", "Goal copy")

        # Read the configurations
        conf = load_config("goal")
        confCHAN = load_config("channel_type")
        path = conf['HdfsFileStandardPath']

        if country is not None:
            path = path + 'COUNTRY=' + mapping_ctry[country] + '/'

        if brand is not None:
            path = path + 'BRAND=' + brand + '/'

        dfGOAL= session.read.parquet(path)
        dfFAM = session.read.parquet(conf['dfFAM'])
        dfCHAN = session.read.parquet(conf['dfCHAN']) \
            .filter(F.col(confCHAN['ColumnToMapGoal']).isNotNull())

        for a in conf['amountsToCast']:
            dfGOAL = dfGOAL.withColumn(a, F.when(F.col(a)=='0', F.lit(None)).otherwise(F.col(a)))
            dfGOAL = dfGOAL.withColumn(a, F.regexp_replace(F.col(a),'\\,','.'))
            dfGOAL = dfGOAL.withColumn(a, F.regexp_replace(F.col(a),'[^0-9\\-\\.]',''))
            dfGOAL = dfGOAL.withColumn(a,F.col(a).cast("float"))

        dfGOAL = dfGOAL.withColumn('YEAR',F.col('YEAR').cast("int"))

        column_value_fam = {row['FAM_TO_MAP']:row['LIB_FAM'] for row in dfFAM.collect()}
        dfGOAL = dfGOAL.replace(to_replace=column_value_fam, subset=['LIB_FAM'])

        column_value_typveh={'VP':'PC','VU':'CV'}
        dfGOAL = dfGOAL.replace(to_replace=column_value_typveh, subset=['PCCV'])

        dfGOAL = dfGOAL.filter(F.col('PCCV')!='AJUST')

        column_value_energy={'(vide)':''}
        dfGOAL = dfGOAL.replace(to_replace=column_value_energy, subset=['ENERGY'])

        column_values_channel_type = {row['LIB_GOAL']: row['LIB_CHAN'] for row in dfCHAN.collect()}
        dfGOAL = dfGOAL.replace(to_replace=column_values_channel_type, subset=['CHANNEL'])

        dfGOAL= dfGOAL .withColumn('DATE', F.to_date(F.col('DATE'), 'dd/MM/yy'))

        dfGOAL = dfGOAL.drop_duplicates()

        dfGOAL.show()
        if write_hdfs:
            write_to_hdfs(dfGOAL, conf["HdfsFileRefinedPath"],("COUNTRY", "BRAND"))
        log("info", "goal is copied.")


    def pdv_processing(write_hdfs=True):
        log("info", "pdv copy")

        # Read the configurations
        conf = load_config("pdv")
        path = conf['HdfsFileStandardPath']

        if country is not None:
            path = path + 'COUNTRY=' + mapping_ctry[country] + '/'

        if brand is not None:
            path = path + 'BRAND=' + brand + '/'

        dfPDV = session.read.parquet(path)

        if country is not None:
            dfPDV = dfPDV.withColumn('COUNTRY', F.lit(mapping_ctry[country]))

        if brand is not None:
            dfPDV = dfPDV.withColumn('BRAND', F.lit(brand))

        dfPDV=dfPDV.filter(F.col('PRISEC')=="P")
        dfPDV=dfPDV.withColumn('CODE_SC_PDV',F.trim(F.col('CODE_SC_PDV')))
        dfPDV = dfPDV.drop_duplicates()

        dfPDV.show()
        if write_hdfs:
            write_to_hdfs(dfPDV,  conf["HdfsFileRefinedPath"],("COUNTRY", "BRAND"))

        log("info", "pdv is copied.")


    def family_processing(write_hdfs=True):
        log("info", "Family copy")
        # Read the configurations
        conf = load_config("family")
        path = conf['HdfsFileStandardPath']

        if brand is not None:
            path = path + 'BRAND=' + brand + '/'

        dfFAMILY = session.read.parquet(path)

        if brand is not None:
            dfFAMILY = dfFAMILY.withColumn('COUNTRY', F.lit(brand))

        dfFAMILY_FINALE = dfFAMILY.drop(conf['ColumnToDrop'])
        dfFAMILY_FINALE = dfFAMILY_FINALE.drop_duplicates()

        dfFAMILY_FINALE.show()

        if write_hdfs:
            write_to_hdfs(dfFAMILY_FINALE,  conf["HdfsFileRefinedPath"],["BRAND"])
        log("info", "family is copied.")

    def date_creating (write_hdfs=True):
        log("info", "Date creating begins")


        # Read the configurations
        conf = load_config("date")
        start_date = datetime.strptime(fromDate, '%d/%m/%Y')#.strftime("%Y-%m-%d")
        start_date = start_date.strftime('%d/%m/%y')
        end_date = datetime.strptime(toDate, '%d/%m/%Y')#.strftime("%Y-%m-%d")
        end_date = end_date.strftime('%d/%m/%y')
        print(start_date)
        print(end_date)
        data_date = pd.date_range(start=start_date, end=end_date)
        print(data_date)
        formatted_date = data_date.strftime('%d/%m/%y')
        print(formatted_date)
        # construct pandas dataframe
        df_date = pd.DataFrame(data_date, columns=['Date'])
        print(df_date)
        # construct all columns needed
        def add0(n):
            if len(str(n)) < 2:
                return '0' + str(n)
            else:
                return str(n)

        df_date['Day'] = pd.DatetimeIndex(df_date['Date']).day
        df_date['Month'] = pd.DatetimeIndex(df_date['Date']).month
        df_date['Year'] = pd.DatetimeIndex(df_date['Date']).year
        df_date['MonthYearNumber'] = pd.DatetimeIndex(df_date['Date']).month.astype(str) + '/' + df_date['Year'].apply(
            lambda x: str(x)[-2:])
        df_date['MonthNameShort'] = df_date['Month'].apply(lambda x: calendar.month_abbr[x])
        df_date['MonthNameLong'] = df_date['Month'].apply(lambda x: calendar.month_name[x])
        df_date['MonthYearShort'] = df_date['MonthNameShort'].astype(str) + '-' + df_date['Year'].astype(str)
        df_date['MonthYearLong'] = df_date['MonthNameLong'].astype(str) + ' ' + df_date['Year'].astype(str)
        df_date['Quarter'] = df_date['Month'].apply(
            lambda m: 'Q1' if m <= 3 else ('Q2' if 3 < m <= 6 else ('Q3' if 6 < m <= 9 else 'Q4')))
        df_date['YearQuarter'] = df_date['Year'].astype(str) + ' ' + df_date['Quarter'].astype(str)
        df_date['Day'] = df_date['Day'].apply(add0)
        df_date['Month'] = df_date['Month'].apply(add0)
        df_date['DateAsInteger'] = df_date['Year'].astype(str) + df_date['Month'].astype(str) + df_date['Day'].astype(
            str)
        df_date['MonthShortAsInterger'] = df_date['Year'].astype(str) + df_date['Month'].astype(str)

        # casting a few columns
        df_date['Date'] = df_date['Date'].apply(lambda d: d.strftime('%d/%m/%y'))
        df_date['Month'] = df_date['Month'].astype('int')
        df_date['Year'] = df_date['Year'].astype('int')
        df_date['DateAsInteger'] = df_date['DateAsInteger'].astype('int')
        df_date['MonthShortAsInterger'] = df_date['MonthShortAsInterger'].astype('int')

        #This is a temporary solution for session.createDataFrame(df_pandas) that brings error
        #get path of the project
        pwd = os.getcwd()
        path = pwd[6:-3]

        df_date.to_csv("/gpfs/" + path + "data/dateTemp.csv", index=False)

        # convert pandas dataframe into spark dataframe
        dfDATE = session.read.format("csv").option("header", "true").load("hdfs:///" + path + "data/dateTemp.csv")
        dfDATE = dfDATE.withColumn('Date', F.to_date(F.col('Date'), 'dd/mm/yy'))\
            .withColumn('Month', F.col('Month').cast('integer')) \
            .withColumn('Year', F.col('Year').cast('integer')) \
            .withColumn('DateAsInteger', F.col('DateAsInteger').cast('integer')) \
            .withColumn('MonthShortAsInterger', F.col('MonthShortAsInterger').cast('integer'))

        if write_hdfs:
            #dfDATE = dfDATE.select(
                #*(F.col("`" + c + "`").alias(c.replace(' ', '_')) for c in dfDATE.columns))
            print("VOICI DE DATAFRAME DATE :")
            dfDATE.show()
            write_to_hdfs(dfDATE,  conf["hdfsFileRefinedPath"])
        log("info", "date is copied.")

    def date_order_creating (write_hdfs=True ):
        log("info", "Date order creating begins")
        # Read the configurations
        conf = load_config("date_order")
        #conf = load_config("date")
        start_date = datetime.strptime(fromDate, '%d/%m/%Y')  # .strftime("%Y-%m-%d")
        start_date = start_date.strftime('%d/%m/%y')
        end_date = datetime.strptime(toDate, '%d/%m/%Y')  # .strftime("%Y-%m-%d")
        end_date = end_date.strftime('%d/%m/%y')
        print(start_date)
        print(end_date)
        #dfSAMARAVIN = samara_processing()
        #start_date = str(dfSAMARAVIN.select(F.min(F.col('DATE_COMMANDE'))).collect()[0][0])
        #end_date = str(dfSAMARAVIN.select(F.max(F.col('DATE_COMMANDE'))).collect()[0][0])

        data_date = pd.date_range(start=start_date, end=end_date)
        print(data_date)
        # construct pandas dataframe
        df_date = pd.DataFrame(data_date, columns=['Date'])
        print(df_date['Date'])

        # construct all columns needed
        def add0(n):
            if len(str(n)) < 2:
                return '0' + str(n)
            else:
                return str(n)

        df_date['Day'] = pd.DatetimeIndex(df_date['Date']).day
        df_date['Month'] = pd.DatetimeIndex(df_date['Date']).month
        df_date['Year'] = pd.DatetimeIndex(df_date['Date']).year
        df_date['MonthYearNumber'] = pd.DatetimeIndex(df_date['Date']).month.astype(str) + '/' + df_date['Year'].apply(
            lambda x: str(x)[-2:])
        df_date['MonthNameShort'] = df_date['Month'].apply(lambda x: calendar.month_abbr[x])
        df_date['MonthNameLong'] = df_date['Month'].apply(lambda x: calendar.month_name[x])
        df_date['MonthYearShort'] = df_date['MonthNameShort'].astype(str) + '-' + df_date['Year'].astype(str)
        df_date['MonthYearLong'] = df_date['MonthNameLong'].astype(str) + ' ' + df_date['Year'].astype(str)
        df_date['Quarter'] = df_date['Month'].apply(
            lambda m: 'Q1' if m <= 3 else ('Q2' if 3 < m <= 6 else ('Q3' if 6 < m <= 9 else 'Q4')))
        df_date['Year quarter'] = df_date['Year'].astype(str) + ' ' + df_date['Quarter'].astype(str)
        df_date['Day'] = df_date['Day'].apply(add0)
        df_date['Month'] = df_date['Month'].apply(add0)
        df_date['DateAsInteger'] = df_date['Year'].astype(str) + df_date['Month'].astype(str) + df_date['Day'].astype(
            str)
        df_date['MonthShortAsInterger'] = df_date['Year'].astype(str) + df_date['Month'].astype(str)

        # casting a few columns
        df_date['Date'] = df_date['Date'].apply(lambda d: d.strftime('%d/%m/%y'))
        df_date['Month'] = df_date['Month'].astype('int')
        df_date['Year'] = df_date['Year'].astype('int')
        df_date['DateAsInteger'] = df_date['DateAsInteger'].astype('int')
        df_date['MonthShortAsInterger'] = df_date['MonthShortAsInterger'].astype('int')

        # This is a temporary solution for session.createDataFrame(df_pandas) that brings error
        # get path of the project
        pwd = os.getcwd()
        path = pwd[6:-3]

        df_date.to_csv("/gpfs/" + path + "data/date_orderTemp.csv", index=False)

        # convert pandas dataframe into spark dataframe
        dfDATE = session.read.format("csv").option("header", "true").load("hdfs:///" + path + "data/date_orderTemp.csv")
        dfDATE = dfDATE.withColumn('Date', F.to_date(F.col('Date'), 'dd/mm/yy')) \
            .withColumn('Month', F.col('Month').cast('integer')) \
            .withColumn('Year', F.col('Year').cast('integer')) \
            .withColumn('DateAsInteger', F.col('DateAsInteger').cast('integer')) \
            .withColumn('MonthShortAsInterger', F.col('MonthShortAsInterger').cast('integer'))

        if write_hdfs:
            dfDATE = dfDATE.select(
                *(F.col("`" + c + "`").alias(c.replace(' ', '_')) for c in dfDATE.columns))
            write_to_hdfs(dfDATE,  conf["hdfsFileRefinedPath"])
        log("info", "date is copied.")

    def tax_rate_processing(write_hdfs=True):
        log("info", "tax_rate copy")

        # Read the configurations
        conf = load_config("tax_rate")
        path = conf['HdfsFileStandardPath']

        if country is not None:
            path = path + 'COUNTRY=' + mapping_ctry[country] + '/'

        dfTAX = session.read.parquet(path)
        dfTAX = dfTAX.withColumn('TAX_RATE',F.regexp_replace(F.regexp_replace(F.col("TAX_RATE")," ",""),",",".").cast('float'))
        if country is not None:
            dfTAX = dfTAX.withColumn('COUNTRY', F.lit(mapping_ctry[country]))

        dfTAX_FINALE = dfTAX
        dfTAX_FINALE.show()
        if write_hdfs:
            write_to_hdfs(dfTAX_FINALE,  conf["HdfsFileRefinedPath"],["COUNTRY"])
        log("info", "tax_rate is copied.")


    def sort_client_promotions_processing(write_hdfs=True):
        log("info", "Sort_Client_Promotions copy")

        # Read the configurations
        conf = load_config("sort_client_promotions")
        dfSRT = session.read.parquet(conf['HdfsFileStandardPath'])

        dfSRT.show()
        # copy the file into Standard Zone
        if write_hdfs:
            write_to_hdfs(dfSRT, conf["HdfsFileRefinedPath"])

        log("info", "sort_client_promotions is copied.")


    def channel_type_processing(write_hdfs=True):
        log("info", "channel_Type copy")
        # Read the configurations
        conf = load_config("channel_type")
        dfCHAN = session.read.parquet(conf['HdfsFileStandardPath'])
        dfCHAN_FINALE = dfCHAN.select('LIB_CHAN')
        dfCHAN_FINALE.show()
        # copy the file into Standard Zone
        if write_hdfs:
            print(conf["HdfsFileRefinedPath"])
            write_to_hdfs(dfCHAN_FINALE, conf["HdfsFileRefinedPath"])
        log("info", "channel_type is copied.")

    def motor_processing(write_hdfs=True):
        log("info", "motor copy")
        # Read the configurations
        conf = load_config("motor")
        path = conf['hdfsFileStandardPath']

        if country is not None:
            path = path + 'COUNTRY=' + mapping_ctry[country] + '/'

        dfMOTOR = session.read.parquet(path)

        if country is not None:
            dfMOTOR = dfMOTOR.withColumn('COUNTRY', F.lit(mapping_ctry[country]))

        # map energy column
        column_value_Energy = {'ELECTRICO': 'Electric', 'Diesel': 'Diesel', 'DIESEL': 'Diesel', 'ESSENCE': 'Petrol',
                               'ELECTRIQUE': 'Electric', 'Gasolina': 'Petrol', 'GASOLINA': 'Petrol'}
        dfMOTOR = dfMOTOR.replace(to_replace=column_value_Energy, subset=['ENERGY'])
        dfMOTOR = dfMOTOR.withColumn('ENERGY', F.regexp_replace(F.col("ENERGY"), "\\?", ""))

        # counts duplicates on cod_motor/country
        duplicates = dfMOTOR.groupby('cod_motor', 'COUNTRY').count()
        dfMOTOR = dfMOTOR.join(duplicates, on=['COD_MOTOR', 'COUNTRY'])
        dfMOTOR = dfMOTOR.withColumnRenamed('count', 'count_duplicates')

        # we used drop_duplicates so if column energia is empty and nb of duplicates > 1 , the others duplicates are filled, we can drop this line
        dfMOTOR = dfMOTOR.withColumn('to_keep',
                                       F.when((F.col('ENERGY') == '') & (F.col('count_duplicates') > 1), 0).otherwise(
                                           1))
        dfMOTOR = dfMOTOR.filter(F.col('to_keep') == 1).drop('to_keep')

        # if duplicates remained, there is differents energia values, so fill energia column with Unknown and drop duplicates to have one vin by row
        dfMOTOR = dfMOTOR.withColumn('ENERGY',
                                       F.when(F.col('count_duplicates') > 1, 'Unknown').otherwise(F.col('ENERGY')))
        dfMOTOR = dfMOTOR.drop_duplicates().drop('count_duplicates')

        dfMOTOR.show()
        # copy the file into Standard Zone
        if write_hdfs:
            write_to_hdfs(dfMOTOR, conf["hdfsFileRefinedPath"], ['COUNTRY'])
        log("info", "motor is copied.")

    if data_of == "mxusm":
        opv_processing(write_hdfs=True)
        mxusm_processing(write_hdfs=True)
    elif data_of == "vinpromo":
        vinpromo_processing( write_hdfs=True)
    elif data_of == "opv":
        opv_processing(write_hdfs=True)
    elif data_of == "samarex":
        samarex_processing(write_hdfs=True)
    elif data_of == "motor":
        motor_processing(write_hdfs=True)
    elif data_of == "promo":
        vinpromo_processing(write_hdfs=True)
        promo_processing(write_hdfs=True)
    elif data_of == "channel_type":
        channel_type_processing(write_hdfs=True)
    elif data_of == 'date':
        date_creating(write_hdfs=True)
    elif data_of == 'date_order':
        date_order_creating( write_hdfs=True)
    elif data_of == "family":
        family_processing(write_hdfs=True)
    elif data_of == "pdv":
        pdv_processing(write_hdfs=True)
    elif data_of == "tax_rate":
        tax_rate_processing(write_hdfs=True)
    elif data_of == "sort_client_promotions":
        sort_client_promotions_processing(write_hdfs=True)
    elif data_of == "goal":
        goal__processing(write_hdfs=True)
    elif data_of =='samara':
        samara_processing()

    elif data_of == "all":
        date_order_creating( write_hdfs=True)
        date_creating(write_hdfs=True)
        sort_client_promotions_processing(write_hdfs=True)
        motor_processing(write_hdfs=True)
        tax_rate_processing(write_hdfs=True)
        pdv_processing(write_hdfs=True)
        family_processing(write_hdfs=True)
        channel_type_processing(write_hdfs=True)
        goal__processing(write_hdfs=True)
        samarex_processing(write_hdfs=True)
        vinpromo_processing(write_hdfs=True)
        promo_processing(write_hdfs=True)
        opv_processing(write_hdfs=True)
        mxusm_processing(write_hdfs=True)

        log("info", "End of --- REFINED ZONE --- Data Processes")