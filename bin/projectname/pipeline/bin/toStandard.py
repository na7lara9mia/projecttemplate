from pyspark.sql import functions as F
from pyspark.sql.types import *
from itertools import chain
from commonFunctions import load_config, copy_tables, create_hdfs_folder, log, timestamp, write_to_hdfs, stop_session,mapping_ctry, countries


def to_standard(session, data_of, fromDate, toDate, country=None, brand=None, isHistorical=None):
    log("info", "Beginning of --- STANDARD ZONE --- Data Processes")
    log("info", "   table: " + str(data_of))
    log("info", "   fromDate: " + str(fromDate))
    log("info", "   toDate: " + str(toDate))
    log("info", "   country: " + str(country))
    log("info", "   brand: " + str(brand))

    def madax_construct(write_hdfs=True):
        log("info", "Madax construction at run.")

        # Read the configurations
        conf = load_config("madax")

        # Loading the conf from HDFS
        df_rbvqtcdc = session.read.load(conf["hdfsFileRawPath"] + conf["cdc"])
        df_rbvqttxm_arc = session.read.load(conf["hdfsFileRawPath"] + conf["txm_arc"])
        df_rbvqtfll = session.read.load(conf["hdfsFileRawPath"] + conf["fll"])
        df_rbvqttff = session.read.load(conf["hdfsFileRawPath"] + conf["tff"])
        df_rbvqtlm1 = session.read.load(conf["hdfsFileRawPath"] + conf["lm1"])
        df_rbvqtveh = session.read.load(conf["hdfsFileRawPath"] + conf["veh"])
        df_rbvqttut = session.read.load(conf["hdfsFileRawPath"] + conf["tut"])
        df_rbvqtcco = session.read.load(conf["hdfsFileRawPath"] + conf["cco"])
        df_rbvqtcaf = session.read.load(conf["hdfsFileRawPath"] + conf["caf"])
        df_rbvqtm10 = session.read.load(conf["hdfsFileRawPath"] + conf["m10"])
        df_rbvqtctp = session.read.load(conf["hdfsFileRawPath"] + conf["ctp"])
        df_rbvqtfam = session.read.load(conf["hdfsFileRawPath"] + conf["fam"])

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
        queryMADAX = open(query, mode='r', encoding='utf-8-sig').read().format(fromDate, toDate, country, brand)

        # Reading the data using the query above
        dfMADAX = session.sql(queryMADAX) \
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

        dfMADAX = dfMADAX \
            .replace(to_replace=column_name_VpVu, subset=['VPVU']) \
            .withColumn('REMIPOUR', F.when(F.col('REMIPOUR') == '0', F.lit(None)).otherwise(F.col('REMIPOUR'))) \
            .withColumn('CODPROM', F.when(F.col('CODPROM') == '0', F.lit(None)).otherwise(F.col('CODPROM')))

        for i in range(len(conf['ColumnToRename']['old'])):
            dfMADAX = dfMADAX.withColumnRenamed(conf['ColumnToRename']['old'][i],
                                                  conf['ColumnToRename']['new'][i])

        # Writing the result in HDFS under partitions
        if write_hdfs:
            write_to_hdfs(dfMADAX, conf["hdfsFileStandardPath"], ("COUNTRY", "BRAND") )

        log("info", "Madax construction is finished.")
    def samara_construct(write_hdfs=True):
        log("info", "Samara construction at run.")

        # Read the configurations
        conf = load_config("samara")

        # Loading the conf from HDFS
        df_sinqtvin = session.read.load(conf["hdfsFileRawPath"] + conf["vin"])
        df_sinqtcli = session.read.load(conf["hdfsFileRawPath"] + conf["cli"])
        df_sinqtver = session.read.load(conf["hdfsFileRawPath"] + conf["ver"])
        df_sinqtfv4 = session.read.load(conf["hdfsFileRawPath"] + conf["fv4"])
        df_sinqtcmp = session.read.load(conf["hdfsFileRawPath"] + conf["cmp"])
        df_sinqtcnd = session.read.load(conf["hdfsFileRawPath"] + conf["cnd"])
        df_sinqtseg = session.read.load(conf["hdfsFileRawPath"] + conf["seg"])
        df_sinqtzds = session.read.load(conf["hdfsFileRawPath"] + conf["zds"])
        df_sinqtsfa = session.read.load(conf["hdfsFileRawPath"] + conf["sfa"])
        df_sinqtfam = session.read.load(conf["hdfsFileRawPath"] + conf["fam"])
        df_sinqtrub = session.read.load(conf["hdfsFileRawPath"] + conf["rub"])
        df_sinqtopc = session.read.load(conf["hdfsFileRawPath"] + conf["opc"])
        df_sinqtcma = session.read.load(conf["hdfsFileRawPath"] + conf["cma"])
        df_sinqtcmi = session.read.load(conf["hdfsFileRawPath"] + conf["cmi"])
        df_sinqtcyr = session.read.load(conf["hdfsFileRawPath"] + conf["cyr"])
        df_sinqtmrq = session.read.load(conf["hdfsFileRawPath"] + conf["mrq"])
        df_sinqtbas = session.read.load(conf["hdfsFileRawPath"] + conf["bas"])

        # Creating the temporary views
        df_sinqtvin.createOrReplaceTempView("df_sinqtvin")
        df_sinqtcli.createOrReplaceTempView("df_sinqtcli")
        df_sinqtver.createOrReplaceTempView("df_sinqtver")
        df_sinqtfv4.createOrReplaceTempView("df_sinqtfv4")
        df_sinqtcmp.createOrReplaceTempView("df_sinqtcmp")
        df_sinqtcnd.createOrReplaceTempView("df_sinqtcnd")
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

        if (country is not None) & (brand is not None):
            query = conf["samaraSQLPathP"]
        else:
            query = conf["samaraSQLPath"]

        # Reading the SQL query from file
        querySAMARA = open(query, mode='r', encoding='utf-8-sig').read().format(fromDate, toDate, country, brand)

        dfSAMARA = session.sql(querySAMARA) \
            .withColumn("year", F.year(F.col("DT_VD"))) \
            .withColumn("month", F.month(F.col("DT_VD"))) \
            .withColumn("day", F.dayofmonth(F.col("DT_VD")))

        for i in range(len(conf['ColumnToRename']['old'])):
            dfSAMARA = dfSAMARA.withColumnRenamed(conf['ColumnToRename']['old'][i], conf['ColumnToRename']['new'][i])

        dfSAMARA=dfSAMARA \
            .withColumn('COUNTRY', F.substring(F.col("COUNTRY"), 3, 2)) \
            .withColumn("CANAL_TYPE", F.concat(F.concat(F.concat(F.concat(
            F.concat(F.concat(F.concat(F.concat(F.col('COUNTRY'), F.lit(" ")), F.col('BRAND'), F.lit(" "))),
                     F.col('CODE_CND')), F.lit(' ')), F.col('CODE_CMA')), F.lit(' ')), F.col('CODE_CMI'))) \
            .withColumn("FIX_MARGIN",
                        F.concat(F.concat(F.concat(F.concat(F.col('year'), F.lit(' ')), F.col('COUNTRY')), F.lit(' ')),
                                 F.col('CODE_VER'))) \
            .replace(to_replace=mapping_ctry, subset=['COUNTRY'])

        #removing lines that are not direct sales
        dfDirectSales = session.read.parquet(conf['dfDirectSales'])
        column_values_channel = {row['KEY_CANAL']: row['TYPE_CANAL'] for row in dfDirectSales.collect()}
        mapping_expr = F.create_map([F.lit(x) for x in chain(*column_values_channel.items())])
        dfSAMARA = dfSAMARA.withColumn('CANAL_TYPE', mapping_expr[dfSAMARA['CANAL_TYPE']])
        dfSAMARA = dfSAMARA.filter(F.col('CANAL_TYPE') != 'Other')

        # Writing the result in HDFS under partitions
        if write_hdfs:
            write_to_hdfs(dfSAMARA, conf["hdfsFileStandardPath"], ("COUNTRY", "BRAND"))

        log("info", "Samara construction is finished.")
    def opv_construct(write_hdfs=True):
        log("info", "OPV construction starting.")
        # Read the configurations
        conf = load_config("opv")
        # get all files starting with opv_FR
        fs = session._jvm.org.apache.hadoop.fs.FileSystem.get(session._jsc.hadoopConfiguration())
        list_status = fs.listStatus(session._jvm.org.apache.hadoop.fs.Path(conf['HdfsFileRawPath']))
        result = [file.getPath().getName() for file in list_status]
        listdf = []

        if country is None:
            for c in countries:
                extracts_OPV = [s for s in result if s.startswith('opv_{0}'.format(str(c)))]
                # homogenize all extracts and select a few columns predifined
                for f in extracts_OPV:
                    df = session.read.csv(conf['HdfsFileRawPath'] + f, sep=';', header=True,encoding='iso-8859-1')
                    df = df.withColumn("COUNTRY", F.lit(c))
                    df = df.select(conf['ColumnToSelect'][c])
                    for i in range(len(conf['ColumnToRename'][c]['old'])):
                        df = df.withColumnRenamed(conf['ColumnToRename'][c]['old'][i],conf['ColumnToRename'][c]['new'][i])
                    for j in range(len(conf['ColumnToCreate'][c]["column"])):
                        df = df.withColumn(conf['ColumnToCreate'][c]['column'][j],F.lit(None).cast(conf['ColumnToCreate'][c]['type'][j]))
                    df = df.select(conf['ColumnToSelectFinale'])
                    listdf.append(df)

        else:
            extracts_OPV = [s for s in result if s.startswith('opv_{0}'.format(str(country)))]
            # homogenize all extracts and select a few columns predifined
            for f in extracts_OPV:
                df = session.read.csv(conf['HdfsFileRawPath'] + f, sep=';', header=True,encoding='iso-8859-1')
                df = df.withColumn("COUNTRY", F.lit(country))
                df = df.select(conf['ColumnToSelect'][country])
                for i in range(len(conf['ColumnToRename'][country]['old'])):
                    df = df.withColumnRenamed(conf['ColumnToRename'][country]['old'][i], conf['ColumnToRename'][country]['new'][i])
                for j in range(len(conf['ColumnToCreate'][country]["column"])):
                    df = df.withColumn(conf['ColumnToCreate'][country]['column'][j],F.lit(None).cast(conf['ColumnToCreate'][country]['type'][j]))
                df = df.select(conf['ColumnToSelectFinale'])
                listdf.append(df)

        dfOPV = listdf[0]
        if len(listdf) > 1:
            for i in range(0, len(listdf) - 1):
                dfOPV = dfOPV.union(listdf[i + 1])

        if brand is not None:
            dfOPV = dfOPV.filter(F.col('BRAND') == brand)

        dfOPV = dfOPV.replace(to_replace=mapping_ctry, subset=['COUNTRY'])

        #dfOPV.select('COUNTRY','BRAND','VIN','FINITION').filter(~(F.col('VIN')=='                 ')).show()

        dfOPV = (dfOPV.withColumn("year", F.year(F.col("DATE_COMANDE"))).withColumn("month", F.month(F.col("DATE_COMANDE"))))
        dfCountriesInserted = dfOPV.select("COUNTRY")
        dfCountriesInserted.drop_duplicates()
        dfCountriesInserted.show()
        if write_hdfs:
            write_to_hdfs(dfOPV,  conf["HdfsFileStandardPath"],("COUNTRY", "BRAND"))

        log("info", "OPV construction is finished.")


    def samarex_construct(write_hdfs=True):
        log("info", "Samarex construction begins.")
        # Read the configurations
        conf = load_config("samarex")
        # get all files starting with Samarex_
        fs = session._jvm.org.apache.hadoop.fs.FileSystem.get(session._jsc.hadoopConfiguration())
        list_status = fs.listStatus(session._jvm.org.apache.hadoop.fs.Path(conf['HdfsFileRawPath']))
        result = [file.getPath().getName() for file in list_status]
        listdf = []
        extracts_SAMAREX = [s for s in result if s.startswith('Samarex_')]
        # homogenize all extracts and select a few columns predifined
        for f in extracts_SAMAREX:
            df = session.read \
                .option("encoding", 'iso-8859-1') \
                .option("header", "true") \
                .option("escape", '\r') \
                .option('multiLine', True) \
                .option("sep", ";").format('csv').load(conf['HdfsFileRawPath'] + f)
            df = df.select(*(F.col("`" + c + "`").alias(c.replace(' ', '_')) for c in df.columns))
            df = df.select(*(F.col("`" + c + "`").alias(c.replace('\r', '')) for c in df.columns))
            df = df.select(conf['ColumnToSelect'])
            for i in range(len(conf['ColumnToRename']['old'])):
                df = df.withColumnRenamed(conf['ColumnToRename']['old'][i],
                                          conf['ColumnToRename']['new'][i])
            listdf.append(df)
        dfSAMAREX = listdf[0]
        if len(listdf) > 1:
            for i in range(0, len(listdf) - 1):
                dfSAMAREX = dfSAMAREX.union(listdf[i + 1])

        dfSAMAREX = dfSAMAREX.replace(to_replace=conf['mapping_ctry'], subset=['COUNTRY'])

        if country is not None:
            dfSAMAREX = dfSAMAREX.filter(F.col('COUNTRY') == mapping_ctry[country])

        if brand is not None:
            dfSAMAREX = dfSAMAREX.filter(F.col('BRAND') == brand)

        if write_hdfs:
            write_to_hdfs(dfSAMAREX, conf["HdfsFileStandardPath"],('COUNTRY','BRAND'))
        log("info", "Samarex construction is finished.")


    def promo_construct(write_hdfs=True):
        log("info", "Promo construction at run.")

        # Read the configurations
        conf = load_config("promo")

        # get all files starting with Promo_
        fs = session._jvm.org.apache.hadoop.fs.FileSystem.get(session._jsc.hadoopConfiguration())
        list_status = fs.listStatus(session._jvm.org.apache.hadoop.fs.Path(conf['HdfsFileRawPath']))
        result = [file.getPath().getName() for file in list_status]
        listdf = []

        if country is None:
            for c in countries:
                extracts_PROMO = [s for s in result if s.startswith('Promo_{0}'.format(str(c)))]
                # homogenize all extracts and select a few columns predifined

                for f in extracts_PROMO:
                    df = session.read.csv(conf['HdfsFileRawPath'] + f, sep=';', header=True, encoding='iso-8859-1')
                    df = df.withColumn("COUNTRY", F.lit(c))
                    df = df.select(conf['ColumnToSelect'])
                    for i in range(len(conf['ColumnToRename']['old'])):
                        df = df.withColumnRenamed(conf['ColumnToRename']['old'][i],
                                                  conf['ColumnToRename']['new'][i])
                    listdf.append(df)

        else:
            extracts_PROMO = [s for s in result if s.startswith('Promo_{0}'.format(str(country)))]
            # homogenize all extracts and select a few columns predifined

            for f in extracts_PROMO:
                df = session.read.csv(conf['HdfsFileRawPath'] + f, sep=';', header=True, encoding='iso-8859-1')
                df = df.withColumn("COUNTRY", F.lit(country))
                df = df.select(conf['ColumnToSelect'])
                for i in range(len(conf['ColumnToRename']['old'])):
                    df = df.withColumnRenamed(conf['ColumnToRename']['old'][i],
                                              conf['ColumnToRename']['new'][i])
                listdf.append(df)
        dfPROMO = listdf[0]
        if len(listdf) > 1:
            for i in range(0, len(listdf) - 1):
                dfPROMO = dfPROMO.union(listdf[i + 1])
        if brand is not None:
            dfPROMO = dfPROMO.filter(F.col('BRAND') == brand)

        dfPROMO = dfPROMO.replace(to_replace=mapping_ctry, subset=['COUNTRY'])
        dfPROMO.show()
        # Writing the result in HDFS under partitions
        if write_hdfs:
            write_to_hdfs(dfPROMO,  conf["HdfsFileStandardPath"],("COUNTRY" ,"BRAND"))
        log("info", "Promo construction is finished.")


    def goal_copy(write_hdfs=True):
        log("info", "Goal copy")

        # Read the configurations
        conf = load_config("goal")
        dfGOAL = session.read.csv(conf["HdfsFileRawPath"], sep=';', header=True,encoding='iso-8859-1')
        for c in dfGOAL.columns:
            dfGOAL = dfGOAL.withColumnRenamed(c, c.strip())
        dfGOAL = dfGOAL.select(conf['ColumnToSelect'])
        for i in range(len(conf['ColumnToRename']['old'])):
            dfGOAL = dfGOAL.withColumnRenamed(conf['ColumnToRename']['old'][i],
                                      conf['ColumnToRename']['new'][i])
        dfGOAL = dfGOAL.replace(to_replace=conf['mapping_ctry'], subset=['COUNTRY'])
        dfGOAL = dfGOAL.replace(to_replace=conf['mapping_brd'], subset=['BRAND'])

        if country is not None:
            dfGOAL = dfGOAL.filter(F.col('COUNTRY') == mapping_ctry[country])
        if brand is not None:
            dfGOAL = dfGOAL.filter(F.col('BRAND') == brand)

        if write_hdfs:
            write_to_hdfs(dfGOAL,  conf["HdfsFileStandardPath"],("COUNTRY" ,"BRAND"))
        log("info", "goal is copied.")


    def pdv_copy(write_hdfs=True):
        log("info", "pdv copy")

        # Read the configurations
        conf = load_config("pdv")
        dfPDV = session.read.csv(conf['HdfsFileRawPath'], sep=';', header=True,encoding='iso-8859-1')
        dfPDV = dfPDV.select(conf['ColumnToSelect'])
        for i in range(len(conf['ColumnToRename']['old'])):
            dfPDV = dfPDV.withColumnRenamed(conf['ColumnToRename']['old'][i],
                                      conf['ColumnToRename']['new'][i])
        # map country column
        dfPDV = dfPDV.replace(to_replace=conf['mapping_ctry'], subset=['COUNTRY'])

        if country is not None:
            dfPDV = dfPDV.filter(F.col('COUNTRY') == mapping_ctry[country])
        if brand is not None:
            dfPDV = dfPDV.filter(F.col('BRAND') == brand)

        if write_hdfs:
            write_to_hdfs(dfPDV,  conf["HdfsFileStandardPath"],("COUNTRY", "BRAND"))

        log("info", "pdv is copied.")


    def tax_rate_copy(write_hdfs=True):
        log("info", "tax_rate copy")

        # Read the configurations
        conf = load_config("tax_rate")
        dfTAX = session.read.csv(conf['HdfsFileRawPath'], sep=';', header=True,encoding='iso-8859-1')
        dfTAX = dfTAX.select(conf['ColumnToSelect'])
        for i in range(len(conf['ColumnToRename']['old'])):
            dfTAX = dfTAX.withColumnRenamed(conf['ColumnToRename']['old'][i],
                                      conf['ColumnToRename']['new'][i])
        if country is not None:
            dfTAX = dfTAX.filter(F.col('COUNTRY') == mapping_ctry[country])
        dfTAX.show()
        if write_hdfs:
            write_to_hdfs(dfTAX,conf["HdfsFileStandardPath"],["COUNTRY"])

        log("info", "tax_rate is copied.")


    def family_copy(write_hdfs=True):
        log("info", "Family copy")
        # Read the configurations
        conf = load_config("family")
        dfFAMILY = session.read.csv(conf['HdfsFileRawPath'], sep=';', header=True,encoding='iso-8859-1')
        dfFAMILY = dfFAMILY.select(conf['ColumnToSelect'])
        for i in range(len(conf['ColumnToRename']['old'])):
            dfFAMILY = dfFAMILY.withColumnRenamed(conf['ColumnToRename']['old'][i],
                                      conf['ColumnToRename']['new'][i])
        if brand is not None:
            dfFAMILY = dfFAMILY.filter(F.col('BRAND') == brand)

        if write_hdfs:
            write_to_hdfs(dfFAMILY, conf["HdfsFileStandardPath"],["BRAND"])
        log("info", "family is copied.")


    def sort_client_promotions_copy(write_hdfs=True):
        log("info", "Sort_Client_Promotions copy")

        # Read the configurations
        conf = load_config("sort_client_promotions")
        dfSRT = session.read.csv(conf['HdfsFileRawPath'], sep=';', header=True,encoding='iso-8859-1')
        dfSRT = dfSRT.select(conf['ColumnToSelect'])
        for i in range(len(conf['ColumnToRename']['old'])):
            dfSRT = dfSRT.withColumnRenamed(conf['ColumnToRename']['old'][i],
                                      conf['ColumnToRename']['new'][i])
        dfSRT.show()
        # copy the file into Standard Zone
        if write_hdfs:
            write_to_hdfs(dfSRT, conf["HdfsFileStandardPath"])


        log("info", "sort_client_promotions is copied.")


    def channel_type_copy(write_hdfs=True):
        log("info", "channel_Type copy")
        # Read the configurations
        conf = load_config("channel_type")
        dfCHAN = session.read.csv(conf['HdfsFileRawPath'], sep=';', header=True,encoding='iso-8859-1')
        dfCHAN = dfCHAN.select(conf['ColumnToSelect'])
        for i in range(len(conf['ColumnToRename']['old'])):
            dfCHAN = dfCHAN.withColumnRenamed(conf['ColumnToRename']['old'][i],
                                      conf['ColumnToRename']['new'][i])
        dfCHAN.show()

        #copy the file into Standard Zone
        if write_hdfs:
            write_to_hdfs(dfCHAN, conf["HdfsFileStandardPath"])
        log("info", "channel_type is copied.")


    def ventes_directes_indirectes_copy(write_hdfs=True):
        log("info", "ventes_directes_indirectes copy")
        # Read the configurations
        conf = load_config("ventes_directes_indirectes")
        df = session.read.csv(conf['HdfsFileRawPath'], sep=';', header=True, encoding='iso-8859-1')
        df = df.select(conf['ColumnToSelect'])
        for i in range(len(conf['ColumnToRename']['old'])):
            df = df.withColumnRenamed(conf['ColumnToRename']['old'][i],
                                          conf['ColumnToRename']['new'][i])
        df.show()
        # copy the file into Standard Zone
        if write_hdfs:
           write_to_hdfs(df, conf["HdfsFileStandardPath"])

        log("info", "ventes_directes_indirectes is copied.")

    def table_ref_marges_copy(write_hdfs=True):
        log("info", "table_ref_marges copy")
        # Read the configurations
        conf = load_config("Table_ref_marges")
        df = session.read.csv(conf['HdfsFileRawPath'], sep=';', header=True, encoding='iso-8859-1')
        df = df.select(conf['ColumnToSelect'])
        for i in range(len(conf['ColumnToRename']['old'])):
            df = df.withColumnRenamed(conf['ColumnToRename']['old'][i],
                                          conf['ColumnToRename']['new'][i])
        df.show()
        # copy the file into Standard Zone
        if write_hdfs:
           write_to_hdfs(df, conf["HdfsFileStandardPath"])

        log("info", "table_ref_marges is copied.")

    def classification_vme_category_copy(write_hdfs=True):
        log("info", "classification_vme_category copy")
        # Read the configurations
        conf = load_config("Classification_VME_Category")
        df = session.read.csv(conf['HdfsFileRawPath'], sep=';', header=True, encoding='iso-8859-1')
        df = df.select(conf['ColumnToSelect'])
        for i in range(len(conf['ColumnToRename']['old'])):
            df = df.withColumnRenamed(conf['ColumnToRename']['old'][i],
                                          conf['ColumnToRename']['new'][i])
        df.show()
        # copy the file into Standard Zone
        if write_hdfs:
           write_to_hdfs(df, conf["HdfsFileStandardPath"])

        log("info", "classification_vme_category is copied.")

    def motor_construct(write_hdfs=True):
        log("info", "Motor construction at run.")

        # Read the configurations
        conf = load_config("motor")

        # Loading the conf from HDFS
        df_rbvqttff = session.read.load(conf["hdfsFileRawPath"] + conf["tff"])
        df_rbvqtlm1 = session.read.load(conf["hdfsFileRawPath"] + conf["lm1"])

        # Creating the temporary views
        df_rbvqttff.createOrReplaceTempView("df_rbvqttff")
        df_rbvqtlm1.createOrReplaceTempView("df_rbvqtlm1")
        ctry= country
        if (country is not None) & (brand is not None):
            query = conf["motorSQLPathP"]
            ctry = mapping_ctry[ctry]
        else:
            query = conf["motorSQLPath"]

        # Reading the SQL query from file
        queryMOTOR= open(query, mode='r', encoding='utf-8-sig').read().format(fromDate, toDate, ctry , brand)

        dfMOTOR = session.sql(queryMOTOR)\
            .drop_duplicates()
        for i in range(len(conf['ColumnToRename']['old'])):
            dfMOTOR = dfMOTOR.withColumnRenamed(conf['ColumnToRename']['old'][i],conf['ColumnToRename']['new'][i])

        dfMOTOR = dfMOTOR.replace(to_replace=mapping_ctry, subset=['COUNTRY'])

        # copy the file into Standard Zone
        if write_hdfs:
            write_to_hdfs(dfMOTOR, conf["hdfsFileStandardPath"], ['COUNTRY'])

        log("info", "motor is copied.")

    if data_of == "mxusm":
        madax_construct(write_hdfs=True)
        samara_construct(write_hdfs=True)
        opv_construct(write_hdfs=True)
    if data_of == "vinpromo":
        samara_construct(write_hdfs=True)
    if data_of == "madax":
        madax_construct(write_hdfs=True)
    elif data_of == "samara":
        table_ref_marges_copy(write_hdfs=True)
        ventes_directes_indirectes_copy(write_hdfs=True)
        samara_construct(write_hdfs=True)
    elif data_of == "opv":
        opv_construct(write_hdfs=True)
    elif data_of == "samarex":
        samarex_construct(write_hdfs=True)
    elif data_of == "promo":
        classification_vme_category_copy(write_hdfs=True)
        promo_construct(write_hdfs=True)
    elif data_of == "channel_type":
        channel_type_copy(write_hdfs=True)
    elif data_of == "family":
        family_copy(write_hdfs=True)
    elif data_of == "pdv":
        pdv_copy(write_hdfs=True)
    elif data_of == "tax_rate":
        tax_rate_copy(write_hdfs=True)
    elif data_of == "motor":
        motor_construct(write_hdfs=True)
    elif data_of == "sort_client_promotions":
        sort_client_promotions_copy(write_hdfs=True)
    elif data_of == "goal":
        goal_copy(write_hdfs=True)

    elif data_of == "all":
        classification_vme_category_copy(write_hdfs=True)
        ventes_directes_indirectes_copy(write_hdfs=True)
        channel_type_copy(write_hdfs=True)
        family_copy(write_hdfs=True)
        tax_rate_copy(write_hdfs=True)
        sort_client_promotions_copy(write_hdfs=True)
        motor_construct(write_hdfs=True)
        madax_construct(write_hdfs=True)
        table_ref_marges_copy(write_hdfs=True)
        samara_construct(write_hdfs=True)
        opv_construct(write_hdfs=True)
        samarex_construct(write_hdfs=True)
        promo_construct(write_hdfs=True)
        pdv_copy(write_hdfs=True)
        goal_copy(write_hdfs=True)

    log("info", "End of --- STANDARD ZONE --- Data Processes")


