from pyspark.sql import functions as F
from pyspark.sql.types import *
from commonFunctions import load_config, copy_tables, create_hdfs_folder, log, timestamp, connect_db, hash_vin, write_oracle, read_hdfs_file


def to_exadata(session, data_of, fromDate, toDate, country=None, brand=None, isHistorical=None):
    log("info", "Beginning of --- EXADATA ZONE --- Data Processes")
    log("info", "   Table: " + str(data_of))
    log("info", "   fromDate: " + str(fromDate))
    log("info", "   toDate: " + str(toDate))
    log("info", "   country: " + str(country))
    log("info", "   brand: " + str(brand))

    def opv_write_exadata(write_exadata=True):
        log("info", "Starting OPV writing in Oracle.")
        # Read the configurations
        conf = load_config("opv")
        dfOPV = read_hdfs_file(session, conf['HdfsFileRefinedPath'])
        dfOPV = hash_vin(dfOPV, conf['ColumnToHash'])
        dfCountriesInserted = dfOPV.select("country")
        dfCountriesInserted.drop_duplicates()
        dfCountriesInserted.show()
        dfOPV.show()
        if write_exadata:
            write_oracle(dfOPV, connect_db(), conf['OPV_table'])
        log("info", "OPV has been written in Oracle.")

    def mxusm_write_exadata(write_exadata=True):
        log("info", "Starting OPV writing in Oracle.")
        # Read the configurations
        conf = load_config("mxusm")
        dfMXUSM = read_hdfs_file(session, conf['HdfsFileRefinedPath'])
        dfMXUSM = hash_vin(dfMXUSM, conf['ColumnToHash'])
        if write_exadata:
            write_oracle(dfMXUSM, connect_db(), conf['MXUSM_table'])
        log("info", "MXUSM has been written in Oracle.")

    def family_write_exadata(write_exadata=True):
        log("info", "Starting OPV writing in Oracle.")
        # Read the configurations
        conf = load_config("family")
        dfFAMILY = read_hdfs_file(session, conf['HdfsFileRefinedPath'])
        if write_exadata:
            write_oracle(dfFAMILY, connect_db(), conf['FAMILY_table'])
        log("info", "FAMILY has been written in Oracle.")

    def goal_write_exadata(write_exadata=True):
        log("info", "Starting OPV writing in Oracle.")
        # Read the configurations
        conf = load_config("goal")
        dfGOAL = read_hdfs_file(session, conf['HdfsFileRefinedPath'])
        if write_exadata:
            write_oracle(dfGOAL, connect_db(), conf['GOAL_table'])
        log("info", "GOAL has been written in Oracle.")

    def pdv_write_exadata(write_exadata=True):
        log("info", "Starting OPV writing in Oracle.")
        # Read the configurations
        conf = load_config("pdv")
        dfPDV = read_hdfs_file(session, conf['HdfsFileRefinedPath'])
        if write_exadata:
            write_oracle(dfPDV, connect_db(), conf['PDV_table'])
        log("info", "PDV has been written in Oracle.")

    def promo_write_exadata(write_exadata=True):
        log("info", "Starting OPV writing in Oracle.")
        # Read the configurations
        conf = load_config("promo")
        dfPROMO = read_hdfs_file(session, conf['HdfsFileRefinedPath'])
        if write_exadata:
            write_oracle(dfPROMO, connect_db(), conf['PROMO_table'])
        log("info", "PROMO has been written in Oracle.")

    def samarex_write_exadata(write_exadata=True):
        log("info", "Starting OPV writing in Oracle.")
        # Read the configurations
        conf = load_config("samarex")
        dfSAMAREX = read_hdfs_file(session, conf['HdfsFileRefinedPath'])
        if write_exadata:
            write_oracle(dfSAMAREX, connect_db(), conf['SAMAREX_table'])
        log("info", "SAMAREX has been written in Oracle.")

    def sort_client_promotions_write_exadata(write_exadata=True):
        log("info", "Starting OPV writing in Oracle.")
        # Read the configurations
        conf = load_config("sort_client_promotions")
        dfSORT = read_hdfs_file(session, conf['HdfsFileRefinedPath'])
        if write_exadata:
            write_oracle(dfSORT, connect_db(), conf['SORT_CLIENT_PROMOTIONS_table'])
        log("info", "SORT CLIENT PROMOTIONS has been written in Oracle.")

    def tax_rate_write_exadata(write_exadata=True):
        log("info", "Starting OPV writing in Oracle.")
        # Read the configurations
        conf = load_config("tax_rate")
        dfTAX = read_hdfs_file(session, conf['HdfsFileRefinedPath'])
        if write_exadata:
            write_oracle(dfTAX, connect_db(), conf['TAX_RATE_table'])
        log("info", "TAX RATE has been written in Oracle.")

    def channel_type_write_exadata(write_exadata=True):
        log("info", "Starting OPV writing in Oracle.")
        # Read the configurations
        conf = load_config("channel_type")
        dfCHAN = read_hdfs_file(session, conf['HdfsFileRefinedPath'])
        if write_exadata:
            write_oracle(dfCHAN, connect_db(), conf['CHANNEL_TYPE_table'])
        log("info", "CHANNEL TYPE has been written in Oracle.")

    def motor_write_exadata(write_exadata=True):
        log("info", "Starting motor writing in Oracle.")
        # Read the configurations
        conf = load_config("motor")
        dfMOTOR = read_hdfs_file(session, conf['hdfsFileRefinedPath'])
        if write_exadata:
            write_oracle(dfMOTOR, connect_db(), conf['MOTOR_table'])
        log("info", "motor has been written in Oracle.")

    def date_write_exadata(write_exadata=True):
        log("info", "Starting date writing in Oracle.")
        # Read the configurations
        conf = load_config("date")
        dfDATE = read_hdfs_file(session, conf['hdfsFileRefinedPath'])
        if write_exadata:
            write_oracle(dfDATE, connect_db(), conf['DATE_table'])
        log("info", "date has been written in Oracle.")

    def date_order_write_exadata(write_exadata=True):
        log("info", "Starting date order writing in Oracle.")
        # Read the configurations
        conf = load_config("date_order")
        dfDATEORDER = read_hdfs_file(session, conf['hdfsFileRefinedPath'])
        if write_exadata:
            write_oracle(dfDATEORDER, connect_db(), conf['DATE_ORDER_table'])
        log("info", "date order has been written in Oracle.")

    def vinpromo_write_exadata(write_exadata=True):
        log("info", "Starting OPV writing in Oracle.")
        # Read the configurations
        conf = load_config("vinpromo")
        dfVINPROMO = read_hdfs_file(session, conf['hdfsFileRefinedPath'])
        if write_exadata:
            write_oracle(dfVINPROMO, connect_db(), conf['VINPROMO_table'])
        log("info", "VINPROMO has been written in Oracle.")

    if data_of == "opv":
        opv_write_exadata(write_exadata=True)
    if data_of == "mxusm":
        mxusm_write_exadata(write_exadata=True)
    if data_of == "family":
        family_write_exadata(write_exadata=True)
    if data_of == "promo":
        promo_write_exadata(write_exadata=True)
    if data_of == "motor":
        motor_write_exadata(write_exadata=True)
    if data_of == "pdv":
        pdv_write_exadata(write_exadata=True)
    if data_of == "goal":
        goal_write_exadata(write_exadata=True)
    if data_of == "samarex":
        samarex_write_exadata(write_exadata=True)
    if data_of == "sort_client_promotions":
        sort_client_promotions_write_exadata(write_exadata=True)
    if data_of == "tax_rate":
        tax_rate_write_exadata(write_exadata=True)
    if data_of == "channel_type":
        channel_type_write_exadata(write_exadata=True)
    if data_of == "vinpromo":
        vinpromo_write_exadata(write_exadata=True)
    if data_of == "date":
       date_write_exadata(write_exadata=True)
    if data_of == "date_order":
        date_order_write_exadata(write_exadata=True)
    elif data_of == "all":
        date_write_exadata(write_exadata=True)
        date_order_write_exadata(write_exadata=True)
        opv_write_exadata(write_exadata=True)
        mxusm_write_exadata(write_exadata=True)
        family_write_exadata(write_exadata=True)
        promo_write_exadata(write_exadata=True)
        motor_write_exadata(write_exadata=True)
        pdv_write_exadata(write_exadata=True)
        goal_write_exadata(write_exadata=True)
        samarex_write_exadata(write_exadata=True)
        sort_client_promotions_write_exadata(write_exadata=True)
        tax_rate_write_exadata(write_exadata=True)
        channel_type_write_exadata(write_exadata=True)
        vinpromo_write_exadata(write_exadata=True)


    log("info", "End of --- EXADATA ZONE --- Data Processes")
