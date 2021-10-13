# Imports
from datetime import datetime, timedelta
from argparse import ArgumentParser
from pyspark.sql.types import *
from projecttemplate.configuration import spark_config
from toRaw import to_raw
from toStandard import to_standard
from toRefined import to_refined
from toExadata import to_exadata
from commonFunctions import log, stop_session

zone_functions = [to_raw, to_standard, to_refined, to_exadata]
zones = {"Source": 0, "Raw": 1, "Standard": 2, "Refined": 3, "Exadata": 4}

# Creating the spark session
spark_context, spark_session = spark_config.get_spark(app_name="vmeca-main",
                                                      executors=3, executor_cores=5, executor_mem='8g',
                                                      dynamic_allocation=True, max_executors=8)

spark_session.conf.set("spark.sql.crossJoin.enabled", "true")

if __name__ == '__main__':

    parser = ArgumentParser()
    parser.add_argument("--table", type=str, default=None, required=False, choices=['all', 'date_order', 'date','motor','madax', 'samara', 'opv','goal', 'pdv', 'promo','samarex','mxusm','family','channel_type','vinpromo','tax_rate','sort_client_promotions'], help="Table name (samara or madax or all")
    parser.add_argument("--fromDate", type=str, default=None, required=False, help="Start Date (dd/MM/yyyy)")
    parser.add_argument("--toDate", type=str, default=None, required=False, help="End Date (dd/MM/yyyy)")
    parser.add_argument("--country", type=str, default=None, required=False, help="BE,FR,IT,SP,GB,NL,PL,AT,PT,DE for Samara")
    parser.add_argument("--brand", type=str, default=None, required=False, help="AP, AC, DS")
    parser.add_argument("--fromZone", type=str, default="Source", required=False, choices=['Raw', 'Standard', 'Refined', 'Exadata'], help="Operation type (zone name)")
    parser.add_argument("--toZone", type=str, default=None, required=True, choices=['Raw', 'Standard', 'Refined', 'Exadata'], help="Operation type (zone name)")
    parser.add_argument("--historical", type=str, default="True", required=False, choices=['True', 'False'])

    args = parser.parse_args()

    table = args.table
    fromDate = args.fromDate
    toDate = args.toDate
    country = args.country
    brand = args.brand
    fromZone = args.fromZone
    toZone = args.toZone

    if (toDate and (fromDate is None)) or (fromDate and (toDate is None)) :
        parser.error("--toDate requires --fromDate and vice versa.")

    if fromDate is None and toDate is None:
        toDate = datetime.now().strftime("%d/%m/%Y")
        #warning / long term : have to change it to 30 months
        fromDate = (datetime.now() - timedelta(days=30)).strftime("%d/%m/%Y")

    for f in range(zones[fromZone], zones[toZone]):
        zone_functions[f](spark_session, table, fromDate, toDate, country, brand, isHistorical=args.historical)

    stop_session(spark_session)  # Stopping the Spark Session
