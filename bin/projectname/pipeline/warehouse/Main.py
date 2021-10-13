# Imports
from argparse import ArgumentParser
from pyspark.sql.types import *
from projecttemplate.configuration import spark_config
from Madax import madax_construct
from Samara import samara_construct
from datetime import datetime

# Creating the spark session
spark_context, spark_session = spark_config.get_spark(app_name="dco-main",
                                                      executors=3, executor_cores=5, executor_mem='8g',
                                                      dynamic_allocation=True, max_executors=8)

spark_session.conf.set("spark.sql.crossJoin.enabled", "true")


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--table", type=str, default=None, required=False, help="Operation type (samara or madax")
    parser.add_argument("--create", type=str, default="vin", required=False, help="samara table type (vin/vinpromo")
    parser.add_argument("--dateFrom", type=str, default=None, required=True, help="Start Date (dd/MM/yyyy)")
    parser.add_argument("--dateTo", type=str, default=None, required=True, help="End Date (dd/MM/yyyy)")
    parser.add_argument("--country", type=str, default=None, required=False, help="BE,FR,IT,ES,GB,NL,PL,AT,PT,DE for Samara")
    parser.add_argument("--brand", type=str, default=None, required=False, help="CP or PP for Samara")
    parser.add_argument("--writeHDFS", type=str, default="False", required=True, help="True or False")
    args = parser.parse_args()

    table = args.table
    dateFrom = args.dateFrom
    dateTo = args.dateTo
    writeHDFS = args.writeHDFS

    if (args.table is None) & (args.country is None) & (args.brand is None):
        # Samara process
        print(datetime.now().strftime("%y/%m/%d %H:%M:%S") + " INFO Samara process started.")
        # Samara (VIN) process
        print(datetime.now().strftime("%y/%m/%d %H:%M:%S") + " INFO Creating samara_" + "vin" + " table.")
        samara_construct(spark_session, "vin", dateFrom, dateTo, writeHDFS)
        # Samara (VINPROMO) process
        print(datetime.now().strftime("%y/%m/%d %H:%M:%S") + " INFO Creating samara_" + "vinpromo" + " table.")
        samara_construct(spark_session, "vinpromo", dateFrom, dateTo, writeHDFS)
        # Madax process
        print(datetime.now().strftime("%y/%m/%d %H:%M:%S") + " INFO Madax process started, creating Madax table.")
        madax_construct(spark_session, dateFrom, dateTo, writeHDFS)
        # Stopping the Spark Session
        spark_session.stop()
        print(datetime.now().strftime("%y/%m/%d %H:%M:%S") + " INFO Session successfully closed.")
    elif args.table == "samara":
        print(datetime.now().strftime("%y/%m/%d %H:%M:%S") + " INFO Samara process started.")
        create = args.create
        print(datetime.now().strftime("%y/%m/%d %H:%M:%S") + " INFO Creating samara_" + create + " table.")
        if (args.country is not None) | (args.brand is not None):
            country = args.country
            brand = args.brand
            samara_construct(spark_session, create, dateFrom, dateTo, writeHDFS, country, brand)
        else:
            samara_construct(spark_session, create, dateFrom, dateTo, writeHDFS)
    elif args.table == "madax":
        print(datetime.now().strftime("%y/%m/%d %H:%M:%S") + " INFO Madax process started, creating Madax table.")
        if (args.country is not None) & (args.brand is not None):
            country = args.country
            brand = args.brand
            madax_construct(spark_session, dateFrom, dateTo, writeHDFS, country, brand)
        else:
            madax_construct(spark_session, dateFrom, dateTo, writeHDFS)
    else:
        print(datetime.now().strftime("%y/%m/%d %H:%M:%S") + " ERROR Please provide a valid table name ('madax' or 'samara')")
        # Stopping the Spark Session
        spark_session.stop()
        print(datetime.now().strftime("%y/%m/%d %H:%M:%S") + " INFO Session successfully closed.")