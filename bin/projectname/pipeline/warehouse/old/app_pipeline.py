"""Example pipeline.

Several actions are performed:
- Load DataConfig (information from the configuration file located in conf/data/perimeter.json)
- Create a spark session
- Read data from hdfs according to the perimeter
- Write data to hdfs

"""

import os
import logging
import datetime
from pyspark.sql import functions as F

from projecttemplate.configuration import spark_config
from projecttemplate.configuration.app import AppConfig
from projecttemplate.configuration.data import DataConfig

from projecttemplate.domain import kpis


def run(filepath_prod_flow="/user/brc05/data/refined/manf001_vehc_prdc_flow/year=2020/month=01"):
    """Run the application pipeline."""
    # Configs
    AppConfig()
    data_config = DataConfig()

    sites = data_config.vhls_perimeter["sites"]
    start_date = datetime.datetime.strptime(data_config.vhls_perimeter["start_date"], "%d/%m/%y")
    end_date = datetime.datetime.strptime(data_config.vhls_perimeter["end_date"], "%d/%m/%y")
    genr_door = data_config.vhls_perimeter["genr_door"]

    # Create a spark session
    _, spark_session = spark_config.get_spark(app_name="app-template", executors=2,
                                              executor_cores=4, executor_mem="4g",
                                              dynamic_allocation=True, max_executors=8)

    logging.info("===========================================================")
    logging.info("Starting application pipeline")

    # Read data from hdfs according to the perimeter
    logging.info(f"Reading data from {filepath_prod_flow}")
    df_flow = spark_session.read.parquet(filepath_prod_flow)
    df_flow = df_flow.filter(df_flow["site_code"].isin(sites)) \
                     .filter(df_flow["pass_date"] >= start_date) \
                     .filter(df_flow["pass_date"] < end_date) \
                     .filter(df_flow["genr_door"] == genr_door)

    # Compute number of vins per site
    vins_count = kpis.compute_n_vins_per_site(df_flow, "site_code")
    vins_count = {row['site_code']: row['count'] for row in vins_count}
    logging.info(f"Number of vehicles per site: {vins_count}")

    # Write only PY data to hdfs (partition by date)
    output_dirpath = '/' + '/'.join(os.environ['DATA'].split('/')[2:])
    logging.info(f"Writing data to {output_dirpath}")
    df_flow.filter(F.col("site_code") == "PY") \
           .withColumn("year", F.year(F.col("pass_date"))) \
           .withColumn("month", F.month(F.col("pass_date"))) \
           .withColumn("day", F.dayofmonth(F.col("pass_date"))) \
           .coalesce(1).write.partitionBy("year", "month", "day") \
           .parquet(output_dirpath, "overwrite")

    logging.info("*** Application pipeline finished ***")


if __name__ == "__main__":
    run()
