import time
from pyspark.sql import functions as F
from pyspark.sql.types import *
from commonFunctions import load_config, copy_tables, create_hdfs_folder, log, timestamp


def to_raw(spark_session, data, fromDate=None, toDate=None, country=None, brand=None, isHistorical=True):
    log("info", "Beginning of --- RAW ZONE --- Data Processes")
    log("info", "   table: " + str(data))
    log("info", "   isHistorical: " + str(isHistorical))

    log("info", data + " table(s) copy process started.")

    def _copy(data_of):
        log("info", "Storing " + data_of + " data in the Raw zone.")
        _conf = load_config(data_of)
        copy_tables(_conf, _conf["hdfsFileSourcePath"], _conf["hdfsFileRawPath"],False, True)
        time.sleep(5)

    def _historical(data_of, histDest):
        _conf = load_config(data_of)
        create_hdfs_folder(histDest)
        time.sleep(5)
        create_hdfs_folder(histDest + data_of + "_source_copy/")
        time.sleep(5)
        copy_tables(_conf, _conf["hdfsFileRawPath"], histDest + data_of + "_source_copy/", True, True)
        time.sleep(5)

    if data == "all":
        _copy("madax")
        _copy("motor")
        _copy("samara")

        if isHistorical:
            _hist_destination = load_config("all")["hdfsFileCopyHistoricalPath"] + "image_as_of_" + timestamp() + "/"
            _historical("madax", _hist_destination)
            _historical("samara", _hist_destination)
            _historical("motor", _hist_destination)
    else:
        _copy(data)
        if isHistorical:
            _historical(data, load_config("all")["hdfsFileCopyHistoricalPath"] + "image_as_of_" + timestamp() + "/")

    time.sleep(5)
    log("info", "End of --- RAW ZONE --- Data Processes")
