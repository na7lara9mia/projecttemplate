import logging
import time
import yaml
import subprocess
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
from projecttemplate.configuration.app import AppConfig

# Log file creation with Run ID
logging.basicConfig(filename='../log/pipeline_run_' + datetime.now().strftime("%Y%m%d%H%M%S") + '.log',
                    format=datetime.now().strftime("%d/%m/%Y %H:%M:%S ") + '%(levelname)s ' + '%(message)s',
                    level=logging.INFO)

mapping_ctry = {'FR': 'France', 'BE': 'Belgium', 'IT': 'Italy', 'SP': 'Spain', 'GB': 'Great Britain',
                'DE': 'Germany', 'PL': 'Poland', 'AT': 'Austria', 'NL': 'Netherlands',
                'PT': 'Portugal'}
countries = ('FR','SP')
brands = ('AP','AC','DS')
mapping_countries_eng = {'France': 'France', 'Belgique': 'Belgium', 'Italie': 'Italy', 'Espagne': 'Spain',
                              'Grande': 'Great Britain', 'Gde-Bretagne': 'Great Britain', 'Allemagne': 'Germany',
                              'Pologne': 'Poland', 'Autriche': 'Austria', 'Pays Bas': 'Netherlands',
                              'Pays-bas': 'Netherlands', 'Pays': 'Netherlands', 'Portugal': 'Portugal'}
mapping_rub= {'PROVISION SUR PERTES FUTURES': 'PSA - Buy Backs',
                             'PRIMES VENTES AUX SOCIETES': 'PSA - B2B promotions',
                             'PRIME QUALITE': 'PSA - Bonus for quality',
                             'PRIMES A LA PERFORMANCE RESEAU': 'PSA - Bonus for dealer performance',
                             'PROMOTIONS CLIENT FINAL': 'PSA - B2C promotions'}


def write_to_hdfs(df, path, list=[]):
    print("Writing in HDFS ...")
    if len(list) > 0:
        partitions = list[0]
        if len(list)-1 != 0:
            for i in range(len(list)-1):
                partitions = partitions , list[i+1]
        df \
            .coalesce(1) \
            .write \
            .mode("overwrite") \
            .partitionBy(partitions) \
            .parquet(path)
    else:
        df \
            .coalesce(1) \
            .write \
            .mode("overwrite") \
            .parquet(path)

def read_hdfs_file(session, path):
    df = session.read.parquet(path)
    return df

def cluster_copy_hdfs(fromPath, toPath, overwrite=True):
    if overwrite:
        cmd = "hadoop distcp -overwrite " + fromPath + " " + toPath
    else:
        # clear_contents_hdfs_folder(toPath)
        cmd = "hadoop distcp " + fromPath + " " + toPath
    try:
        subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        log("info", "Copying successful!")
    except Exception as e:
        log("error", "Copying unsuccessful!")
        log("error", str(e))


def copy_data_in_hdfs(fromPath, toPath, overwrite):
    if overwrite:
        cmd = "hdfs dfs -cp -f " + fromPath + " " + toPath + ";echo $?"
    else:
        cmd = "hdfs dfs -cp " + fromPath + " " + toPath + ";echo $?"
    try:
        if int(subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()[0]) == 0:
            log("info", "Copying successful.")
        else:
            log("error", "Copying unsuccessful!")
    except Exception as e:
        log("info", str(e))


# def clear_contents_hdfs_folder(folderPath):
#     try:
#         log("INFO", "Resetting the contents of the destination folder.")
#         proc = subprocess.Popen(["hdfs", "dfs", "-rm", "-r", folderPath + "/*"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#     except Exception as e:
#         log("ERROR", str(e))


def copy_tables(conf, source, destination, backUp=False, overwrite=True):
    for t in conf["tables"]:
        if backUp:
            t = t.split('/')[1]
        log("info", "Copying table " + t)
        print("from", source + t)
        print("to", destination)
        copy_data_in_hdfs(source + t, destination + "/", overwrite)
            # while hdfs_file_check(destination + "/" + t) != 0:


def hdfs_file_check(folderPath):
    f_check = "hdfs dfs -test -d " + folderPath + ";echo $?"
    try:
        return int(subprocess.Popen(f_check, shell=True, stdout=subprocess.PIPE).communicate()[0])
    except Exception as e:
        log("error", str(e))


def create_hdfs_folder(folderPath):
    if hdfs_file_check(folderPath) != 0:
        try:
            proc = subprocess.Popen(["hdfs", "dfs", "-mkdir", folderPath], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        except Exception as e:
            log("error", str(e))
    else:
        log("info", "Folder already exists.")


def timestamp():
    return datetime.now().strftime("%d-%m-%Y_%H-%M-%S")


def open_file(config_path):
    with open(r'' + config_path) as file:
        return yaml.load(file, Loader=yaml.FullLoader)


def load_config(data_of):
    if data_of not in ['samara', 'date', 'date_order', 'motor','madax', 'goal','opv', 'pdv', 'tax_rate', 'promo', 'family', 'samarex', 'vinpromo', "mxusm", 'channel_type','sort_client_promotions', 'Table_ref_marges','ventes_directes_indirectes','Classification_VME_Category','all']:
        AssertionError("Please provide a valid data type name ('madax' or 'samara' or 'all')")
    else:
        return open_file("../conf/" + data_of + ".yaml")


def log(typ, msg):
    if typ == "info":
        logging.info(msg)
    elif typ == "warning":
        logging.warning(msg)
    elif typ == "error":
        logging.error(msg)
    else:
        pass


def connect_db():
    app_config = AppConfig()
    print(app_config.db_uri_jdbc)
    return app_config.db_uri_jdbc


def hash_vin(df, column):
    df = df.withColumn(column, F.sha2(F.concat(F.col(column), F.lit('-VME')), 256))
    return df


def write_oracle(df, db_uri, table):
    print("Writing in Oracle ...")
    df.write.option("truncate", "true").jdbc(url=db_uri, table=table, mode="overwrite")


def stop_session(session):
    session.stop()
    logging.info("Session successfully closed.")
