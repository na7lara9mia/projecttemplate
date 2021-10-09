import os
import logging
import atexit
from pyspark.sql import SparkSession


def get_spark(app_name, queue='default', driver_cores=4, driver_mem='4g',
              executors=2, executor_cores=4, executor_mem='4g',
              dynamic_allocation=True, max_executors=8):
    """Create Spark application.

    Parameters
    ----------
    app_name : string
        Spark application name which must contain the application prd.
    queue : string, either 'default', 'preprod', 'prod'
        Name of the yarn queue to which the application is submitted.
    driver_cores : int
        Number of cores to use for the driver process.
    driver_mem : string, e.g. '4g'
        Amount of memory to use for the driver process.
    executors : int
        Number of executors for static allocation. With dynamic_allocation=True,
        the initial set of executors will be at least this large.
    executor_cores : int
        Number of cores to use on each executor.
    executor_mem : string, e.g. '8g'
        Amount of memory to use per executor process.
    dynamic_allocation : bool
        Whether to use dynamic resource allocation, which scales the number of executors
        registered with this application up and down based on the workload.
    max_executors : int
        Maximum number of executors to run if dynamic allocation is enabled.

    Returns
    -------
    spark_context : pyspark.context.SparkContext
        Context of the Spark application
    spark_session : pyspark.sql.session.SparkSession
        Session of the Spark application

    """
    logging.info('Creating Spark application with the provided configuration')
    logging.info('Environment variables are the following:')
    logging.info('\n' + get_spark_environment_variables())
    spark_session = build_spark_session(app_name=app_name, queue=queue,
                                        driver_cores=driver_cores, driver_mem=driver_mem,
                                        executors=executors, executor_cores=executor_cores,
                                        executor_mem=executor_mem,
                                        dynamic_allocation=dynamic_allocation,
                                        max_executors=max_executors)
    spark_context = spark_session.sparkContext
    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

    atexit.register(lambda: spark_context.stop())
    return spark_context, spark_session


def get_spark_environment_variables():
    """Get Spark environment variables."""
    return '\n'.join([
        "{} : {}".format(var, os.environ.get(var, '-variable is unset-'))
        for var in [
            'ENVIRONMENT',
            'PYSPARK_PYTHON',
            'PYSPARK_DRIVER_PYTHON',
            'SPARK_CLASSPATH',
            'SPARK_HOME',
            'PYTHONPATH',
        ]
    ])


def build_spark_session(app_name, queue='default',
                        driver_cores=1, driver_mem='15g',
                        executors=2, executor_cores=4, executor_mem='15g',
                        dynamic_allocation=True, max_executors=8):
    """Build Spark session."""
    return SparkSession.builder\
        .appName(app_name) \
        .config('spark.master', 'yarn') \
        .config('spark.submit.deployMode', 'client') \
        .config("spark.yarn.queue", queue) \
        .config("spark.driver.cores", driver_cores) \
        .config("spark.driver.memory", driver_mem) \
        .config('spark.executor.instances', executors) \
        .config('spark.executor.cores', executor_cores) \
        .config('spark.executor.memory', executor_mem) \
        .config("spark.shuffle.service.enabled", dynamic_allocation) \
        .config("spark.dynamicAllocation.enabled", dynamic_allocation) \
        .config("spark.dynamicAllocation.maxExecutors", max_executors) \
        .config('spark.driver.extraClassPath', '/soft/ora1210/db/jdbc/lib/ojdbc6.jar') \
        .config('spark.executor.extraClassPath', '/soft/ora1210/db/jdbc/lib/ojdbc6.jar') \
        .getOrCreate()
