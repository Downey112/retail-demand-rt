"""SparkSession factory with Hive support, set up for a single Windows machine."""
import os
import sys

from rdf import config as C


def _set_env():
    os.environ.setdefault("JAVA_HOME", str(C.JAVA_HOME))
    os.environ.setdefault("HADOOP_HOME", str(C.HADOOP_HOME))
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    for p in (C.JAVA_HOME / "bin", C.HADOOP_HOME / "bin"):
        if str(p) not in os.environ["PATH"]:
            os.environ["PATH"] = f"{p};{os.environ['PATH']}"


def get_spark(app: str, memory: str = "3g", cores: int = 6, shuffle_partitions: int = 16):
    _set_env()
    from pyspark.sql import SparkSession

    for p in (C.WAREHOUSE, C.METASTORE, C.CHECKPOINTS):
        p.mkdir(parents=True, exist_ok=True)
    metastore = C.METASTORE.as_posix()
    spark = (
        SparkSession.builder.master(f"local[{cores}]")
        .appName(app)
        .config("spark.driver.memory", memory)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.warehouse.dir", C.WAREHOUSE.as_uri())
        .config("spark.jars.packages", ",".join(C.SPARK_PACKAGES))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={metastore}")
        .config("javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={metastore}/metastore_db;create=true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")   # keep business_date a string
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {C.HIVE_DB}")
    return spark
