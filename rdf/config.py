"""Project-wide settings. Paths, scope, model and inventory assumptions live here."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW_M5 = DATA / "raw" / "m5"
RETAIL_DB = DATA / "retail_ops.db"          # the "operational" source system (SQLite)

LAKE = ROOT / "lake"                        # stands in for HDFS
LAKE_RAW = LAKE / "raw"                     # Sqoop-style batch imports land here
LAKE_BRONZE = LAKE / "bronze"               # raw Kafka events land here
LAKE_SERVING = LAKE / "serving"             # small parquet files for Streamlit / Power BI
WAREHOUSE = LAKE / "warehouse"              # Hive managed tables
CHECKPOINTS = LAKE / "_checkpoints"
MODELS = LAKE / "models"
METASTORE = LAKE / "metastore"              # embedded Derby Hive metastore
HIVE_DB = "retail"

# Local tooling (no admin rights, no Docker): portable JDK 17, winutils, Kafka 3.9 in KRaft mode.
TOOLS = Path(r"C:\Users\courts\tools")
JAVA_HOME = TOOLS / "jdk17"
HADOOP_HOME = TOOLS / "hadoop"
KAFKA_HOME = TOOLS / "kafka"
KAFKA_BOOTSTRAP = "localhost:9092"
SPARK_PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4",
    "org.xerial:sqlite-jdbc:3.53.4.0",
]

TOPIC_POS = "pos-transactions"
TOPIC_INVENTORY = "inventory-updates"
TOPIC_STORE_EVENTS = "store-events"

# History in the source DB ends here; the 28 held-out days after it are replayed as the live stream.
HISTORY_END = "2016-05-22"
STREAM_START = "2016-05-23"
STREAM_DAYS = 28

# Stores that are modelled, streamed and optimised. The lake holds all 10.
SCOPE_STORES = ["CA_1", "CA_2", "CA_3", "CA_4"]
TRAIN_START = "2014-05-01"                  # modelling window (older history stays in the lake)

HORIZON = 7                                 # direct forecasts for the next 7 days (all features lag >= 7)
VALIDATION_ORIGINS = 4                      # weekly backtest origins at the end of history
RETRAIN_EVERY_DAYS = 7                      # periodic retraining inside the stream

# Inventory assumptions (M5 has no costs or stock levels, so these are stated assumptions).
LEAD_TIME_DAYS = 3
ORDER_COST = 1.0                            # fixed cost per SKU order line (store orders ride one consolidated truck), USD
HOLDING_RATE_ANNUAL = 0.25                  # holding cost as a share of unit price per year
GROSS_MARGIN = 0.30                         # underage cost per unit = margin x price
SERVICE_LEVEL_CAP = (0.80, 0.99)            # newsvendor critical ratio is clipped to this range

ECOM_SHARE = 0.15                           # synthetic share of units sold online (M5 has one channel)
SEED = 42
