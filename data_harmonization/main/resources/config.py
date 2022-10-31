import os

# application configuration
APP_NAME = "data_harmonization"
DB_NAME = "data_harmonization"

# cloud credentials
awsAccessKey = "AWS_ACCESS_KEY_ID"
awsSecretKey = "AWS_SECRET_ACCESS_KEY"
awsDefaultRegion = "AWS_DEFAULT_REGION"


# database credentials
mysqlUser = "root"
mysqlPassword = "Root_123"  # "Root#1234"
mysqlLocalHost = "localhost"  # netifaces.gateways()['default'][2][0]
mysqlDriver = "com.mysql.cj.jdbc.Driver"
mysqlPort = "3306"

# logging credentials


# spark configurations
sparkCustomConfigs = [
    ("spark.executor.memory", "8g"),
    ("spark.executor.cores", "3"),
    ("spark.cores.max", "3"),
    ("spark.driver.memory", "8g"),
    ("spark.driver.maxResultSize", "4g"),
]

# table names
raw_entity_table = "rawentity"
blocking_table = "semimerged"
classification_table = "merged"
graph_connected_components_table = "connectedprofiles"
benchmark_table = "benchmark"
merged_table = "result"

# Threshold
jaccard_distance = 0.8
benchmark_confidence = 0.6
blocking_threshold = 0.8
classifier_confidence = 0.8
match_ratio = 0.5

# model_parameters
word_2_vec = 1  # 0 -> use count vectorizer, 1 -> word_2_vec
feature_for_deduplication = []

# logging directories

current_dir = os.path.dirname(os.path.abspath(__file__))
logging_dir = current_dir + "/logs/"
