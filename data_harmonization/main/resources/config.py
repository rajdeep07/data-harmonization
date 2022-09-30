from os import environ as env
import netifaces

# cloud credentials
awsAccessKey = "AWS_ACCESS_KEY_ID"
awsSecretKey = "AWS_SECRET_ACCESS_KEY"
awsDefaultRegion = "AWS_DEFAULT_REGION"


# database credentials
mysqlUser = "default"
mysqlPassword = "default"
mysqlLocalHost = netifaces.gateways()["default"][2][0]
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


# minLSH configuration
minlsh_n_hashes = 200
minlsh_band_size = 5
minlsh_shingle_size = 5
minlsh_n_docs = 300
minlsh_max_doc_length = 400
minlsh_n_similar_docs = 10
minlsh_collect_indexes = False