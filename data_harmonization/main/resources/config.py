from os import environ as env
import netifaces

# application configuration
APP_NAME = "data_harmonization"
DB_NAME = "data_harmonization"

# cloud credentials
awsAccessKey = "AWS_ACCESS_KEY_ID"
awsSecretKey = "AWS_SECRET_ACCESS_KEY"
awsDefaultRegion = "AWS_DEFAULT_REGION"


# database credentials
mysqlUser = "root"
mysqlPassword = "Root_123"
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
