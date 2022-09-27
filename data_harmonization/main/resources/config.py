from os import environ as env
import netifaces

# cloud credentials
awsAccessKey = "AWS_ACCESS_KEY_ID"
awsSecretKey = "AWS_SECRET_ACCESS_KEY"
awsDefaultRegion = "AWS_DEFAULT_REGION"


# database credentials
mysqlUser = "default"
mysqlPassword = "default"
mysqlLocalHost = netifaces.gateways()['default'][2][0]
mysqlDriver = "com.mysql.cj.jdbc.Driver"
mysqlPort = "3306"


# logging credentials
