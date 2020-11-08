from py2neo import Graph
import configparser

def connect():

    config = configparser.ConfigParser()
    config.read('./config.ini')

    host = config["SETTINGS"]["DBHOST"]
    user = config["SETTINGS"]["DBUSERNAME"]
    password = config["SETTINGS"]["DBPASSWORD"]
    dbName = config["SETTINGS"]["DBNAME"]

    return Graph(bolt=True, host=host, user=user, password=password, name=dbName)
