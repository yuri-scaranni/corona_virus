import os
import pandas as pd
import mysql.connector as mysql
from sqlalchemy import create_engine


db_acessos = {
    "user": "etl",
    "password": "Tocadovento@05051996*",
    "server": "localhost",
    "database": "scaranni_data"
}

def load(file_full_path, tablename):
    print('>>>> Iniciando Dataframe e abrindo conexão com banco')

    dataframe = pd.read_csv(file_full_path, sep=';')

    sqlEngine = create_engine('mysql+mysqlconnector://{}:{}@{}/{}'.
                              format(db_acessos['user'],
                                     db_acessos['password'],
                                     db_acessos['server'],
                                     db_acessos['database']),
                                pool_recycle=3600)

    dbConnection = sqlEngine.connect()
    print('>>>> Conectado')

    try:
        frame = dataframe.to_sql(tablename, dbConnection, if_exists='replace', index=False)
    except ValueError as e:
        print(e)
    print('>>>> Carga finalizada')





