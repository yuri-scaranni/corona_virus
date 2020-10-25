import os, io, requests, zipfile, csv, shutil
from datetime import datetime as dt
import pandas as pd
import mysql.connector as mysql
from sqlalchemy import create_engine
import sys

tablename = sys.argv[0]
save_path = '/home/ec2-user/scaranni/arquivos/'
file_type = '.csv'
file = tablename + file_type
file_full_path = f'/home/ec2-user/scaranni/arquivos/{file}'

def load(file_full_path):
    print('>>>> Iniciando Dataframe e abrindo conexão com banco')

    dataframe = pd.read_csv(file_full_path, sep=',')

    sqlEngine = create_engine('mysql+mysqlconnector://{user}:{password}@{server}/{database}'.
                              format(user='etl', password='Tocadovento@05051996*', server='localhost', database='scaranni_data'), pool_recycle=3600)

    dbConnection = sqlEngine.connect()
    print('>>>> Conectado')

    try:
        frame = dataframe.to_sql(tablename, dbConnection, if_exists='replace', index=False)
    except ValueError as e:
        print(e)
    print('>>>> Carga finalizada')

#dados = extract()
up = load(file_full_path)


