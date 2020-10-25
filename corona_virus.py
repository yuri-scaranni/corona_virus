import os, io, requests, zipfile, csv, shutil
from datetime import datetime as dt
import pandas as pd
import mysql.connector as mysql
from sqlalchemy import create_engine

tablename = "corona_virus_brasil"
save_path = '/home/ec2-user/scaranni/corona_virus'
#save_path = r'C:\Users\scaranni\PycharmProjects\yuri_env\codes\extra-o-coronavirus'
file_type = '.csv'
#file_date = "{}".format(dt.now().strftime("%Y-%m-%d_%H-%M-%S"))
file = tablename + file_type
file_full_path = os.path.join(save_path, file)


def extract():
    print(">>>> Iniciando Extração da tabela Corona Virus Brasil")
    json_url = "https://xx9p7hp1p7.execute-api.us-east-1.amazonaws.com/prod/PortalGeral"
    headers = {'x-parse-application-id': 'unAFkcaNDeXajurGB7LChj8SgQYS2ptm'}
    json_data = requests.get(json_url, headers=headers)

    if json_data.status_code != 200:
        raise Exception("Não foi possível acessar o link {} corretamente".format(json_url))

    dic = json_data.json()
    url_tabela = dic['results'][0]['arquivo']['url']
    data_content = requests.get(url_tabela)
    with open(os.path.join(save_path, file), 'wb') as f:
        f.write(data_content.content)

    print(">>>> Fim da Extração da tabela Corona Virus Brasil")
    return os.path.join(save_path, file)


def load():
    print('>>>> Iniciando Dataframe e abrindo conexão com banco')

    dataframe = pd.read_csv(file_full_path, sep=';')

    print(dataframe.head())
    sqlEngine = create_engine('mysql+mysqlconnector://root:@127.0.0.1/scaranni_data', pool_recycle=3600)

    dbConnection = sqlEngine.connect()
    print('>>>> Conectado')

    try:
        frame = dataframe.to_sql(tablename, dbConnection, if_exists='replace', index=False)
    except ValueError as e:
        print(e)
    print('>>>> Carga finalizada')

dados = extract()
up = load()


