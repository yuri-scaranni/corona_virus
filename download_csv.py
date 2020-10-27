import os
import zipfile
import shutil
import io
import requests
import pandas as pd


def extract(save_path, file_folder):
    print(">>>> Iniciando Extração da tabela Corona Virus Brasil")
    json_url = "https://xx9p7hp1p7.execute-api.us-east-1.amazonaws.com/prod/PortalGeral"
    headers = {'x-parse-application-id': 'unAFkcaNDeXajurGB7LChj8SgQYS2ptm'}
    json_data = requests.get(json_url, headers=headers)

    if json_data.status_code != 200:
        raise Exception("Não foi possível acessar o link {} corretamente".format(json_url))

    dic = json_data.json()
    url_tabela = dic['results'][0]['arquivo']['url']
    data_content = requests.get(url_tabela)

    arquivo_zip = zipfile.ZipFile(io.BytesIO(data_content.content))
    arquivo_zip.extractall(path=save_path)
    for root, arq, files in os.walk(save_path):
        for f in files:
            if f.endswith(".csv"):
                path_csv = os.path.join(root, f)
                new_name = os.path.join(root, 'corona_virus_brasil.csv')
                os.rename(path_csv, new_name)
                shutil.move(new_name, file_folder)
                shutil.rmtree(root)

            else:
                continue
    #with open(os.path.join(save_path, file), 'wb') as f:
    #    f.write(data_content.content)

    print(">>>> Fim da Extração da tabela Corona Virus Brasil")





