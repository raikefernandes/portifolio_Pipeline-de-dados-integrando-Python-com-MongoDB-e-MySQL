from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests

def connect_mongo(uri):

    #criando conexão com o banco de dados
    client = MongoClient(uri, server_api=ServerApi('1'))

    #emviando um ping para o banco de dados e comfirmar a conexão
    try:
        client.admin.command('ping')
        print('Conexão realizada com sucesso')
    except Exception as e:
        print(e)
    return client
def create_connect_db(client, db_name):
    #criando conexão com o banco de dados
    db = client[db_name]
    return db
def create_connect_collection(db, col_name):
    #criando conexão com a coleção
    db[col_name]
    return db[col_name]
def extract_api_data(url):
    #extrair dados da api
    return requests.get(url).json()
def insert_data(col, data):
    result = col.insert_many(data)
    n_docs_inseridos = len(result.inserted_ids)
    return n_docs_inseridos 
