# 📊 Pipeline de Dados: Integração com Python, MongoDB e MySQL

Este projeto demonstra a construção de um pipeline de dados completo utilizando Python para integrar bancos de dados NoSQL (MongoDB) e relacionais (MySQL). O pipeline realiza o processo de ETL (Extração, Transformação e Carga), facilitando a movimentação e transformação de dados entre diferentes fontes.

---

## 🚀 Objetivos

- Implementar um pipeline de dados utilizando Python.
- Integrar e manipular dados entre MongoDB e MySQL.
- Demonstrar o processo de ETL de forma prática e eficiente.
- Utilizar bibliotecas populares do ecossistema Python para manipulação de dados.

---

## 🛠️ Tecnologias e Ferramentas Utilizadas

- **Python 3.x**: Linguagem principal para implementação do pipeline.
- **Pandas**: Manipulação e transformação de dados.
- **PyMongo**: Interação com o MongoDB.
- **MySQL Connector**: Conexão e operações com o MySQL.
- **Jupyter Notebook**: Prototipação e demonstrações interativas.
- **Apache Airflow** (opcional): Orquestração do pipeline de dados.
- **Docker** (opcional): Containerização do ambiente de desenvolvimento.

---

## 📁 Estrutura do Projeto

```
├── data/                   # Arquivos de dados utilizados no projeto
├── notebooks/              # Jupyter Notebooks com análises e testes
├── scripts/                # Scripts Python para ETL
├── requirements.txt        # Dependências do projeto
└── README.md               # Documentação do projeto
```

---

## ⚙️ Como Executar o Projeto

1. **Clone o repositório:**

   ```bash
   git clone https://github.com/raikefernandes/portifolio_Pipeline-de-dados-integrando-Python-com-MongoDB-e-MySQL.git
   cd portifolio_Pipeline-de-dados-integrando-Python-com-MongoDB-e-MySQL
   ```

2. **Crie um ambiente virtual e ative-o:**

   ```bash
   python -m venv venv
   source venv/bin/activate  # No Windows use: venv\Scripts\activate
   ```

3. **Instale as dependências:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure as conexões com os bancos de dados:**

   - **MongoDB**: Certifique-se de que o MongoDB está em execução e configure a URI de conexão no script correspondente.
   - **MySQL**: Verifique se o MySQL está ativo e configure as credenciais de acesso no script correspondente.

5. **Execute o pipeline:**

   ```bash
   python scripts/etl_pipeline.py
   ```

---

## 📌 Funcionalidades Implementadas

- Extração de dados de uma fonte (pode ser uma API ou arquivo).
- Armazenamento inicial dos dados no MongoDB.
- Transformação dos dados utilizando Pandas.
- Carga dos dados transformados no MySQL.
- Orquestração do processo com Apache Airflow (opcional).

---

## 🖼️ Demonstrações

*Adicione aqui capturas de tela ou gifs que demonstrem o funcionamento do pipeline, como visualizações no MongoDB, transformações com Pandas e inserções no MySQL.*

---

## 📚 Referências

- Curso [Pipeline de dados: integrando Python com MongoDB e MySQL](https://www.alura.com.br/curso-online-pipeline-dados-integrando-python-mongodb-mysql) da Alura.
- Documentações oficiais:
  - [Pandas](https://pandas.pydata.org/docs/)
  - [PyMongo](https://pymongo.readthedocs.io/)
  - [MySQL Connector/Python](https://dev.mysql.com/doc/connector-python/en/)

---

## 🤝 Contribuições

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou pull requests para sugerir melhorias ou correções.

---

## 📄 Licença

Este projeto está licenciado sob a [MIT License](LICENSE).
