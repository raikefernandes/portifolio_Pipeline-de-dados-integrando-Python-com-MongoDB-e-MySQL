# Pipeline de Dados: Integração Python, MongoDB e MySQL

## Objetivo

Este projeto tem como objetivo construir um pipeline de dados completo, integrando **MongoDB** (NoSQL) e **MySQL** (relacional) usando **Python**. O pipeline realiza o processo de **ETL** (Extração, Transformação e Carga) entre os dois bancos de dados, facilitando a movimentação de dados entre fontes distintas.

O foco principal do projeto é demonstrar como manipular dados de diferentes fontes e realizar a transformação e o carregamento de dados de forma eficiente utilizando bibliotecas populares do ecossistema Python.

---

## Tecnologias Utilizadas

- **Python 3.x**: Linguagem principal utilizada para implementar o pipeline.
- **pandas**: Para manipulação e transformação de dados.
- **pymongo**: Para interação com MongoDB (NoSQL).
- **mysql.connector**: Para interação com MySQL (relacional).
- **Jupyter Notebook** (opcional): Para prototipação e demonstrações interativas.
- **Apache Airflow** (opcional): Para orquestrar o pipeline de dados em uma agenda automatizada.
- **Docker** (opcional): Para garantir que o ambiente de execução seja consistente em diferentes máquinas.

---

## Arquitetura do Sistema

O pipeline realiza as seguintes etapas:

1. **Extração de Dados**: 
   - Os dados são extraídos do banco **MongoDB**.
   
2. **Transformação de Dados**:
   - A transformação é feita no Python utilizando **pandas** para realizar limpeza, agregações e outras operações necessárias nos dados.
   
3. **Carga de Dados**: 
   - Os dados transformados são carregados no banco **MySQL**.

O fluxo de dados segue a seguinte arquitetura:

