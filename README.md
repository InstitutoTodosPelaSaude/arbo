# Arbo
Pipeline de análise de dados de exames de diagnóstico para arbovírus (RADIM).

## Dependências
1. Docker
2. Python 8 ou superior

## Como executar

#### Container Docker
1. Crie um banco Postgres
2. Preencha o arquivo ```.env```, renomeando o arquivo ```.env.example``` 
3. Execute o docker-compose.yml
    ```sh
    docker compose up -d
    ```

#### Localmente
1. Preencha o arquivo ```.env```, renomeando o arquivo ```.env.example``` 
2. Crie e edite o arquivo ```profiles.yml``` na raiz do projeto, seguindo as [instruções do DBT](https://docs.getdbt.com/docs/core/connect-data-platform/profiles.yml)
3. Crie um ambiente virtual python
4. Instale as dependências:
    ```sh
    pip install -r requirements.txt
    ```
5. Execute o Dagster:
    ```sh
    DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 dagster dev
    ```
