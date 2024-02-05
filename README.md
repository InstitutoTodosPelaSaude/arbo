# Arbo
Pipeline de análise de dados de exames de diagnóstico para arbovírus (RADIM).

## Dependências
1. [Docker](https://docs.docker.com/get-docker/)
2. Linux ou [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install
) (Windows)

## Instalando com Docker

### Configurando Postgres

No diretório ```postgres``` faça as seguintes alterações:

1. Crie um arquivo ```.env``` e preencha as informações com base no modelo ```.env.example```. Estas serão as credenciais do usuário root do banco.

2. Crie um arquivo ```create_database.sql``` com base no modelo ```create_database_example.sql``` e altere as credenciais dos usuários _itps_dev_ e _dagster_. Este arquivo será executado na criação do banco e irá criar os usuários e as tabelas necessárias para o projeto.

3. Inicie os contêiners do ```docker-compose.yml```
    ```sh
    docker compose up -d
    ```

4. Acesse a interface do Adminer em ```localhost:8091``` para testar a conexão com o banco. No campo hostname, insira ```postgresdwitps``` e porta ```5432```. Acesse com o usuário de escolha.

### Configurando o dbt

No diretório ```dbt``` faça as seguintes alterações:

1. Crie um arquivo ```profiles.yml``` com base no modelo ```profiles_example.yml``` e insira as credenciais dos usuarios para conexão com o banco.

    1.   A seção ```dev``` deve ser preenchida com as credenciais do usuário de desenvolvimento e a seção ```prod``` com as credenciais do usuário de produção.

    2. Para executar em ambiente produtivo, altere o campo ```target``` para ```prod```.

    3. Caso esteja executando em ambiente de desenvolvimento local, para se conectar ao postgres do docker, altere o campo ```host``` para o IP da sua máquina, que pode ser obtido com o comando ```hostname -I``` (geralmente é o primeiro IP retornado) e a porta para ```5433```.

### Configurando o Dagster

1. No diretório raiz, crie um arquivo ```.env``` com base no modelo ```.env.example``` e preencha as variáveis de ambiente com as credenciais do usuário de desenvolvimento.

    * Preencha os campos DB_* com as credenciais do ambiente (dev, prod).

    * Preencha os campos DB_DAGSTER_* com as credenciais do banco de dados onde o dagster irá armazenar os metadados.

    * Preencha os campos DAGSTER_* para configurar o usuário e senha que dará acesso ao Dagster UI.

2. Inicie os contêiners do ```docker-compose.yml```
    ```sh
    docker compose up -d
    ```

3. Acesse a interface em ```localhost:80```.

# Estrutura do projeto

## Serviços
| Serviço               | Descrição                                         |
| :---:                 | :---:                                             |
| Postgres              | Banco de dados que armazena todas as etapas de            processamento de dados, além do histórico de processamento das pipelines do Dagster|
| Dagster + DBT         | Ferramenta de orquestração de tarefas, responsável por executar as etapas de processamento de dados e as automações implementadas  |
| Streamlit             | Frontend da aplicação Arbo, proporciona a interação com a pipeline e visualização de gráficos sincronizados ao banco de dados da pipeline |
| Nginx                 | Proxy reverso responsável pelo controle das rotas e autenticação                                                                |
| xlsx2csv              | API para converter arquivos XLSX para CSV sem comprometer o formato das datas                                                         |

## Arquivos de configuração


| Arquivo           | Objetivo                        | Serviços que usam |
| :---:             | :---:                           | :---:             |
| postgres/.env     | criada a partir do postgres/.env.example, configura o user e senha do banco de dados.                              | Postgres          |
| postgres/create_database.sql | criada a partir do postgres/create_database_example.sql, deve ser configurada as credenciais dos usuários itps_dev e dagster para o banco de dados              | Postgres          |
| dbt/profiles.yml  | criada a partir do dbt/profiles_example.yml, configura a conexão do banco de dados pelo DBT, assim como seleciona o banco de produção ou desenvolvimento                                       | DBT
| .env              | criada a partir do arquivo .env.example, configura o banco de dados a ser acessado pelos códigos Python e pelo Dagster. Além disso, também configura as credenciais de acesso na autenticação do frontend| Dagster+DBT, Nginx, Streamlit           |