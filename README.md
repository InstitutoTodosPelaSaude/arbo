# Arbo
Pipeline de análise de dados de exames de diagnóstico para arbovírus (RADIM).

## Dependências
1. Docker

## Como executar

### Instalando com Docker

### Configurando Postgres

No diretório ```postgres``` faça as seguintes alterações:

1. Crie um arquivo ```.env``` e preencha as informações com base no modelo ```.env.example```. Estas serão as credenciais do usuário root do banco.

2. Crie um arquivo ```create_database.sql``` com base no modelo ```create_database_example.sq``` e altere as credenciais dos usuários _itps_dev_ e _dagster_. Este arquivo será executado na criação do banco e irá criar os usuários e as tabelas necessárias para o projeto.

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

2. Inicie os contêiners do ```docker-compose.yml```
    ```sh
    docker compose up -d
    ```

3. Acesse a interface em ```localhost:3000```.