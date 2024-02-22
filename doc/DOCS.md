## A Pipeline CI/CD
A Pipeline de CI/CD (Continuous Integration/Continuous Deployment) é um processo que realiza o deploy automático do código no ambiente de produção (servidor).
Ela está configurada no arquivo `.github/workflows/deploy.yaml`  e utiliza o recurso de **GitHub Actions** disponível no GitHub.

### Execução da Pipeline
A pipeline de deploy é ativada automaticamente de acordo com as regras definidas no arquivo de configuração.
Por padrão, está configurada para executar mediante qualquer push (commits, merges) na branch `main`

### Como funciona?
Uma vez iniciada, o GitHub provisiona máquinas para executar os passos definidos no arquivo de configuração. Após a execução, as máquinas são deprovisionadas.

## Configurar a Pipeline CI/CD

### Clonar o repositório no servidor

Criar um usuário no servidor. Acessar home do usuário `cd ~`

`git clone https://github.com/InstitutoTodosPelaSaude/arbo`

### Configurar chaves ssh

Gerar par de chaves ssh para o usuário com `ssh-keygen`

Copiar a chave pública no arquivo `~/.ssh/authorized_hosts`

Copiar a chave privada e criar github secret do repositório arbo[link]

Settings > Actions > Secrets and variables > Secrets

Criar os secrets:

| HOST_IP | Ip do servidor |
| --- | --- |
| HOST_IP | Ip/hostname do server |
| HOST_USER | Usuário ssh |
| SSH_PRIVATE_KEY | Chave privada |

⚠️ IMPORTANTE - Garantir que as informações estão sendo criadas como SECRET no Github


## Estrutura do projeto

### Serviços
| Serviço               | Descrição                                         |
| :---:                 | :---:                                             |
| Postgres              | Banco de dados que armazena todas as etapas de            processamento de dados, além do histórico de processamento das pipelines do Dagster|
| Dagster + DBT         | Ferramenta de orquestração de tarefas, responsável por executar as etapas de processamento de dados e as automações implementadas  |
| Streamlit             | Frontend da aplicação Arbo, proporciona a interação com a pipeline e visualização de gráficos sincronizados ao banco de dados da pipeline |
| Nginx                 | Proxy reverso responsável pelo controle das rotas e autenticação                                                                |
| xlsx2csv              | API para converter arquivos XLSX para CSV sem comprometer o formato das datas                                                         |

### Arquivos de configuração


| Arquivo           | Objetivo                        | Serviços que usam |
| :---:             | :---:                           | :---:             |
| postgres/.env     | criada a partir do postgres/.env.example, configura o user e senha do banco de dados.                              | Postgres          |
| postgres/create_database.sql | criada a partir do postgres/create_database_example.sql, deve ser configurada as credenciais dos usuários itps_dev e dagster para o banco de dados              | Postgres          |
| dbt/profiles.yml  | criada a partir do dbt/profiles_example.yml, configura a conexão do banco de dados pelo DBT, assim como seleciona o banco de produção ou desenvolvimento                                       | DBT
| .env              | criada a partir do arquivo .env.example, configura o banco de dados a ser acessado pelos códigos Python e pelo Dagster. Além disso, também configura as credenciais de acesso na autenticação do frontend| Dagster+DBT, Nginx, Streamlit           |
