### A Pipeline CI/CD
A Pipeline de CI/CD (Continuous Integration/Continuous Deployment) é um processo que realiza o deploy automático do código no ambiente de produção (servidor).
Ela está configurada no arquivo `.github/workflows/deploy.yaml`  e utiliza o recurso de **GitHub Actions** disponível no GitHub.

#### Execução da Pipeline
A pipeline de deploy é ativada automaticamente de acordo com as regras definidas no arquivo de configuração.
Por padrão, está configurada para executar mediante qualquer push (commits, merges) na branch `main`

#### Como funciona?
Uma vez iniciada, o GitHub provisiona máquinas para executar os passos definidos no arquivo de configuração. Após a execução, as máquinas são deprovisionadas.

### Configurar a Pipeline CI/CD

#### Clonar o repositório no servidor

Criar um usuário no servidor. Acessar home do usuário `cd ~`

`git clone https://github.com/InstitutoTodosPelaSaude/arbo`

#### Configurar chaves ssh

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

