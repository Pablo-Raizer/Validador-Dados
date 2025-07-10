# ✅ Validador de Dados - ERP Director

<div align="center">
  <img src="assets/icone.ico" alt="Ícone do Validador" width="128"/>
  
  **Ferramenta para validação automatizada de dados entre sistemas legados e ERP Director**
  
  [![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
  [![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
  [![Status](https://img.shields.io/badge/Status-Ativo-brightgreen.svg)]()
</div>

---

## 📋 Sobre o Projeto

O **Validador de Dados ERP Director** é uma ferramenta desenvolvida para **acelerar e automatizar a validação de dados** durante implantações de ERP. Realiza comparações precisas entre sistemas legados (Firebird, MySQL, SQL Server) e o banco de destino SQL Server do ERP Director.

### 🎯 Objetivo Principal
Garantir **consistência, agilidade e rastreabilidade** na migração de dados, reduzindo erros manuais e tempo de validação durante implantações.

---

## ⚙️ Principais Funcionalidades

### 🔗 **Conectividade Multi-Banco**
- Suporte completo para **Firebird**, **MySQL** e **SQL Server**
- Conexões simultâneas para comparação em tempo real
- Configuração flexível de credenciais por ambiente

### 📄 **Sistema de Regras Avançado**
- Regras de validação em arquivos **JSON externos**
- Suporte a **lógica condicional** e **normalização automática**
- Validação campo a campo com **múltiplas fontes de dados**
- Templates em Excel para criação visual de regras

### 📊 **Validação Inteligente**
- Comparação **linha a linha** entre bancos
- Normalização automática de dados (CNPJ, CPF, IE, datas)
- Detecção de divergências com **status detalhado**
- Validação de chaves primárias e campos obrigatórios

### 📥 **Exportação Completa**
- Exportação automática para **Google Sheets**
- Geração de relatórios locais em **Excel (.xlsx)** e **CSV**
- Logs detalhados com **rastreabilidade completa**
- Dashboards com **totais e estatísticas**

### 💡 **Interface Moderna**
- Interface gráfica **intuitiva** para seleção de pipelines
- **Log ao vivo** durante execução
- Seleção dinâmica de regras e configurações
- Suporte a execução via **terminal** ou **interface**

---

## 🏗️ Arquitetura do Sistema

### 📁 **Estrutura Modular**

```text
validacao_dados/
├── 📂 assets/                    # Recursos visuais
│   └── icone.ico                 # Ícone da aplicação
├── 📂 config/                    # Configurações
│   ├── credentials.py            # Credenciais dos bancos
│   └── google_credentials.json   # Credenciais Google Sheets
├── 📂 core/                      # Núcleo do sistema
│   ├── 📂 connectors/            # Conectores de banco
│   │   ├── firebird_connector.py
│   │   ├── mysql_connector.py
│   │   └── sqlserver_connector.py
│   ├── 📂 pipelines/             # Pipelines de validação
│   │   ├── firebird_to_sql.py
│   │   ├── mysql_to_sql.py
│   │   └── base_pipeline.py
│   ├── 📂 reports/               # Geração de relatórios
│   │   ├── google_sheets.py
│   │   ├── excel_exporter.py
│   │   └── csv_exporter.py
│   ├── validator.py              # ⭐ Motor de validação
│   ├── normalizadores.py         # ⭐ Normalização de dados
│   └── comparadores.py           # ⭐ Comparadores especializados
├── 📂 data/                      # Dados de entrada e saída
│   ├── 📂 input/                 # Templates e dados de entrada
│   │   └── template_regras_clientes.xlsx
│   └── 📂 output/                # Resultados da validação
│       ├── 📂 jsons/             # Regras geradas
│       ├── 📂 excel/             # Relatórios Excel
│       └── 📂 csv/               # Relatórios CSV
├── 📂 rules/                     # Regras de validação
│   ├── regras_clientes.json
│   ├── regras_produtos.json
│   └── regras_fornecedores.json
├── 📂 utils/                     # Utilitários
│   ├── gerar_jsons_de_planilha.py
│   ├── database_utils.py
│   └── file_utils.py
├── interface.py                  # 🖥️ Interface gráfica
├── main.py                       # 🎯 Execução via terminal
├── run_validador.bat             # 🚀 Iniciador da interface
├── Rodar_regras_json.bat         # 🔧 Gerador de regras
├── requirements.txt              # 📦 Dependências
└── README.md                     # 📚 Documentação
```

### 🧩 **Componentes Principais**

| Componente | Função | Tecnologia |
|------------|--------|------------|
| **🎯 Core Engine** | Motor principal de validação | Python |
| **🔗 Connectors** | Conectores multi-banco | PyODBC, Firebird, MySQL |
| **📊 Validators** | Lógica de comparação | Pandas, NumPy |
| **📄 Rules Engine** | Sistema de regras JSON | JSON, Lógica condicional |
| **📈 Reports** | Geração de relatórios | Google Sheets API, OpenPyXL |
| **🖥️ Interface** | Interface gráfica | Tkinter |

---

## 🚀 Instalação e Configuração

### 📋 **Pré-requisitos**
- **Python 3.8+** instalado
- **Drivers de banco** configurados (ODBC, Firebird, MySQL)
- **Credenciais de acesso** aos bancos de dados
- **Conta Google** para exportação (opcional)

### 🛠️ **Instalação**

#### 1️⃣ **Preparar o Ambiente**

```powershell
# Clone o repositório
git clone https://github.com/Pablo-Raizer/Validador-Dados.git
cd Validador-Dados

# Crie o ambiente virtual
python -m venv .venv

# Ative o ambiente virtual
.venv\Scripts\Activate.ps1

# Se houver erro de permissão
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser

# Instale as dependências
pip install -r requirements.txt
```

#### 2️⃣ **Configurar Credenciais**

```python
# config/credentials.py
DATABASE_CONFIGS = {
    'firebird': {
        'host': 'localhost',
        'database': 'caminho/para/banco.fdb',
        'user': 'SYSDBA',
        'password': 'masterkey'
    },
    'mysql': {
        'host': 'localhost',
        'database': 'nome_banco',
        'user': 'root',
        'password': 'senha'
    },
    'sqlserver': {
        'server': 'localhost',
        'database': 'ERP_Director',
        'user': 'sa',
        'password': 'senha'
    }
}
```

#### 3️⃣ **Configurar Google Sheets (Opcional)**

1. Acesse [Google Cloud Console](https://console.cloud.google.com/)
2. Crie um projeto e habilite Google Sheets API
3. Baixe o arquivo `credentials.json`
4. Renomeie para `google_credentials.json` e coloque em `config/`

---

## 📖 Guia de Uso

### 🎯 **Execução Rápida**

#### **Via Interface Gráfica** (Recomendado)
```bash
# Inicie a interface
run_validador.bat

# Ou execute diretamente
python interface.py
```

#### **Via Terminal**
```bash
# Execute validação específica
python main.py firebird_to_sql regras_clientes.json

# Com parâmetros adicionais
python main.py mysql_to_sql regras_produtos.json --verbose
```

### 📄 **Criando Regras de Validação**

#### **Método 1: Via Excel (Recomendado)**

1. Abra o template `data/input/template_regras_clientes.xlsx`
2. Preencha as regras conforme necessário
3. Execute o gerador:
   ```bash
   python utils/gerar_jsons_de_planilha.py
   ```

#### **Método 2: Criação Manual JSON**

```json
{
  "entidade": "cliente",
  "descricao": "Validação de dados de clientes",
  "primary_key": ["DFcod_cliente", "CHAVEPES"],
  "campos": [
    {
      "destino": "DFnome",
      "origem": "NOME",
      "normalizar": "texto",
      "obrigatorio": true,
      "descricao": "Nome do cliente"
    },
    {
      "destino": "DFcnpj_cpf",
      "origem": ["CNPJ", "CPF"],
      "condicao": {
        "coluna": "TIPOFISCAL",
        "valores": {
          "F": "CPF",
          "J": "CNPJ"
        }
      },
      "normalizar": "documento",
      "obrigatorio": true,
      "descricao": "Documento fiscal (CPF/CNPJ)"
    },
    {
      "destino": "DFinscr_estadual",
      "origem": "IE",
      "normalizar": "inscricao_estadual",
      "obrigatorio": false,
      "descricao": "Inscrição estadual"
    },
    {
      "destino": "DFdata_inativacao",
      "origem": "DATADESATIVADO",
      "normalizar": "data",
      "obrigatorio": false,
      "descricao": "Data de inativação"
    }
  ]
}
```

### 🔍 **Tipos de Normalização Disponíveis**

| Tipo | Descrição | Exemplo |
|------|-----------|---------|
| `texto` | Normalização básica de strings | Remove espaços, maiúsculas |
| `documento` | CPF/CNPJ | `123.456.789-01` → `12345678901` |
| `inscricao_estadual` | IE | Remove caracteres especiais |
| `data` | Datas | `2023-12-31` → `31/12/2023` |
| `numero` | Valores numéricos | `1,234.56` → `1234.56` |
| `telefone` | Números de telefone | `(11) 9999-9999` → `11999999999` |
| `cep` | CEP | `01234-567` → `01234567` |

### 📊 **Interpretando Resultados**

#### **Status de Validação**
- ✅ **Conforme**: Dados idênticos após normalização
- ❌ **Divergente**: Diferenças encontradas
- ⚠️ **Atenção**: Campos nulos ou valores suspeitos
- 🔍 **Investigar**: Regras condicionais não atendidas

#### **Localização dos Resultados**
- 📁 **Local**: `data/output/` (Excel e CSV)
- ☁️ **Online**: [Google Sheets](https://docs.google.com/spreadsheets/d/16u8KSsLNidNtlZ4daX2Vs-GALqgM7OvESHRpGB9VUI8)

---

## 🔧 Personalização e Extensão

### 🆕 **Adicionando Nova Entidade**

1. **Crie o arquivo de regras**:
   ```json
   # rules/regras_nova_entidade.json
   {
     "entidade": "nova_entidade",
     "primary_key": ["id_campo"],
     "campos": [...]
   }
   ```

2. **Execute pela interface ou terminal**:
   ```bash
   python main.py firebird_to_sql regras_nova_entidade.json
   ```

### 🔌 **Adicionando Novo Conector**

1. **Crie o conector**:
   ```python
   # core/connectors/novo_banco_connector.py
   class NovoBancoConnector:
       def __init__(self, config):
           self.config = config
       
       def connect(self):
           # Implementar conexão
           pass
       
       def execute_query(self, query):
           # Implementar execução
           pass
   ```

2. **Registre no sistema**:
   ```python
   # core/pipelines/novo_banco_to_sql.py
   from .base_pipeline import BasePipeline
   from ..connectors.novo_banco_connector import NovoBancoConnector
   
   class NovoBancoToSQLPipeline(BasePipeline):
       # Implementar pipeline
       pass
   ```

### 🧪 **Adicionando Novo Normalizador**

```python
# core/normalizadores.py
def normalizar_custom(valor):
    """
    Novo normalizador personalizado
    """
    if not valor:
        return None
    
    # Implementar lógica de normalização
    return valor.strip().upper()

# Registrar no mapeamento
NORMALIZADORES = {
    'custom': normalizar_custom,
    # ... outros normalizadores
}
```

---

## 📈 Monitoramento e Logs

### 📊 **Logs Detalhados**
- **Interface**: Log ao vivo durante execução
- **Terminal**: Output detalhado com timestamps
- **Arquivo**: Logs salvos em `data/output/logs/`

### 🔍 **Métricas Disponíveis**
- Total de registros processados
- Percentual de conformidade
- Tempo de execução por etapa
- Erros e warnings detalhados

### 📋 **Exemplo de Relatório**

```
=== RELATÓRIO DE VALIDAÇÃO ===
Entidade: clientes
Pipeline: firebird_to_sql
Data: 2024-01-15 14:30:00

📊 Estatísticas:
- Total de registros: 1,234
- Conformes: 1,180 (95.6%)
- Divergentes: 54 (4.4%)
- Tempo execução: 2m 34s

🔍 Principais Divergências:
- DFcnpj_cpf: 32 registros (2.6%)
- DFinscr_estadual: 22 registros (1.8%)
```

---

## 🔐 Segurança

### 🛡️ **Práticas de Segurança**
- **Credenciais**: Nunca versione arquivos de credenciais
- **Logs**: Não registre dados sensíveis em logs
- **Conexões**: Use conexões seguras (SSL/TLS)
- **Permissões**: Acesso somente leitura nos bancos origem

### 🔒 **Configuração Segura**
```python
# config/credentials.py
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_CONFIGS = {
    'firebird': {
        'host': os.getenv('FB_HOST'),
        'database': os.getenv('FB_DATABASE'),
        'user': os.getenv('FB_USER'),
        'password': os.getenv('FB_PASSWORD')
    }
}
```

---

## 🚀 Roadmap

### 🔮 **Próximas Funcionalidades**
- [ ] **Dashboard Web** com métricas em tempo real
- [ ] **API REST** para integração com outros sistemas
- [ ] **Agendamento automático** de validações
- [ ] **Notificações** por email/Slack
- [ ] **Suporte a Oracle** e **PostgreSQL**
- [ ] **Validação de relacionamentos** entre entidades
- [ ] **Machine Learning** para detecção de padrões

### 🎯 **Melhorias Planejadas**
- [ ] **Performance**: Processamento paralelo
- [ ] **Interface**: Migração para web (Flask/Django)
- [ ] **Testes**: Cobertura de testes automatizados
- [ ] **Docker**: Containerização da aplicação
- [ ] **CI/CD**: Pipeline de integração contínua

---

## 🤝 Contribuição

### 📝 **Como Contribuir**

1. **Fork** o repositório
2. **Crie** uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. **Commit** suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. **Push** para a branch (`git push origin feature/AmazingFeature`)
5. **Abra** um Pull Request

### 🐛 **Reportar Bugs**
- Use as [Issues](https://github.com/Pablo-Raizer/Validador-Dados/issues)
- Inclua detalhes do erro, ambiente e steps para reproduzir

### 💡 **Sugerir Melhorias**
- Abra uma [Issue](https://github.com/Pablo-Raizer/Validador-Dados/issues) com label `enhancement`
- Descreva o caso de uso e benefícios

---

## 📞 Suporte

### 🆘 **Resolução de Problemas**

#### **Erro de Conexão com Banco**
```
Erro: Não foi possível conectar ao banco
Solução: Verifique credenciais e conectividade
```

#### **Erro de Permissão PowerShell**
```
Erro: Execution Policy
Solução: Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
```

#### **Erro de Importação**
```
Erro: ModuleNotFoundError
Solução: pip install -r requirements.txt
```

### 📧 **Contato**
- **Desenvolvedor**: Pablo Raizer
- **GitHub**: [@Pablo-Raizer](https://github.com/Pablo-Raizer)
- **Issues**: [Validador-Dados/Issues](https://github.com/Pablo-Raizer/Validador-Dados/issues)

---

## 📄 Licença

Este projeto está sob a licença **MIT**. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

---

## 🙏 Agradecimentos

- **Equipe ERP Director** pelo suporte e feedback
- **Comunidade Python** pelas bibliotecas utilizadas
- **Contribuidores** que ajudaram a melhorar o projeto

---

<div align="center">
  <p>Feito por <a href="https://github.com/Pablo-Raizer">Pablo Raizer</a></p>
  <p>⭐ Se este projeto te ajudou, considere dar uma estrela!</p>
</div>