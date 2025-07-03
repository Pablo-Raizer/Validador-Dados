# ✅ Validador de Dados - ERP Director

Ferramenta para **validação automatizada de dados** entre sistemas legados (Firebird, MySQL, SQL Server) e o banco de destino **SQL Server (ERP Director)**.  
Desenvolvida para acelerar a validação de dados durante implantações, com foco em consistência, agilidade e rastreabilidade.

---

## ⚙️ Principais Funcionalidades

- 🔗 Validação entre diferentes tipos de bancos: Firebird, MySQL e SQL Server
- 📄 Regras de validação externas em arquivos `.json`, com suporte a lógica condicional e normalização de dados
- 📊 Comparação dinâmica e linha a linha dos dados (ex: CNPJ, IE, Nome, Data Inativação)
- 📥 Exportação automática para Google Sheets + geração de relatórios locais em `.xlsx` e `.csv`
- 💡 Interface gráfica simples para selecionar pipeline e regras
- 📦 Modular e escalável para múltiplas entidades (clientes, produtos, fornecedores, etc.)

---

## 🧠 Alterações e Melhorias Recentes

- ✅ **Ordenação inteligente dos resultados**: agora baseada na chave primária (`CODIGO`) e respeita a ordem dos campos definidos no JSON
- ✅ **Separação total entre normalização e comparação**: os comparadores agora não fazem mais normalizações internas
- ✅ **Aplicação automática de normalizadores** com base nos campos definidos no JSON
- ✅ **Pipeline atualizado** para garantir que a ordenação dos resultados respeite a estrutura declarada nas regras
- ✅ **Interface dinâmica**: usuário escolhe pipeline e JSON diretamente pela interface
- ✅ **Geração de regras via planilha** com script auxiliar (`gerar_jsons_de_planilha.py`)

---

## 📁 Estrutura do Projeto

```
validacao_dados/
│   interface.py               # Interface gráfica
│   main.py                    # Executor de pipeline via terminal
│   run_validador.bat          # Atalho para iniciar a interface
│   Rodar_regras_json.bat      # Atalho para executar com JSON direto
│   README.md                  # Este documento
│   requirements.txt           # Dependências do projeto
│
├── assets/                    # Ícone usado no executável
├── config/                    # Credenciais e configurações globais
├── core/
│   ├── connectors/            # Firebird, MySQL e SQL Server
│   ├── pipelines/             # Lógica de execução por tipo de origem
│   ├── reports/               # Exportação para Google Sheets
│   └── validator.py           # Validador principal
│
├── data/
│   ├── input/                 # Planilhas para gerar regras
│   └── output/                # Arquivos de saída (CSV/XLSX)
│       └── jsons/             # Regras geradas a partir da planilha
│
├── rules/                     # Regras de validação em JSON
└── utils/                     # Scripts auxiliares (geração de JSONs)
```

---

## 🔄 Fluxo de Validação

```mermaid
graph TD
    A[run_validador.bat] --> B[interface.py (GUI)]
    B --> C[Usuário escolhe pipeline + JSON de regras]
    C --> D[Executa pipeline (ex: firebird_to_sql)]
    D --> E[Conectores buscam dados dos bancos]
    E --> F[Carrega regras do JSON escolhido]
    F --> G[Aplica normalizadores e comparadores do JSON]
    G --> H[Validação campo a campo (core/validator.py)]
    H --> I[Exportação automática Google Sheets + .xlsx/.csv]
```

---
## 🧩 Componentes do Projeto (Visão Geral)

| Componente                          | Função                                                                 |
| ---------------------------------- | ---------------------------------------------------------------------- |
| `run_validador.bat`                | Inicia a interface gráfica com duplo clique                            |
| `interface.py`                     | Interface visual: seleção de pipeline, escolha de regras e log ao vivo |
| `main.py`                          | Executa o validador direto via terminal com um JSON de regras          |
| `core/validator.py`                | Aplica a lógica de comparação linha a linha com base nas regras JSON   |
| `core/pipelines/*.py`              | Scripts que orquestram conexão, leitura de dados e execução da validação |
| `core/connectors/*.py`             | Conexão com bancos Firebird, MySQL, SQL Server                         |
| `core/reports/google_sheets.py`    | Envia automaticamente os resultados para o Google Sheets               |
| `rules/*.json`                     | Arquivos com regras de validação (campos, condições, mapeamentos)      |
| `data/input/`                      | Templates de planilha para gerar JSONs                                 |
| `data/output/`                     | Exporta arquivos `.csv` e `.xlsx` com os resultados da validação       |
| `utils/gerar_jsons_de_planilha.py`| Script que converte planilha Excel em regras JSON                      |
| `config/`                          | Contém credenciais, configurações globais e arquivos sensíveis         |
| `assets/icone.ico`                 | Ícone usado na versão `.exe`                                           |

---

👣 Como Usar – Passo a Passo Rápido
🛠️ 1. Prepare o ambiente (executar no PowerShell como administrador)- Só na primeira vez

# Crie a pasta do projeto (onde quiser)

C:\validacao_dados

# No PowerShell localize a pasta do projeto

cd "C:\Users\SeuUsuario\Documents\validacao_dados"

# Crie o ambiente virtual
python -m venv .venv

# Ative o ambiente virtual no PowerShell
.venv\Scripts\Activate.ps1

# (⚠️ Caso apareça erro de permissão, execute isso e tente ativar de novo:)
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser

# Instale as dependências do projeto
pip install -r requirements.txt

# Para desativar o ambiente virtual quando terminar:

deactivate

---

### 🧠 2. Prepare suas regras de validação

- Crie ou edite o `.json` dentro da pasta `rules/` ou `data/output/jsons/`
- Use a planilha `template_regras_clientes.xlsx` (em `data/input/`) para montar visualmente
- Execute o script `utils/gerar_jsons_de_planilha.py` para gerar os arquivos JSON automaticamente

---

### 🎯 3. Execute a Validação

Você pode rodar de dois jeitos:

#### ✅ Via Interface Gráfica

1. **Clique duplo** em `Iniciar_Validador.bat`
2. Escolha o pipeline desejado (ex: Firebird → SQL Server)
3. Selecione o arquivo `.json` com as regras
4. Clique em **"Executar Validação"**
5. Acompanhe o log da execução em tempo real

---

#### ⚙️ Via Terminal

```bash
python main.py firebird_to_sql regras_clientes.json
```

---

### 📊 4. Veja os Resultados

- Os arquivos `.xlsx` e `.csv` são salvos em `data/output/`
- Resultado também é enviado automaticamente para o Google Sheets  
  🔗 [Acessar Planilha](https://docs.google.com/spreadsheets/d/16u8KSsLNidNtlZ4daX2Vs-GALqgM7OvESHRpGB9VUI8)

---

### 🔄 5. Reaproveitar? Só trocar o JSON e executar de novo

---
## 📋 Exemplo de Regra JSON

```json
{
  "entidade": "cliente",
  "primary_key": ["DFcod_cliente", "CHAVEPES"],
  "campos": [
    {
      "destino": "DFnome",
      "origem": "NOME",
      "normalizar": "texto_simples"
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
      "normalizar": "documento"
    },
    {
      "destino": "DFinscr_estadual",
      "origem": "IE",
      "normalizar": "texto_simples"
    },
    {
      "destino": "DFdata_inativacao",
      "origem": "DATADESATIVADO",
      "normalizar": "data_sem_hora"
    }
  ]
}
```

---

## 🔐 Segurança

- 🔒 As credenciais estão armazenadas em `config/google_credentials.json` e `config/credentials.py`
- ⚠️ Nunca subir essas informações para repositórios públicos


---

## 📦 Expansão e Manutenção

### Adicionar nova entidade:
1. Crie um novo arquivo de regras em `rules/`
2. Se necessário, crie um novo pipeline em `core/pipelines/`
3. Execute pela interface e selecione a nova regra

### Gerar regras a partir de planilha:
Use o script `utils/gerar_jsons_de_planilha.py` com o template em `data/input/`

---

## ✅ Observações Finais

- Banco de destino sempre será SQL Server
- Interface exibe logs ao vivo
- Campos divergentes recebem status `Divergente`
- Relatório inclui resumo com totais de ativos/inativos
- Suporte a expansão futura para entidades adicionais
---

