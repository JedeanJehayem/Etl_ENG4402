#Consolidação e Padronização de Bases de Prestadores

## 📝 Descrição
Este projeto realiza o processo de transformação e consolidação de três bases de prestadores de saúde em um único arquivo CSV padronizado[cite: 1]. [cite_start]O foco principal é a **qualidade, consistência e preparação dos dados** para análise posterior.

Os arquivos de entrada representam diferentes tipos de prestadores:
* **Hospitais**
* **SADT** (Serviços de Auxílio Diagnóstico e Terapia) 
* **Hospitais Dia** 

---

## 🚀 Fluxo de Processamento 
O script segue uma ordem lógica de tratamento para garantir a integridade da base final:
1. **Leitura** dos três arquivos CSV.
2. **Padronização** dos nomes das colunas e tratamento de nulos.
3. **Limpeza Textual** e padronização de campos (NOME FANTASIA, MUNICIPIO, UF).
4. **Tratamento de Identificadores** (CNPJ e CNES).
5. **Validação** de UF e padronização de data.
6. **Consolidação**, remoção de duplicados e exportação do CSV final.

---

## 🛠️ Principais Transformações

### 1. Padronização de Colunas e Nulos
* **Nomenclatura:** As colunas passam por remoção de acentos, conversão para maiúsculas e substituição de sublinhados por espaços para garantir compatibilidade entre datasets.
* **Valores Nulos:** Diferentes representações de ausência de dados (como `NULL`, `NAN`, `N/A`, `-`) são convertidas para um padrão único.

### 2. Tratamento de Campos Críticos
* **CNPJ:** Limpeza de caracteres não numéricos e preenchimento com zeros à esquerda até 14 dígitos.
* **CNES:** Limpeza e preenchimento com zeros até 7 dígitos[cite: 11]. [cite_start]Caso o valor exceda 7 dígitos, o registro é mantido e marcado na coluna `OBSERVACAO` como `-MAIOR QUE 7-`.
* **UF:** Validação para garantir duas letras maiúsculas correspondentes a uma unidade federativa válida do Brasil; caso contrário, o valor é convertido para nulo.
* **Data de Atualização:** Conversão para o formato padrão `YYYY-MM-DD`.

### 3. Rastreabilidade
* [cite_start]Cada registro recebe uma coluna **ORIGEM** (HOSPITAIS, SADT ou HOSPITAIS_DIA), permitindo identificar a fonte original do dado após a unificação.

---

## 📂 Estrutura de Arquivos

### Entrada 
O script busca os seguintes arquivos no dicionário ARQUIVOS:
* `Hospital - 100%.CSV` 
* `sadt_100.CSV` 
* `hospitais_dia_100.CSV` 

### Saída 
Gera o arquivo consolidado:
* **Arquivo:** `base_consolidada_fator_qualidade.csv` 
* **Colunas Finais:** `NOME FANTASIA`, `CNES`, `CNPJ`, `UF`, `MUNICIPIO`, `DATA DE ATUALIZACAO`, `ORIGEM`, `OBSERVACAO`
* **Formatação:** Separador `;`, codificação `utf-8-sig` e aspas em todos os campos (`QUOTE_ALL`) para facilitar a abertura no Excel.

---

## 💻 Requisitos e Execução 

### Pré-requisitos
* Python 3.x 
* Biblioteca `pandas` 
* Bibliotecas padrão: `csv`, `re`, `unicodedata` 

### Instalação
```bash
pip install pandas
