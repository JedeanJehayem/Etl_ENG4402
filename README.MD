# [cite_start]Consolidação e Padronização de Bases de Prestadores[cite: 1]

## 📝 Descrição
[cite_start]Este projeto realiza o processo de transformação e consolidação de três bases de prestadores de saúde em um único arquivo CSV padronizado[cite: 1]. [cite_start]O foco principal é a **qualidade, consistência e preparação dos dados** para análise posterior[cite: 1].

[cite_start]Os arquivos de entrada representam diferentes tipos de prestadores[cite: 2]:
* [cite_start]**Hospitais** [cite: 2]
* [cite_start]**SADT** (Serviços de Auxílio Diagnóstico e Terapia) [cite: 2]
* [cite_start]**Hospitais Dia** [cite: 2]

---

## [cite_start]🚀 Fluxo de Processamento [cite: 27]
[cite_start]O script segue uma ordem lógica de tratamento para garantir a integridade da base final[cite: 27]:
1. [cite_start]**Leitura** dos três arquivos CSV[cite: 27].
2. [cite_start]**Padronização** dos nomes das colunas e tratamento de nulos[cite: 27].
3. [cite_start]**Limpeza Textual** e padronização de campos (NOME FANTASIA, MUNICIPIO, UF)[cite: 9, 27].
4. [cite_start]**Tratamento de Identificadores** (CNPJ e CNES)[cite: 27].
5. [cite_start]**Validação** de UF e padronização de data[cite: 27].
6. [cite_start]**Consolidação**, remoção de duplicados e exportação do CSV final[cite: 27].

---

## 🛠️ Principais Transformações

### 1. Padronização de Colunas e Nulos
* [cite_start]**Nomenclatura:** As colunas passam por remoção de acentos, conversão para maiúsculas e substituição de sublinhados por espaços para garantir compatibilidade entre datasets[cite: 7].
* [cite_start]**Valores Nulos:** Diferentes representações de ausência de dados (como `NULL`, `NAN`, `N/A`, `-`) são convertidas para um padrão único[cite: 8].

### 2. Tratamento de Campos Críticos
* [cite_start]**CNPJ:** Limpeza de caracteres não numéricos e preenchimento com zeros à esquerda até 14 dígitos[cite: 10].
* [cite_start]**CNES:** Limpeza e preenchimento com zeros até 7 dígitos[cite: 11]. [cite_start]Caso o valor exceda 7 dígitos, o registro é mantido e marcado na coluna `OBSERVACAO` como `-MAIOR QUE 7-`[cite: 11, 24].
* [cite_start]**UF:** Validação para garantir duas letras maiúsculas correspondentes a uma unidade federativa válida do Brasil; caso contrário, o valor é convertido para nulo[cite: 12].
* [cite_start]**Data de Atualização:** Conversão para o formato padrão `YYYY-MM-DD`[cite: 13].

### 3. Rastreabilidade
* [cite_start]Cada registro recebe uma coluna **ORIGEM** (HOSPITAIS, SADT ou HOSPITAIS_DIA), permitindo identificar a fonte original do dado após a unificação[cite: 16].

---

## 📂 Estrutura de Arquivos

### [cite_start]Entrada [cite: 4]
[cite_start]O script busca os seguintes arquivos no dicionário ARQUIVOS[cite: 4]:
* [cite_start]`Hospital - 100%.CSV` [cite: 4]
* [cite_start]`sadt_100.CSV` [cite: 4]
* [cite_start]`hospitais_dia_100.CSV` [cite: 4]

### [cite_start]Saída [cite: 5, 19]
Gera o arquivo consolidado:
* [cite_start]**Arquivo:** `base_consolidada_fator_qualidade.csv` [cite: 5]
* [cite_start]**Colunas Finais:** `NOME FANTASIA`, `CNES`, `CNPJ`, `UF`, `MUNICIPIO`, `DATA DE ATUALIZACAO`, `ORIGEM`, `OBSERVACAO`[cite: 19].
* [cite_start]**Formatação:** Separador `;`, codificação `utf-8-sig` e aspas em todos os campos (`QUOTE_ALL`) para facilitar a abertura no Excel[cite: 20, 21].

---

## [cite_start]💻 Requisitos e Execução 

### Pré-requisitos
* [cite_start]Python 3.x 
* [cite_start]Biblioteca `pandas` 
* [cite_start]Bibliotecas padrão: `csv`, `re`, `unicodedata` 

### Instalação
```bash
pip install pandas
