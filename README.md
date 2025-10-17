# Projeto de Pipeline e Análise de Dados Bancários

**Autor:** Paulo Munhoz

## Visão Geral do Projeto

Este projeto implementa um pipeline completo de engenharia e análise de dados para um banco brasileiro fictício. O processo abrange desde a geração de dados sintéticos (clientes, transações e indicadores macroeconômicos) até a transformação desses dados seguindo uma arquitetura Medallion (Bronze, Silver, Gold) e, por fim, a análise e visualização de Indicadores-Chave de Desempenho (KPIs).

O pipeline simula o comportamento transacional de clientes ao longo de um período de 25 anos (2000-2025), permitindo análises sobre a adoção digital, volume financeiro por canal, perfil de risco dos clientes e a correlação da atividade bancária com o cenário macroeconômico.

## Estrutura do Repositório

  * `gerador_clientes.py`: Script Python para gerar dados sintéticos de clientes (`d_customer.csv`).
  * `coletor_dados_macro.py`: Script Python para coletar dados macroeconômicos históricos do Banco Central do Brasil (`d_macro_economic.csv`).
  * `gerador_transcoes.py`: Script Python para gerar dados sintéticos de transações bancárias (`f_transactions.csv`).
  * `Transformação_tabelas.ipynb`: Notebook SQL (Databricks) que implementa o pipeline ETL da camada Bronze para Silver e Gold.
  * `KPI'S.ipynb`: Notebook SQL (Databricks) que calcula e visualiza os KPIs de negócio a partir da camada Gold.
  * `Movimentação Bancária 2025-10-17 17_41.pdf`: Um exemplo de dashboard/relatório com as visualizações geradas no notebook de KPIs.
  * `.gitignore`: Configurado para ignorar os arquivos `.csv` gerados.

## Como Funciona o Pipeline

O projeto é dividido em três etapas principais:

### 1\. Geração de Dados (Scripts Python)

Antes de iniciar a análise, os dados brutos (camada Bronze) são criados:

1.  **`gerador_clientes.py`**: Cria o arquivo `d_customer.csv`.

      * Gera dados demográficos (nome, idade, cidade, estado) e financeiros (data de criação da conta, faixa de renda, score de crédito) para 15.000 clientes fictícios.

2.  **`coletor_dados_macro.py`**: Cria o arquivo `d_macro_economic.csv`.

      * Utiliza a biblioteca `bcb` para buscar dados mensais históricos (2000-2025) de indicadores brasileiros: SELIC, IPCA, Taxa de Desemprego e PIB.

3.  **`gerador_transcoes.py`**: Cria o arquivo `f_transactions.csv`.

      * Lê o `d_customer.csv` e gera um histórico de transações para cada cliente, simulando um comportamento realista.
      * Considera a data de abertura da conta, o tipo de transação (incluindo a introdução do PIX em 2020) e o canal (com aumento da probabilidade de uso de canais digitais ao longo do tempo).

### 2\. Transformação de Dados (ETL)

Os arquivos `.csv` gerados são tratados como a camada **Bronze**. O notebook `Transformação_tabelas.ipynb` é responsável por processá-los. (Assume-se que os CSVs foram carregados em tabelas `bronze_customer`, `bronze_transactions` e `bronze_macro_economic` na plataforma de dados, como o Databricks).

  * **Camada Silver**:

      * `silver_customers`: Tabela de clientes com tipos de dados corrigidos (ex: datas).
      * `silver_transactions`: Tabela de transações com tipos de dados corrigidos (ex: timestamp, decimal) e criação de uma chave `anomes_id` para joins.
      * `silver_macro_economic`: Tabela macroeconômica com tipos de dados corrigidos e uma chave `anomes_id`.

  * **Camada Gold**:

      * `d_date`: Criação de uma tabela de dimensão de datas.
      * `gold_f_transactions`: Tabela de fatos principal, unindo as tabelas Silver (transações, clientes, dados macroeconômicos) e a dimensão de data. Esta tabela é otimizada para as consultas analíticas.

### 3\. Análise e Visualização (KPIs)

O notebook `KPI'S.ipynb` consome os dados da tabela `gold_f_transactions` para gerar as seguintes análises:

  * **KPI 1: Adoção Digital ao Longo do Tempo**:

      * Mede o percentual de transações feitas por canais digitais (Mobile App, Internet Banking) em relação ao total, mês a mês.

  * **KPI 2: Volume Financeiro por Canal**:

      * Agrega o volume financeiro (R$) e o número de transações por canal (Agência, Caixa Eletrônico, Internet Banking, Mobile App) ao longo dos anos.

  * **KPI 3: Análise do Perfil de Risco dos Clientes**:

      * Distribui a base de clientes em faixas de risco (Alto, Médio, Bom, Excelente) com base no `score_de_credito`.

  * **KPI 4: Correlação com Indicadores Macroeconômicos**:

      * Plota o volume total transacionado ao longo do tempo contra as taxas SELIC e de desemprego, permitindo uma análise visual de correlações.

O arquivo `Movimentação Bancária ... .pdf` apresenta as visualizações gráficas resultantes dessas análises.

## Como Executar o Projeto

1.  **Pré-requisitos**:

      * Python 3.x
      * Bibliotecas Python: `pandas`, `numpy`, `faker`, `bcb`
      * Um ambiente de processamento de dados SQL (ex: Databricks, Spark, etc.) capaz de executar notebooks.

2.  **Passo a Passo**:

    1.  Instale as bibliotecas Python necessárias:
        ```bash
        pip install pandas numpy faker bcb
        ```
    2.  Execute os scripts Python na ordem correta para gerar os dados brutos:
        ```bash
        python gerador_clientes.py
        python coletor_dados_macro.py
        python gerador_transcoes.py
        ```
    3.  Isto criará três arquivos: `d_customer.csv`, `d_macro_economic.csv`, e `f_transactions.csv`.
    4.  Carregue esses três arquivos `.csv` para sua plataforma de dados (ex: Databricks) e crie as tabelas da camada Bronze (ex: `workspace.default.bronze_customer`, `workspace.default.bronze_transactions`, `workspace.default.bronze_macro_economic`).
    5.  Abra e execute o notebook `Transformação_tabelas.ipynb` para processar os dados Brutos e criar as tabelas Silver e Gold.
    6.  Abra e execute o notebook `KPI'S.ipynb` para calcular os KPIs e gerar as visualizações.
