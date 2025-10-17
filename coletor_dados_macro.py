# arquivo: coletor_dados_macro.py

# -*- coding: utf-8 -*-
"""
================================================================================
SCRIPT 3: COLETA DE DADOS MACROECONÔMICOS (VERSÃO ROBUSTA)
================================================================================
Objetivo: Coletar dados históricos de indicadores do Brasil via API do BCB.
Este script foi desenhado para ser resiliente a falhas e a diferentes datas
de início de cada série histórica.
"""

import pandas as pd
from bcb import sgs
from datetime import datetime

# --- CONFIGURAÇÃO INICIAL ---
print("Iniciando a coleta de dados macroeconômicos...")

# Dicionário com os códigos oficiais das séries temporais do BCB
codigos_sgs = {
    "SELIC_DIARIA": 11,
    "IPCA_MENSAL": 433,
    "DESEMPREGO_MENSAL": 24369,
    "PIB_MENSAL": 24368,
}
DATA_INICIO_GERAL = datetime(2000, 1, 1)
DATA_FIM_GERAL = datetime(2025, 10, 15)

# --- LÓGICA DE COLETA E PROCESSAMENTO ---
# 1. Cria um DataFrame "mestre" com todos os meses do período.
# Isto garante que a nossa estrutura de datas é sólida, mesmo que a API falhe.
date_range = pd.date_range(
    start=DATA_INICIO_GERAL, end=DATA_FIM_GERAL, freq="ME"
)  # 'ME' = Month End
df_final = pd.DataFrame(date_range, columns=["data_fim_mes"])

print("Estrutura de datas mestre criada. Buscando e integrando cada série econômica...")

# 2. Itera sobre cada indicador, busca-o individualmente e integra-o ao DataFrame mestre.
for nome_serie, codigo_serie in codigos_sgs.items():
    print(f"  -> Processando série: '{nome_serie}'...")
    try:
        # Lógica especial para a SELIC, que é diária e tem um limite de consulta de 10 anos na API
        if nome_serie == "SELIC_DIARIA":
            df_serie_bruta = sgs.get(
                {nome_serie: codigo_serie}, start=DATA_INICIO_GERAL, end=DATA_FIM_GERAL
            )  # A lib `python-bcb` já lida com o loop de anos
            df_serie_bruta[nome_serie] = df_serie_bruta[
                nome_serie
            ].ffill()  # Preenche dias sem valor (fins de semana)
            df_mensal_temp = df_serie_bruta.resample(
                "ME"
            ).last()  # Agrega para o último valor do mês
            df_mensal_temp.rename(
                columns={"SELIC_DIARIA": "selic_fim_mes_anualizada"}, inplace=True
            )
        else:
            # Para as séries mensais, a busca é direta
            df_serie_bruta = sgs.get(
                {nome_serie: codigo_serie}, start=DATA_INICIO_GERAL, end=DATA_FIM_GERAL
            )
            df_mensal_temp = df_serie_bruta.resample(
                "ME"
            ).mean()  # Agrega pela média do mês

        # Junta a série processada ao nosso DataFrame final usando a data como chave
        df_mensal_temp.index.name = "data_fim_mes"
        df_final = pd.merge(df_final, df_mensal_temp, on="data_fim_mes", how="left")
        print(f"     Série '{nome_serie}' integrada com sucesso.")
    except Exception as e:
        print(
            f"     AVISO: Falha ao buscar a série '{nome_serie}'. A coluna ficará vazia. Detalhes: {e}"
        )

# --- FINALIZAÇÃO E EXPORTAÇÃO ---
df_final.to_csv(
    "d_macro_economic.csv", index=False, encoding="utf-8-sig", date_format="%Y-%m-%d"
)
print("Sucesso! O arquivo 'd_macro_economic.csv' foi recriado.")
