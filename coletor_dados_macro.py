# -*- coding: utf-8 -*-

"""
================================================================================
Passo 2.1: Coletor de Dados Macroeconômicos (VERSÃO FINAL E ROBUSTA)
================================================================================
Objetivo: Coletar dados históricos de indicadores macroeconômicos chave do
Brasil, tratando as limitações de data de início de cada série e possíveis
falhas na API.

Esta versão foi reescrita para ser mais resiliente, criando um DataFrame
mestre e mesclando cada série individualmente.
"""

# 1. Importação das bibliotecas necessárias
import pandas as pd
from bcb import sgs
from datetime import datetime

# 2. Configuração inicial
print("Iniciando a coleta de dados macroeconômicos do Banco Central do Brasil...")

codigos_sgs = {
    "SELIC_DIARIA": 11,
    "IPCA_MENSAL": 433,
    "DESEMPREGO_MENSAL": 24369,
    "PIB_MENSAL": 24368,
}

data_inicio_geral = datetime(2000, 1, 1)
data_fim_geral = datetime(2025, 10, 15)

# 3. Criação de um DataFrame mestre com um intervalo de datas mensal
# Esta é a base do nosso ficheiro final. Garantimos que a coluna de data existirá sempre.
date_range = pd.date_range(
    start=data_inicio_geral, end=data_fim_geral, freq="ME"
)  # 'ME' = Month End
df_final = pd.DataFrame(date_range, columns=["data_fim_mes"])

print("Estrutura de datas mestre criada. Buscando e integrando cada série econômica...")

# 4. Busca e integração de cada série individualmente
for nome_serie, codigo_serie in codigos_sgs.items():
    print(f"  -> Processando série: '{nome_serie}' (código {codigo_serie})...")

    try:
        df_serie_bruta = None
        # Lógica especial para a SELIC por ser diária e ter o limite de 10 anos
        if nome_serie == "SELIC_DIARIA":
            lista_blocos_selic = []
            ano_inicio_bloco = data_inicio_geral.year
            anos_por_bloco = 9
            while ano_inicio_bloco <= data_fim_geral.year:
                ano_fim_bloco = ano_inicio_bloco + anos_por_bloco
                data_inicio_bloco = datetime(ano_inicio_bloco, 1, 1)
                data_fim_bloco = min(datetime(ano_fim_bloco, 12, 31), data_fim_geral)
                df_bloco = sgs.get(
                    {nome_serie: codigo_serie},
                    start=data_inicio_bloco,
                    end=data_fim_bloco,
                )
                lista_blocos_selic.append(df_bloco)
                ano_inicio_bloco = ano_fim_bloco + 1

            df_serie_bruta = pd.concat(lista_blocos_selic)
            df_serie_bruta = df_serie_bruta.loc[
                ~df_serie_bruta.index.duplicated(keep="first")
            ]
            df_serie_bruta.sort_index(inplace=True)

            # Processamento para agregar a SELIC diária em mensal
            df_serie_bruta[nome_serie] = df_serie_bruta[nome_serie].ffill()
            df_mensal_temp = df_serie_bruta.resample("ME").last()
            df_mensal_temp.rename(
                columns={"SELIC_DIARIA": "selic_fim_mes_anualizada"}, inplace=True
            )

        else:  # Para as séries mensais
            df_serie_bruta = sgs.get(
                {nome_serie: codigo_serie}, start=data_inicio_geral, end=data_fim_geral
            )
            # O resample garante o alinhamento correto no final do mês
            df_mensal_temp = df_serie_bruta.resample("ME").mean()

        # Junta a série processada ao nosso DataFrame final
        df_mensal_temp.index.name = "data_fim_mes"
        df_final = pd.merge(df_final, df_mensal_temp, on="data_fim_mes", how="left")
        print(f"     Série '{nome_serie}' integrada com sucesso.")

    except Exception as e:
        print(
            f"     AVISO: Falha ao buscar ou processar a série '{nome_serie}'. A coluna ficará vazia. Detalhes: {e}"
        )
        # Se falhar, garante que a coluna exista, mas com valores nulos
        if nome_serie == "SELIC_DIARIA":
            if "selic_fim_mes_anualizada" not in df_final.columns:
                df_final["selic_fim_mes_anualizada"] = pd.NA
        else:
            if nome_serie not in df_final.columns:
                df_final[nome_serie] = pd.NA

# 5. Limpeza Final
print("Finalizando o tratamento dos dados...")
# Reordena as colunas para a ordem desejada
colunas_finais = [
    "data_fim_mes",
    "selic_fim_mes_anualizada",
    "IPCA_MENSAL",
    "DESEMPREGO_MENSAL",
    "PIB_MENSAL",
]
colunas_presentes = [col for col in colunas_finais if col in df_final.columns]
df_final = df_final[colunas_presentes]

# Remove as linhas iniciais que não têm nenhum dado económico
primeiras_datas_validas = [
    df_final[col].first_valid_index()
    for col in colunas_presentes
    if col != "data_fim_mes"
]
if primeiras_datas_validas:
    data_minima = min(idx for idx in primeiras_datas_validas if idx is not None)
    df_final = df_final[df_final.index >= data_minima]

# 6. Salvamento do Arquivo
output_filename = "d_macro_economic.csv"
df_final.to_csv(
    output_filename,
    index=False,
    encoding="utf-8-sig",
    float_format="%.4f",
    date_format="%Y-%m-%d",
)

print("-" * 50)
print(
    f"Sucesso! O arquivo '{output_filename}' foi recriado com {len(df_final)} registros mensais."
)
print(
    "Abaixo estão as 10 primeiras linhas do arquivo, mostrando os dados mais antigos:"
)
print(df_final.head(10))
print("\nE as 5 últimas linhas, mostrando os dados mais recentes:")
print(df_final.tail())
print("-" * 50)
