# -*- coding: utf-8 -*-

"""
================================================================================
Passo 1.2: Geração do Histórico de Transações (Tabela de Fatos)
================================================================================
Objetivo: Gerar um conjunto de dados sintético para a tabela de fatos de
transações, vinculando cada transação a um cliente da dimensão D_CUSTOMER.

Este script lê o arquivo 'd_customer.csv' e cria um novo arquivo CSV chamado
'f_transactions.csv', simulando o comportamento transacional dos clientes
ao longo do tempo.
"""

# 1. Importação das bibliotecas necessárias
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# 2. Configuração inicial
print("Iniciando a geração de dados de transações...")

# Definições do comportamento transacional
DATA_LANCAMENTO_PIX = datetime(2020, 11, 16)
TIPOS_DE_TRANSACAO_PRE_PIX = ["Débito", "Crédito", "TED", "DOC", "Boleto"]
TIPOS_DE_TRANSACAO_POS_PIX = [
    "Débito",
    "Crédito",
    "TED",
    "Boleto",
    "PIX",
    "PIX",
]  # PIX duplicado para aumentar a probabilidade
CANAIS = [
    "Agência",
    "Caixa Eletrônico",
    "Internet Banking",
    "Mobile App",
    "Mobile App",
    "Mobile App",
]  # Mobile App triplicado

# Carrega os dados dos clientes gerados no passo anterior
try:
    df_clientes = pd.read_csv("d_customer.csv", parse_dates=["data_criacao_conta"])
except FileNotFoundError:
    print("ERRO: O arquivo 'd_customer.csv' não foi encontrado.")
    print("Por favor, execute o script 'gerador_clientes.py' primeiro.")
    exit()

# 3. Geração das transações
lista_transacoes = []
id_transacao_global = 1

# Itera sobre cada cliente para gerar seu histórico de transações
print(
    f"Gerando transações para {len(df_clientes)} clientes. Isso pode levar alguns minutos..."
)
for index, cliente in df_clientes.iterrows():
    customer_id = cliente["customer_id"]
    data_abertura = cliente["data_criacao_conta"]
    renda_cliente = cliente["faixa_renda_mensal"]

    # Define o número de transações a serem geradas para este cliente
    # Clientes com scores mais altos tendem a transacionar mais
    num_transacoes = int(np.random.normal(loc=150, scale=40))
    # Ajusta o número de transações com base na renda
    if "12001+" in renda_cliente:
        num_transacoes *= 1.5
    elif "8001" in renda_cliente:
        num_transacoes *= 1.2

    # Define o período em que as transações podem ocorrer para este cliente
    data_hoje = datetime(2025, 10, 15)
    periodo_dias = (data_hoje - data_abertura).days

    if periodo_dias <= 0:
        continue  # Pula clientes que abriram conta hoje e ainda não têm transações

    for _ in range(int(num_transacoes)):
        # --- Geração dos dados de cada transação ---

        # Gera uma data aleatória para a transação dentro do período de atividade do cliente
        dias_aleatorios = random.randint(0, periodo_dias)
        data_transacao = data_abertura + timedelta(days=dias_aleatorios)

        # Determina o tipo de transação com base na data (era pré ou pós-PIX)
        if data_transacao < DATA_LANCAMENTO_PIX:
            tipo_transacao = random.choice(TIPOS_DE_TRANSACAO_PRE_PIX)
        else:
            # Após o lançamento do PIX, sua probabilidade aumenta com o tempo
            dias_pos_pix = (data_transacao - DATA_LANCAMENTO_PIX).days
            prob_pix = min(
                0.7, dias_pos_pix / (365 * 3.0)
            )  # Atinge 70% de chance em ~3 anos
            if random.random() < prob_pix:
                tipo_transacao = "PIX"
            else:
                tipo_transacao = random.choice(["Débito", "Crédito", "TED", "Boleto"])

        # Gera o valor da transação
        # Usamos uma distribuição log-normal para simular muitas transações pequenas e poucas grandes
        if tipo_transacao == "Crédito":  # Compras de crédito tendem a ter valor maior
            valor_transacao = round(np.random.lognormal(mean=4.5, sigma=1.0), 2)
        elif tipo_transacao in ["TED", "PIX"]:
            valor_transacao = round(np.random.lognormal(mean=5.0, sigma=1.2), 2)
        else:
            valor_transacao = round(np.random.lognormal(mean=3.8, sigma=0.8), 2)

        # Garante um valor mínimo
        valor_transacao = max(1.00, valor_transacao)

        # Define o canal, com maior probabilidade de ser digital com o passar do tempo
        anos_desde_2000 = data_transacao.year - 2000
        prob_digital = min(0.9, anos_desde_2000 / 25.0)  # Atinge 90% de chance em 2025
        if random.random() < prob_digital:
            canal = random.choice(["Mobile App", "Internet Banking"])
        else:
            canal = random.choice(["Agência", "Caixa Eletrônico"])

        # Adiciona a transação à lista
        lista_transacoes.append(
            {
                "transaction_id": id_transacao_global,
                "customer_id": customer_id,
                "data_transacao": data_transacao.strftime("%Y-%m-%d %H:%M:%S"),
                "tipo_transacao": tipo_transacao,
                "valor_transacao": valor_transacao,
                "canal": canal,
            }
        )
        id_transacao_global += 1

    # Imprime um feedback a cada 100 clientes processados
    if (index + 1) % 100 == 0:
        print(f"  ... {index + 1}/{len(df_clientes)} clientes processados.")

# 4. Conversão para DataFrame e Salvamento
print("Convertendo a lista de transações para um DataFrame do Pandas...")
df_transacoes = pd.DataFrame(lista_transacoes)

# Salva o DataFrame em um arquivo CSV
output_filename = "f_transactions.csv"
df_transacoes.to_csv(output_filename, index=False, encoding="utf-8-sig")

print("-" * 50)
print(
    f"Sucesso! O arquivo '{output_filename}' foi criado com {len(df_transacoes)} transações."
)
print("Abaixo estão as 5 primeiras linhas do arquivo gerado:")
print(df_transacoes.head())
print("-" * 50)
