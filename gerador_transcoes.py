# arquivo: gerador_transacoes.py

# -*- coding: utf-8 -*-
"""
================================================================================
SCRIPT 2: GERAÇÃO DA TABELA DE FATOS DE TRANSAÇÕES (F_TRANSACTIONS)
================================================================================
Objetivo: Gerar um histórico de transações para cada cliente, simulando a
evolução de canais e tipos de transação ao longo do tempo.
Este script lê 'd_customer.csv' e cria 'f_transactions.csv'.
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# --- CONFIGURAÇÃO INICIAL ---
print("Iniciando a geração de dados de transações...")

# Carrega os dados dos clientes. O script irá falhar se o arquivo não existir.
df_clientes = pd.read_csv("d_customer.csv", parse_dates=["data_criacao_conta"])

# Constantes que modelam o comportamento do mercado
DATA_LANCAMENTO_PIX = datetime(2020, 11, 16)
DATA_CENARIO_ATUAL = datetime(2025, 10, 15)

# --- LÓGICA DE GERAÇÃO ---
lista_transacoes = []
id_transacao_global = 1

# Itera sobre cada cliente para gerar o seu histórico individual
for _, cliente in df_clientes.iterrows():
    periodo_dias_cliente = (DATA_CENARIO_ATUAL - cliente["data_criacao_conta"]).days
    if periodo_dias_cliente <= 0:
        continue

    # O número de transações é influenciado pelo score de crédito do cliente
    num_transacoes = int(
        np.random.normal(loc=150, scale=40) * (cliente["score_de_credito"] / 700)
    )

    for _ in range(num_transacoes):
        # Gera uma data aleatória para a transação dentro do período de atividade do cliente
        data_transacao = cliente["data_criacao_conta"] + timedelta(
            days=random.randint(0, periodo_dias_cliente)
        )

        # Simula a adoção do PIX: a probabilidade de usar PIX aumenta com o tempo após o seu lançamento
        if data_transacao >= DATA_LANCAMENTO_PIX:
            dias_pos_pix = (data_transacao - DATA_LANCAMENTO_PIX).days
            prob_pix = min(
                0.7, dias_pos_pix / (365 * 3.0)
            )  # Atinge 70% de chance em ~3 anos
            tipo_transacao = (
                "PIX"
                if random.random() < prob_pix
                else random.choice(["Débito", "Crédito", "TED", "Boleto"])
            )
        else:
            tipo_transacao = random.choice(
                ["Débito", "Crédito", "TED", "DOC", "Boleto"]
            )

        # Simula a transformação digital: a probabilidade de usar um canal digital aumenta ao longo dos anos
        prob_digital = min(0.9, (data_transacao.year - 2000) / 25.0)
        canal = (
            random.choice(["Mobile App", "Internet Banking"])
            if random.random() < prob_digital
            else random.choice(["Agência", "Caixa Eletrônico"])
        )

        # O valor da transação segue uma distribuição log-normal (muitas transações pequenas, poucas grandes)
        valor_transacao = max(1.00, round(np.random.lognormal(mean=4.0, sigma=1.2), 2))

        lista_transacoes.append(
            {
                "transaction_id": id_transacao_global,
                "customer_id": cliente["customer_id"],
                "data_transacao": data_transacao.strftime("%Y-%m-%d %H:%M:%S"),
                "tipo_transacao": tipo_transacao,
                "valor_transacao": valor_transacao,
                "canal": canal,
            }
        )
        id_transacao_global += 1

# --- FINALIZAÇÃO E EXPORTAÇÃO ---
df_transacoes = pd.DataFrame(lista_transacoes)
df_transacoes.to_csv("f_transactions.csv", index=False, encoding="utf-8-sig")

print(
    f"Sucesso! O arquivo 'f_transactions.csv' foi criado com {len(df_transacoes)} transações."
)
