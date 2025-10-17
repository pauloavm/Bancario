# arquivo: gerador_clientes.py

# -*- coding: utf-8 -*-
"""
================================================================================
SCRIPT 1: GERAÇÃO DA DIMENSÃO DE CLIENTES (D_CUSTOMER)
================================================================================
Objetivo: Gerar um conjunto de dados sintético para a dimensão de clientes de
um banco brasileiro fictício, cobrindo o período de 2000 a 2025.
Este script cria um arquivo CSV chamado 'd_customer.csv'.
"""

# 1. Importação das bibliotecas necessárias
import pandas as pd
import numpy as np
from faker import Faker
from datetime import date, timedelta
import random

# --- CONFIGURAÇÃO INICIAL ---
print("Iniciando a geração de dados de clientes...")

# Inicializa o Faker para gerar dados em português do Brasil (nomes, cidades, etc.)
fake = Faker("pt_BR")

# Define o período de tempo para a aquisição de novos clientes no banco
DATA_INICIO = date(2000, 1, 1)
DATA_FIM = date(2025, 10, 15)
TOTAL_DIAS_PERIODO = (DATA_FIM - DATA_INICIO).days

# Define o número total de clientes a serem gerados para simular um banco de grande porte
NUMERO_DE_CLIENTES = 15000

# --- LÓGICA DE GERAÇÃO ---
# Lista para armazenar os dados de cada cliente antes de criar o DataFrame
lista_clientes = []

for i in range(NUMERO_DE_CLIENTES):
    # Gera uma data de criação da conta ponderada, simulando um crescimento acelerado
    # nos últimos anos (transformação digital), em vez de uma distribuição linear.
    dias_desde_inicio = int(np.random.power(a=2.5) * TOTAL_DIAS_PERIODO)
    data_criacao_conta = DATA_INICIO + timedelta(days=dias_desde_inicio)
    data_criacao_conta = min(
        data_criacao_conta, DATA_FIM
    )  # Garante que não ultrapasse a data final

    # Gera uma data de nascimento realista para o cliente (entre 18 e 80 anos)
    data_nascimento = fake.date_of_birth(minimum_age=18, maximum_age=80)

    # Adiciona os dados do cliente a um dicionário
    lista_clientes.append(
        {
            "customer_id": 1000 + i,
            "nome_completo": fake.name(),
            "data_nascimento": data_nascimento,
            "idade": (date.today() - data_nascimento) // timedelta(days=365.25),
            "cidade": fake.city(),
            "estado": fake.state_abbr(),
            "data_criacao_conta": data_criacao_conta,
            "faixa_renda_mensal": random.choice(
                [
                    "0-1500",
                    "1501-3000",
                    "3001-5000",
                    "5001-8000",
                    "8001-12000",
                    "12001+",
                ]
            ),
            "score_de_credito": max(
                300, min(950, int(np.random.normal(loc=650, scale=150)))
            ),
        }
    )

# --- FINALIZAÇÃO E EXPORTAÇÃO ---
# Converte a lista de dicionários num DataFrame do Pandas
df_clientes = pd.DataFrame(lista_clientes)

# Salva o DataFrame num arquivo CSV com codificação UTF-8 para suportar caracteres especiais
df_clientes.to_csv("d_customer.csv", index=False, encoding="utf-8-sig")

print(
    f"Sucesso! O arquivo 'd_customer.csv' foi criado com {len(df_clientes)} clientes."
)
