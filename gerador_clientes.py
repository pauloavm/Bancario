# -*- coding: utf-8 -*-

"""
================================================================================
Passo 1.1: Geração da Base de Clientes (Dimensão Cliente)
================================================================================
Objetivo: Gerar um conjunto de dados sintético para a dimensão de clientes de
um banco brasileiro fictício, cobrindo o período de 2000 a 2025.

Este script cria um arquivo CSV chamado 'd_customer.csv' com dados demográficos
e financeiros realistas para os clientes.
"""

# 1. Importação das bibliotecas necessárias
import pandas as pd
import numpy as np
from faker import Faker
from datetime import date, timedelta
import random

# 2. Configuração inicial
print("Iniciando a geração de dados de clientes...")

# Inicializa o Faker para gerar dados em português do Brasil
fake = Faker("pt_BR")

# Define o período de tempo para a aquisição de clientes
data_inicio = date(2000, 1, 1)
data_fim = date(2025, 10, 15)
total_dias = (data_fim - data_inicio).days

# Define o número total de clientes a serem gerados
# Simulando um grande banco, vamos gerar 15.000 clientes para este exemplo.
# Este número pode ser aumentado para simular bancos maiores.
NUMERO_DE_CLIENTES = 15000

# 3. Criação da lista de clientes
# Esta lista armazenará os dados de cada cliente como um dicionário
lista_clientes = []

for i in range(NUMERO_DE_CLIENTES):
    # --- Geração de Dados Demográficos ---

    # Gera o nome do cliente
    nome_completo = fake.name()

    # Gera uma data de nascimento realista (entre 18 e 80 anos)
    hoje = date.today()
    data_nascimento = fake.date_of_birth(minimum_age=18, maximum_age=80)
    idade = (hoje - data_nascimento) // timedelta(days=365.25)

    # Gera localização (cidade e estado)
    cidade = fake.city()
    estado = fake.state_abbr()

    # --- Geração de Dados Bancários e Financeiros ---

    # Simula a data de criação da conta, com maior probabilidade de ser mais recente
    # Usamos uma distribuição ponderada para simular crescimento acelerado nos últimos anos
    dias_desde_inicio = int(np.random.power(a=2.5) * total_dias)
    data_criacao_conta = data_inicio + timedelta(days=dias_desde_inicio)
    # Garante que a data não ultrapasse o dia de hoje
    if data_criacao_conta > data_fim:
        data_criacao_conta = data_fim

    # Simula a faixa de renda mensal em Reais (BRL)
    faixas_de_renda = [
        "0-1500",
        "1501-3000",
        "3001-5000",
        "5001-8000",
        "8001-12000",
        "12001+",
    ]
    renda_mensal = random.choice(faixas_de_renda)

    # Simula uma pontuação de crédito (Score)
    # Scores mais baixos são mais comuns, então usamos uma distribuição que favorece isso
    score_de_credito = max(300, min(950, int(np.random.normal(loc=650, scale=150))))

    # 4. Adiciona o cliente à lista
    lista_clientes.append(
        {
            "customer_id": 1000 + i,  # Cria um ID único para cada cliente
            "nome_completo": nome_completo,
            "data_nascimento": data_nascimento,
            "idade": idade,
            "cidade": cidade,
            "estado": estado,
            "data_criacao_conta": data_criacao_conta,
            "faixa_renda_mensal": renda_mensal,
            "score_de_credito": score_de_credito,
        }
    )

    # Imprime um feedback a cada 1000 clientes gerados
    if (i + 1) % 1000 == 0:
        print(f"{i + 1}/{NUMERO_DE_CLIENTES} clientes gerados...")

# 5. Conversão para DataFrame do Pandas e Salvamento
print("Convertendo a lista para um DataFrame do Pandas...")
df_clientes = pd.DataFrame(lista_clientes)

# Ajusta os tipos de dados das colunas para otimização
df_clientes["data_nascimento"] = pd.to_datetime(df_clientes["data_nascimento"])
df_clientes["data_criacao_conta"] = pd.to_datetime(df_clientes["data_criacao_conta"])

# Salva o DataFrame em um arquivo CSV
output_filename = "d_customer.csv"
df_clientes.to_csv(output_filename, index=False, encoding="utf-8-sig")

print("-" * 50)
print(
    f"Sucesso! O arquivo '{output_filename}' foi criado com {len(df_clientes)} clientes."
)
print("Abaixo estão as 5 primeiras linhas do arquivo gerado:")
print(df_clientes.head())
print("-" * 50)
