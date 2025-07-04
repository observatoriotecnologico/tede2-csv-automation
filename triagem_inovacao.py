# triagem_inovacao.py

import pandas as pd
import glob
import os
import json
import gspread
import numpy as np
from google.oauth2.service_account import Credentials
import sys

print("INFO: Iniciando o script triagem_inovacao.py")

# --- Configurações ---
PALAVRAS = [
    "inova", "patente", "tecnolog", "protótipo", "produto", "process", "algoritmo", "método",
    "síntese", "software", "dispositivo", "bioativo", "aplica", "automa", "engenharia",
    "startup", "spin-off", "modelo", "composto", "diagnóstic", "biomaterial", "nano", "encapsul", "inteligência artificial"
]
REGEX = '|'.join(PALAVRAS)
PASTA_CSV_ENTRADA = 'csvs'

# --- Verificação de Arquivos CSV de Entrada ---
print(f"INFO: Verificando a pasta de entrada para CSVs: {PASTA_CSV_ENTRADA}")
if not os.path.exists(PASTA_CSV_ENTRADA):
    print(f"ERRO FATAL: A pasta de CSVs '{PASTA_CSV_ENTRADA}' NÃO foi encontrada.", file=sys.stderr)
    sys.exit(1)

arquivos = glob.glob(os.path.join(PASTA_CSV_ENTRADA, '*.csv'))
if not arquivos:
    print(f"ALERTA: NENHUM arquivo CSV encontrado na pasta '{PASTA_CSV_ENTRADA}'. A planilha será atualizada apenas com o cabeçalho.", file=sys.stderr)
else:
    print(f"INFO: Encontrados {len(arquivos)} arquivos CSV na pasta '{PASTA_CSV_ENTRADA}'.")
    for f in arquivos:
        print(f"  - {os.path.basename(f)}")

# --- Leitura e Filtragem dos CSVs ---
df_final = []
total_registros_lidos = 0

for arquivo in arquivos:
    try:
        # Força a leitura de todas as colunas como string para evitar problemas de tipo de dado
        df = pd.read_csv(arquivo, dtype=str, encoding='utf-8-sig')
        total_registros_lidos += len(df)
        print(f"INFO: Lendo arquivo: {os.path.basename(arquivo)} com {len(df)} registros.")

        # Garante que as colunas de filtro existem e preenche nulos
        for campo in ['titulo', 'palavras_chave', 'resumo']:
            if campo not in df.columns:
                df[campo] = ''
        df[['titulo', 'palavras_chave', 'resumo']] = df[['titulo', 'palavras_chave', 'resumo']].fillna('')

        filtro = (
            df['titulo'].str.contains(REGEX, case=False, na=False) |
            df['palavras_chave'].str.contains(REGEX, case=False, na=False) |
            df['resumo'].str.contains(REGEX, case=False, na=False)
        )
        selecionados = df[filtro].copy()

        if not selecionados.empty:
            selecionados['arquivo_origem'] = os.path.basename(arquivo)
            df_final.append(selecionados)
            print(f"INFO: {len(selecionados)} registros selecionados de {os.path.basename(arquivo)}.")
        else:
            print(f"INFO: Nenhum registro selecionado de {os.path.basename(arquivo)}.")

    except Exception as e:
        print(f"ERRO: Falha ao processar o arquivo '{os.path.basename(arquivo)}': {e}", file=sys.stderr)

# --- Consolida os resultados e aplica limpeza robusta ---
if df_final:
    df_consolidado = pd.concat(df_final, ignore_index=True)
    print(f"INFO: Todos os CSVs processados. Total lido: {total_registros_lidos}. Total filtrado: {len(df_consolidado)}")
    
    # --- LIMPEZA DEFINITIVA "FORÇA BRUTA" ---
    # Etapa 1: Preenche qualquer valor nulo (NaN) com uma string vazia.
    df_consolidado.fillna('', inplace=True)
    
    # Etapa 2: Converte TODAS as colunas para o tipo string. Isso transforma np.inf em "inf".
    df_consolidado = df_consolidado.astype(str)
    
    # Etapa 3: Substitui as representações em string de valores inválidos.
    df_consolidado.replace(['nan', 'inf', '-inf', 'Infinity', '-Infinity', 'NaN'], '', inplace=True)

else:
    df_consolidado = pd.DataFrame(columns=[
        'ano', 'semestre', 'data_base',
