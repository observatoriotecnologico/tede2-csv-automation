# update_tede_csv.py

import os
import glob
from sickle import Sickle
import pandas as pd
from datetime import datetime
import time
import sys

# --- Configurações ---
OAI_ENDPOINT = 'https://tede.bc.uepb.edu.br/oai/request'
PASTA_CSV_SAIDA = 'csvs'

# --- Funções Auxiliares ---
def ano_semestre(data_str):
    try:
        if 'T' in data_str:
            dt = datetime.strptime(data_str[:10], '%Y-%m-%d')
        elif '-' in data_str and len(data_str) == 10:
            dt = datetime.strptime(data_str, '%Y-%m-%d')
        elif len(data_str) == 4:
            return data_str, '1' 
        else:
            print(f"ALERTA: Formato de data não reconhecido para '{data_str}'. Pulando.", file=sys.stderr)
            return None, None

        ano = dt.year
        semestre = '1' if dt.month <= 6 else '2'
        return str(ano), semestre
    except ValueError:
        print(f"ALERTA: Erro ao parsear data '{data_str}'. Pulando.", file=sys.stderr)
        return None, None
    except Exception as e:
        print(f"ERRO INESPERADO na função ano_semestre para '{data_str}': {e}. Pulando.", file=sys.stderr)
        return None, None

# --- Coleta de Dados OAI-PMH (com Paginação) ---
def coletar_registros_oai():
    print(f"INFO: Conectando ao endpoint OAI-PMH: {OAI_ENDPOINT}")
    sickle = Sickle(OAI_ENDPOINT)

    all_records_data = []
    record_count = 0

    try:
        # Inicia a primeira requisição
        response = sickle.ListRecords(metadataPrefix='oai_dc')
        current_records = response.records
        resumption_token = response.resumption_token

        # Processa o primeiro lote
        for record in current_records:
            md = record.metadata
            all_records_data.append({
                'titulos': '; '.join([t for t in md.get('title', []) if t is not None]),
                'autor': '; '.join([a for a in md.get('creator', []) if a is not None]),
                'orientador': '; '.join([c for c in md.get('contributor', []) if c and not str(c).startswith('CPF') and not str(c).startswith('http')]),
                'datas': [d for d in md.get('date', []) if d is not None],
                'curso': '; '.join([c for c in md.get('publisher', []) if c is not None]),
                'palavras_chave': '; '.
