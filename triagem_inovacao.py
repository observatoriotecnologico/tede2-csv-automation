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
        df = pd.read_csv(arquivo, dtype=str, encoding='utf-8-sig')
        total_registros_lidos += len(df)
        print(f"INFO: Lendo arquivo: {os.path.basename(arquivo)} com {len(df)} registros.")

        # Garante que as colunas de filtro existem
        for campo in ['titulo', 'palavras_chave', 'resumo']:
            if campo not in df.columns:
                df[campo] = ''
                print(f"ALERTA: Coluna '{campo}' não encontrada em {os.path.basename(arquivo)}. Criando coluna vazia.", file=sys.stderr)
        
        # Preenche valores nulos nas colunas de busca para evitar erros com .str.contains
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

# --- Consolida os resultados e limpa valores ---
if df_final:
    df_consolidado = pd.concat(df_final, ignore_index=True)
    print(f"INFO: Todos os CSVs processados. Total lido: {total_registros_lidos}. Total filtrado: {len(df_consolidado)}")
    
    # --- ETAPA ÚNICA E DEFINITIVA DE LIMPEZA ---
    # 1. Substitui valores numéricos especiais (NaN, Inf) por strings vazias.
    df_consolidado.replace([np.nan, np.inf, -np.inf], '', inplace=True)
    # 2. Converte TUDO para o tipo string para máxima compatibilidade com a API do Google Sheets.
    df_consolidado = df_consolidado.astype(str)
else:
    df_consolidado = pd.DataFrame(columns=[
        'ano', 'semestre', 'data_base', 'titulo', 'autor', 'orientador', 'curso',
        'palavras_chave', 'resumo', 'link', 'arquivo_origem'
    ])
    print("ALERTA: Nenhum registro de inovação encontrado. A planilha será atualizada apenas com os cabeçalhos.", file=sys.stderr)

# ------- INÍCIO DA PARTE DE EXPORTAÇÃO PARA GOOGLE SHEETS -------
print("\nINFO: Iniciando conexão com Google Sheets.")
try:
    creds_dict = json.loads(os.environ['GOOGLE_CREDS'])
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    gc = gspread.authorize(creds)
    print("INFO: Autenticação com Google API BEM-SUCEDIDA.")

    SHEET_ID = "1Ft0IDKEgRe5HnyPPtmgdicK_xrmrQYTBctGs0glS5aI"
    NOME_ABA = "Coleta"

    try:
        sh = gc.open_by_key(SHEET_ID)
        print(f"INFO: Planilha '{sh.title}' aberta com sucesso.")
    except gspread.SpreadsheetNotFound:
        print(f"ERRO FATAL: Planilha com ID '{SHEET_ID}' não encontrada ou sem permissão. Verifique se o e-mail da conta de serviço foi compartilhado na planilha.", file=sys.stderr)
        sys.exit(1)

    # Remove a aba antiga para garantir uma atualização limpa
    try:
        wks_antiga = sh.worksheet(NOME_ABA)
        sh.del_worksheet(wks_antiga)
        print(f"INFO: Aba antiga '{NOME_ABA}' removida.")
    except gspread.exceptions.WorksheetNotFound:
        print(f"INFO: Aba '{NOME_ABA}' não existia. Uma nova será criada.")

    # Prepara os dados para envio
    headers = df_consolidado.columns.tolist()
    data_to_send = [headers] + df_consolidado.values.tolist()
    
    # Adiciona a nova aba com o tamanho correto
    rows_count = len(data_to_send)
    cols_count = len(headers)
    wks = sh.add_worksheet(title=NOME_ABA, rows=rows_count, cols=cols_count)
    print(f"INFO: Nova aba '{NOME_ABA}' criada.")

    # DEBUG: Verifica a primeira linha de dados apenas se ela existir
    if len(data_to_send) > 1:
        print(f"DEBUG: Primeira linha de dados a ser enviada: {data_to_send[1][:3]}...") # Mostra apenas 3 itens
    else:
        print("DEBUG: Não há linhas de dados para enviar (apenas cabeçalho).")

    # Atualiza a planilha
    wks.update(data_to_send, value_input_option='USER_ENTERED')
    print(f'\nSUCESSO: Planilha "{NOME_ABA}" atualizada com {len(df_consolidado)} registros!')

except Exception as e:
    print(f"\nERRO FATAL DURANTE A ATUALIZAÇÃO DA PLANILHA: {type(e).__name__}: {e}", file=sys.stderr)
    sys.exit(1)
