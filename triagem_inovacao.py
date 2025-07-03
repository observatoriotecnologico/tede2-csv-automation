# triagem_inovacao.py

import pandas as pd
import glob
import os
import json
import gspread
from google.oauth2.service_account import Credentials
import sys # Para saída de erro controlada

print("INFO: Iniciando o script triagem_inovacao.py")

# --- Configurações ---
PALAVRAS = [
    "inova", "patente", "tecnolog", "protótipo", "produto", "process", "algoritmo", "método",
    "síntese", "software", "dispositivo", "bioativo", "aplica", "automa", "engenharia",
    "startup", "spin-off", "modelo", "composto", "diagnóstic", "biomaterial", "nano", "encapsul", "inteligência artificial"
]
REGEX = '|'.join(PALAVRAS)

PASTA_CSV_ENTRADA = 'csvs' # Pasta onde esperamos encontrar os CSVs

# --- Verificação de Arquivos CSV de Entrada ---
print(f"INFO: Verificando a pasta de entrada para CSVs: {PASTA_CSV_ENTRADA}")
if not os.path.exists(PASTA_CSV_ENTRADA):
    print(f"ERRO FATAL: A pasta de CSVs '{PASTA_CSV_ENTRADA}' NÃO foi encontrada. Verifique o download/descompactação do artefato.", file=sys.stderr)
    sys.exit(1) # Encerra o script com erro se a pasta não existir

arquivos = glob.glob(os.path.join(PASTA_CSV_ENTRADA, '*.csv'))
if not arquivos:
    print(f"ALERTA: NENHUM arquivo CSV encontrado na pasta '{PASTA_CSV_ENTRADA}'. Verifique se o artefato foi descompactado corretamente e se contém CSVs.", file=sys.stderr)
    # Se não há CSVs, o DataFrame consolidado será vazio, e apenas o cabeçalho será enviado.
    # Não faremos um sys.exit(1) aqui para que a planilha seja atualizada (mesmo que vazia).
else:
    print(f"INFO: Encontrados {len(arquivos)} arquivos CSV na pasta '{PASTA_CSV_ENTRADA}'.")
    for f in arquivos:
        print(f"  - {f}")

# --- Leitura e Filtragem dos CSVs ---
df_final = []
total_registros_lidos = 0
total_registros_filtrados = 0

for arquivo in arquivos:
    try:
        # Usar 'utf-8-sig' para ler CSVs gerados pelo update_tede_csv.py
        df = pd.read_csv(arquivo, dtype=str, encoding='utf-8-sig') 
        total_registros_lidos += len(df)
        print(f"INFO: Lendo arquivo: {os.path.basename(arquivo)} com {len(df)} registros.")

        # Garante que as colunas existem para evitar KeyError
        for campo in ['titulo', 'palavras_chave', 'resumo']:
            if campo not in df.columns:
                df[campo] = ''
                print(f"ALERTA: Coluna '{campo}' não encontrada em {os.path.basename(arquivo)}. Criando coluna vazia para filtragem.", file=sys.stderr)

        # Aplica o filtro de inovação
        filtro = (
            df['titulo'].str.contains(REGEX, case=False, na=False) |
            df['palavras_chave'].str.contains(REGEX, case=False, na=False) |
            df['resumo'].str.contains(REGEX, case=False, na=False)
        )
        selecionados = df[filtro].copy()

        if not selecionados.empty:
            selecionados['arquivo_origem'] = os.path.basename(arquivo)
            df_final.append(selecionados)
            total_registros_filtrados += len(selecionados)
            print(f"INFO: {len(selecionados)} registros selecionados do arquivo {os.path.basename(arquivo)} por palavras-chave de inovação.")
        else:
            print(f"INFO: Nenhum registro selecionado do arquivo {os.path.basename(arquivo)} por palavras-chave de inovação.")

    except Exception as e:
        print(f"ERRO: Falha ao processar o arquivo '{os.path.basename(arquivo)}': {e}", file=sys.stderr)

# Consolida os resultados
if df_final:
    df_consolidado = pd.concat(df_final, ignore_index=True)
    print(f"INFO: Todos os CSVs processados e consolidados. Total de registros lidos: {total_registros_lidos}. Total de registros filtrados para inovação: {len(df_consolidado)}")
else:
    df_consolidado = pd.DataFrame(columns=[
        'ano', 'semestre', 'data_base', 'titulo', 'autor', 'orientador', 'curso', 
        'palavras_chave', 'resumo', 'link', 'arquivo_origem'
    ])
    print("ALERTA: Nenhum registro de inovação encontrado após a filtragem em todos os CSVs. A planilha será atualizada apenas com os cabeçalhos.", file=sys.stderr)


# ------- INÍCIO DA PARTE DE EXPORTAÇÃO PARA GOOGLE SHEETS -------
print("INFO: Iniciando conexão com Google Sheets via gspread.")
try:
    # Lê as credenciais do Secret do GitHub
    creds_dict = json.loads(os.environ['GOOGLE_CREDS'])
    creds = Credentials.from_service_account_info(creds_dict, scopes=[
        "https://www.googleapis.com/auth/spreadsheets"
    ])
    gc = gspread.authorize(creds)
    print("INFO: Autenticação com Google Sheets BEM-SUCEDIDA.")

    SHEET_ID = "1Ft0IDKEgRe5HnyPPTmgdicK_xrmrQYTBctGs0glS5aI" # Seu ID da planilha 'tede-uepb'
    NOME_ABA = "Coleta"  # Nome da aba

    sh = gc.open_by_key(SHEET_ID)
    print(f"INFO: Planilha '{sh.title}' aberta com sucesso (ID: {SHEET_ID}).")

    # Remove a aba antiga se existir
    try:
        wks = sh.worksheet(NOME_ABA)
        print(f"INFO: Aba '{NOME_ABA}' encontrada. Removendo...")
        sh.del_worksheet(wks)
        print(f"INFO: Aba '{NOME_ABA}' removida com sucesso.")
    except gspread.exceptions.WorksheetNotFound:
        print(f"INFO: Aba '{NOME_ABA}' NÃO encontrada. Prosseguindo para criar a nova aba...")
        pass # Não faz nada, pois vai criar a aba na próxima etapa

    # Prepara os dados para envio (cabeçalhos + linhas de dados)
    headers = df_consolidado.columns.tolist()
    data_to_send = [headers] + df_consolidado.values.tolist()

    # Garante que a aba tem tamanho suficiente (mínimo de 1 linha para cabeçalho)
    rows_count = max(len(data_to_send), 1) 
    cols_count = len(headers) if headers else 1 # Garante pelo menos 1 coluna

    # Cria a nova aba com os dados
    wks = sh.add_worksheet(title=NOME_ABA, rows=rows_count, cols=cols_count)
    print(f"INFO: Nova aba '{NOME_ABA}' criada com {rows_count} linhas e {cols_count} colunas.")

    # Envia os dados para a planilha
    wks.update(data_to_send)

    print(f'SUCESSO: Planilha "{NOME_ABA}" atualizada com {len(df_consolidado)} registros filtrados de inovação!')
    print("INFO: Script triagem_inovacao.py finalizado.")

except gspread.exceptions.SpreadsheetNotFound:
    print(f"ERRO FATAL: Planilha com ID '{SHEET_ID}' NÃO encontrada. Verifique o ID e as permissões de compartilhamento da conta de serviço.", file=sys.stderr)
    sys.exit(1)
try:
    # código
except gspread.exceptions.NoValidUrlKeyFound:
    print('INFO: Script update_tede_csv.py concluído.')
