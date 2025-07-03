import pandas as pd
import glob
import os
import json
import gspread
from google.oauth2.service_account import Credentials

# Palavras-chave relevantes
PALAVRAS = [
    "inova", "patente", "tecnolog", "protótipo", "produto", "process", "algoritmo", "método",
    "síntese", "software", "dispositivo", "bioativo", "aplica", "automa", "engenharia",
    "startup", "spin-off", "modelo", "composto", "diagnóstic", "biomaterial", "nano", "encapsul", "inteligência artificial"
]
REGEX = '|'.join(PALAVRAS)

arquivos = glob.glob('csvs/*.csv')
df_final = []

for arquivo in arquivos:
    df = pd.read_csv(arquivo, dtype=str)
    for campo in ['titulo', 'palavras_chave', 'resumo']:
        if campo not in df.columns:
            df[campo] = ''
    filtro = (
        df['titulo'].str.contains(REGEX, case=False, na=False) |
        df['palavras_chave'].str.contains(REGEX, case=False, na=False) |
        df['resumo'].str.contains(REGEX, case=False, na=False)
    )
    selecionados = df[filtro].copy()
    if not selecionados.empty:
        selecionados['arquivo_origem'] = os.path.basename(arquivo)
        df_final.append(selecionados)

# Consolida os resultados
if df_final:
    df_consolidado = pd.concat(df_final, ignore_index=True)
else:
    # Se nenhum encontrado, cria DataFrame vazio com as colunas esperadas
    df_consolidado = pd.DataFrame(columns=[
        'ano', 'semestre', 'data_base', 'titulo', 'autor', 'orientador', 'curso', 'palavras_chave', 'resumo', 'link', 'arquivo_origem'
    ])

# ------- INÍCIO DA PARTE DE EXPORTAÇÃO PARA GOOGLE SHEETS -------

# Lê as credenciais do Secret do GitHub
creds_dict = json.loads(os.environ['GOOGLE_CREDS'])
creds = Credentials.from_service_account_info(creds_dict, scopes=[
    "https://www.googleapis.com/auth/spreadsheets"
])
gc = gspread.authorize(creds)

# Substitua pelo ID da sua planilha
SHEET_ID = "1Ft0IDKEgRe5HnyPPtmgdicK_xrmrQYTBctGs0glS5aI"
NOME_ABA = "Filtro"  # Nome da aba que será criada/substituída

sh = gc.open_by_key(SHEET_ID)

# Remove a aba antiga se existir
try:
    wks = sh.worksheet(NOME_ABA)
    sh.del_worksheet(wks)
except gspread.exceptions.WorksheetNotFound:
    pass

# Cria a nova aba com os dados
wks = sh.add_worksheet(title=NOME_ABA, rows=str(len(df_consolidado)+1), cols=str(len(df_consolidado.columns)))
wks.update([df_consolidado.columns.values.tolist()] + df_consolidado.values.tolist())

print(f'Planilha {NOME_ABA} atualizada com {len(df_consolidado)} registros!')
