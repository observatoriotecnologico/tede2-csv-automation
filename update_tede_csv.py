# update_tede_csv.py

import os
from sickle import Sickle
import pandas as pd
from datetime import datetime

PASTA_DRIVE = 'csvs'  # Salvar localmente

sickle = Sickle('https://tede.bc.uepb.edu.br/oai/request')
records = sickle.ListRecords(metadataPrefix='oai_dc')

def ano_semestre(data_str):
    try:
        if 'T' in data_str:
            dt = datetime.strptime(data_str[:10], '%Y-%m-%d')
        elif '-' in data_str:
            dt = datetime.strptime(data_str, '%Y-%m-%d')
        elif len(data_str) == 4:
            return data_str, '1'
        else:
            return None, None
        ano = dt.year
        semestre = '1' if dt.month <= 6 else '2'
        return str(ano), semestre
    except Exception:
        return None, None

dados = []
for record in records:
    md = record.metadata
    titulos = '; '.join([t for t in md.get('title', []) if t is not None])
    autor = '; '.join([a for a in md.get('creator', []) if a is not None])
    orientador = '; '.join([c for c in md.get('contributor', []) if not c.startswith('CPF') and not c.startswith('http') and c is not None])
    datas = [d for d in md.get('date', []) if d is not None]
    data_base = datas[-1] if datas else ''
    ano, semestre = ano_semestre(data_base)
    curso = '; '.join([c for c in md.get('publisher', []) if c is not None])
    palavras_chave = '; '.join([p for p in md.get('subject', []) if p is not None])
    resumo = md.get('description', [''])[0] if md.get('description', []) and md.get('description', [''])[0] is not None else ''
    link = next((id for id in md.get('identifier', []) if id is not None and id.startswith('http')), '')
    dados.append({
        'ano': ano or '',
        'semestre': semestre or '',
        'data_base': data_base,
        'titulo': titulos,
        'autor': autor,
        'orientador': orientador,
        'curso': curso,
        'palavras_chave': palavras_chave,
        'resumo': resumo,
        'link': link
    })

df = pd.DataFrame(dados)
df = df[df['ano'] != '']

now = datetime.now()
ano_atual = now.year
mes_atual = now.month
ano_limite = ano_atual - 1
semestre_limite = '1' if mes_atual <= 6 else '2'

df['ano_int'] = pd.to_numeric(df['ano'], errors='coerce')
df['semestre_int'] = pd.to_numeric(df['semestre'], errors='coerce')
df = df.dropna(subset=['ano_int', 'semestre_int'])

df_filtrado = df[
    (df['ano_int'] < ano_limite) |
    ((df['ano_int'] == ano_limite) & (df['semestre_int'] <= int(semestre_limite)))
].copy()

df_filtrado = df_filtrado.drop(columns=['ano_int', 'semestre_int'])

os.makedirs(PASTA_DRIVE, exist_ok=True)

print(f'Gerando arquivos CSV até o semestre {ano_limite}_S{semestre_limite}...')
for (ano, semestre), grupo in df_filtrado.groupby(['ano', 'semestre']):
    if not ano or not semestre:
        continue
    filename = f'{PASTA_DRIVE}/tede_uepb_{ano}_S{semestre}.csv'
    if os.path.exists(filename):
        print(f'Arquivo já existe: {filename}. Pulando.')
    else:
        grupo.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f'Gerado: {filename} - {len(grupo)} registros')

print('Processo concluído! Arquivos CSV salvos por semestre na pasta local (até o mesmo semestre do ano anterior ao atual, sem sobreposição).')
