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
        # Tenta formatos mais comuns do OAI-PMH
        if 'T' in data_str: # Ex: 2023-01-15T10:30:00Z
            dt = datetime.strptime(data_str[:10], '%Y-%m-%d')
        elif '-' in data_str and len(data_str) == 10: # Ex: 2023-01-15
            dt = datetime.strptime(data_str, '%Y-%m-%d')
        elif len(data_str) == 4: # Ex: 2023 (apenas ano)
            # Para apenas o ano, considere o primeiro semestre por padrão para fins de categorização
            return data_str, '1' 
        else: # Formato não reconhecido
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
                # O contributor pode ter vários tipos, filtraremos por aqueles que não são CPF ou URL
                'orientador': '; '.join([c for c in md.get('contributor', []) if c and not str(c).startswith('CPF') and not str(c).startswith('http')]),
                'datas': [d for d in md.get('date', []) if d is not None],
                'curso': '; '.join([c for c in md.get('publisher', []) if c is not None]),
                'palavras_chave': '; '.join([p for p in md.get('subject', []) if p is not None]),
                'resumo': md.get('description', [''])[0] if md.get('description', []) else '',
                'link': next((id for id in md.get('identifier', []) if id and str(id).startswith('http')), '') # Pega o primeiro identifier que é uma URL
            })
        record_count += len(current_records)
        print(f"INFO: Coletado o primeiro lote de {len(current_records)} registros. Total acumulado: {record_count}")

        # Continua buscando lotes com o resumptionToken
        while resumption_token is not None and resumption_token.token:
            print(f"INFO: Buscando próximo lote com resumptionToken: {resumption_token.token} (Total atual: {record_count})")
            time.sleep(0.5) # Pequeno delay para não sobrecarregar o servidor
            response = sickle.ListRecords(resumptionToken=resumption_token.token)
            current_records = response.records
            resumption_token = response.resumption_token
            
            for record in current_records:
                md = record.metadata
                all_records_data.append({
                    'titulos': '; '.join([t for t in md.get('title', []) if t is not None]),
                    'autor': '; '.join([a for a in md.get('creator', []) if a is not None]),
                    'orientador': '; '.join([c for c in md.get('contributor', []) if c and not str(c).startswith('CPF') and not str(c).startswith('http')]),
                    'datas': [d for d in md.get('date', []) if d is not None],
                    'curso': '; '.join([c for c in md.get('publisher', []) if c is not None]),
                    'palavras_chave': '; '.join([p for p in md.get('subject', []) if p is not None]),
                    'resumo': md.get('description', [''])[0] if md.get('description', []) else '',
                    'link': next((id for id in md.get('identifier', []) if id and str(id).startswith('http')), '')
                })
            record_count += len(current_records)
            print(f"INFO: Coletados {len(current_records)} registros neste lote. Total acumulado: {record_count}")

        print(f"INFO: Coleta OAI-PMH concluída. Total de registros coletados: {record_count}")
        return all_records_data

    except Exception as e:
        print(f"ERRO FATAL na coleta OAI-PMH: {e}", file=sys.stderr)
        return []

# --- Processamento dos Dados ---
def processar_dados(raw_data):
    processed_data = []
    for item in raw_data:
        data_base = item['datas'][-1] if item['datas'] else ''
        ano, semestre = ano_semestre(data_base)

        processed_data.append({
            'ano': ano or '',
            'semestre': semestre or '',
            'data_base': data_base,
            'titulo': item['titulos'],
            'autor': item['autor'],
            'orientador': item['orientador'],
            'curso': item['curso'],
            'palavras_chave': item['palavras_chave'],
            'resumo': item['resumo'],
            'link': item['link']
        })
    return pd.DataFrame(processed_data)

# --- Lógica Principal ---
if __name__ == "__main__":
    print("INFO: Iniciando update_tede_csv.py")
    raw_collected_data = coletar_registros_oai()
    
    if not raw_collected_data:
        print("Nenhum dado coletado. Encerrando o processo de geração de CSVs.", file=sys.stderr)
        sys.exit(1)

    df = processar_dados(raw_collected_data)
    df = df[df['ano'] != ''] # Remove linhas sem ano definido

    # Filtragem por data (mesmo semestre do ano anterior ao atual)
    now = datetime.now()
    ano_atual = now.year
    mes_atual = now.month
    ano_limite = ano_atual - 1
    semestre_limite = '1' if mes_atual <= 6 else '2' # Mês de Julho (7) em diante é 2º semestre

    print(f"INFO: Filtrando dados para incluir até o semestre {ano_limite}_S{semestre_limite}...")
    df['ano_int'] = pd.to_numeric(df['ano'], errors='coerce')
    df['semestre_int'] = pd.to_numeric(df['semestre'], errors='coerce')
    df = df.dropna(subset=['ano_int', 'semestre_int']) # Remove linhas onde ano_int ou semestre_int são NaN

    df_filtrado_por_data = df[
        (df['ano_int'] < ano_limite) |
        ((df['ano_int'] == ano_limite) & (df['semestre_int'] <= int(semestre_limite)))
    ].copy()

    df_filtrado_por_data = df_filtrado_por_data.drop(columns=['ano_int', 'semestre_int'])

    # Garante que a pasta de saída existe
    os.makedirs(PASTA_CSV_SAIDA, exist_ok=True)
    print(f"INFO: Pasta de saída '{PASTA_CSV_SAIDA}' verificada/criada.")

    # Geração dos arquivos CSV
    print(f'INFO: Gerando arquivos CSV por semestre na pasta "{PASTA_CSV_SAIDA}"...')
    gerados_count = 0
    for (ano, semestre), grupo in df_filtrado_por_data.groupby(['ano', 'semestre']):
        if not ano or not semestre:
            print(f"ALERTA: Ignorando grupo com ano/semestre inválido: {ano}_S{semestre}", file=sys.stderr)
            continue
        filename = os.path.join(PASTA_CSV_SAIDA, f'tede_uepb_{ano}_S{semestre}.csv')
        
        # Consideração: Seu código original pulava se o arquivo existia.
        # Para automação semestral, você PODE QUERER sobrescrever para garantir dados mais recentes.
        # Se quiser sobrescrever, remova o 'if os.path.exists(filename):'
        # Se quiser manter a lógica de não sobrescrever, mantenha como está.
        # Por enquanto, vou manter a lógica original.
        if os.path.exists(filename):
            print(f'ALERTA: Arquivo já existe: {filename}. Pulando geração para evitar sobreposição.', file=sys.stderr)
        else:
            grupo.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f'SUCESSO: Gerado: {filename} com {len(grupo)} registros')
            gerados_count += 1

    print(f'INFO: Processo de geração de CSVs concluído! Total de arquivos gerados: {gerados_count}.')
    print(f'INFO: Os CSVs estão na pasta "{PASTA_CSV_SAIDA}"')
    
    print(f"DEBUG: Listando arquivos na pasta '{PASTA_CSV_SAIDA}':")
    for root, dirs, files in os.walk(PASTA_CSV_SAIDA):
        for file in files:
            print(os.path.join(root, file))
    print("INFO: Script update_tede_csv.py finalizado.")
