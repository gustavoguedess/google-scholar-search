#"coelho silvestre" and "curitiba" and "alimento"
#%%
from scholarly import scholarly
import pandas as pd
from contextlib import suppress
import json
from datetime import datetime
from hashlib import sha256
import os
from tqdm import tqdm 

def pesquisa_google_scholar(termo, idioma):
    data = datetime.now().strftime('%m-%d-%Y')
    base_dados = 'Google academico'

    os.makedirs('pesquisas', exist_ok=True)

    filename = 'pesquisas/'+sha256(termo.encode()).hexdigest()+'.json'
    try:
        with open(filename, 'r') as f:
            resultados = json.loads(f.read())
    except:
        try:
            iterador = scholarly.search_pubs(termo)
            
            resultados = []
            for resultado in iterador:
                resultados.append(resultado)
        except Exception as e:
            print(f"Erro ao pesquisar termo {termo} - {e}")

        with open(filename, 'w') as f:
            f.write(json.dumps(resultados, indent=2))
    
    publicacoes_list = []
    for resultado in tqdm(resultados):
        bib = resultado['bib']

        publicacao = {}

        with suppress(KeyError): publicacao['Data'] = data
        with suppress(KeyError): publicacao['Base de Dados'] = base_dados
        with suppress(KeyError): publicacao['Idioma'] = idioma
        with suppress(KeyError): publicacao['Termos'] = termo
        with suppress(KeyError): publicacao['Titulo'] = bib['title']
        with suppress(KeyError): publicacao['Resumo'] = bib['abstract']
        with suppress(KeyError): publicacao['Ano de Publicação'] = bib['pub_year']
        with suppress(KeyError): publicacao['Autores'] = ', '.join(bib['author'])
        with suppress(KeyError): publicacao['Publicação'] = resultado['eprint_url']
        with suppress(KeyError): publicacao['Repositório'] = resultado['pub_url']
        with suppress(KeyError): publicacao['Rank do Google'] = resultado['gsrank']
        with suppress(KeyError): publicacao['Qtde de Citações'] = resultado['num_citations']


        publicacoes_list.append(publicacao)
    
    return publicacoes_list

df_pesquisa = pd.read_csv('pesquisa.csv')

publicacoes = []
for row in df_pesquisa.iterrows():
    idioma, pesquisa = row[1]
    print(f"Pesquisando termo {idioma}: {pesquisa}")
    pubs = pesquisa_google_scholar(pesquisa, idioma)
    publicacoes.extend(pubs)

df = pd.DataFrame(publicacoes)
df

#%% download pdf in parallel
import requests
from multiprocessing.pool import ThreadPool

def download_file(url):
    try:
        filename = 'publicacoes/'+sha256(url.encode()).hexdigest()+'.pdf'
        with open(filename, 'rb') as f:
            return
    except:
        try:
            response = requests.get(url, stream=True, verify=False, timeout=10)
            with open(filename, 'wb') as f:
                f.write(response.content)
        except:
            pass

os.makedirs('publicacoes', exist_ok=True)

pool = ThreadPool(8)
print('BAIXANDO ARQUIVOS...')
pool.map(download_file, df['Publicação'].values)
print('ARQUIVOS BAIXADOS!!!')
#%%
from pypdf import PdfReader

tqdm.pandas()

def get_tipo_publicacao(url):
    # check if url is nan
    if not isinstance(url, str): return ''

    if 'article' in url or 'artigo' in url:
        return 'Artigo'
    
    if 'riunet' in url:
        return 'Artigo'
    
    try:
        filename = 'publicacoes/'+sha256(url.encode()).hexdigest()+'.pdf'
        print(filename)
        pdf = PdfReader(open(filename, 'rb'))
    except:
        if 'core.ac' in url:
            return 'TCC'
        if 'researchgate' in url:
            return 'Artigo'
        if 'scielo' in url:
            return 'Artigo'
        if 'academia.edu' in url:
            return 'Indisponível'
        if 'repositor' in url:
            return 'Tese'
        
        return ''
    

    tcc_termos = [
        'trabalho de conclusão de curso',
        'tcc',
        'tonografia',
        'trabajo final de carrera',
        'trabajo de graduación',
        'proyecto de investigación',
        'trabajo de titulación',
        'trabajo de grado',
    ]
    tese_termos = [
        'tese',
        'tesis'
        'thesis ',
        ' thesis',
        'master',
        'magister',
    ]
    dissertacao_termos = [
        'dissertação',
        'dissertation',
        'doctorat',
    ]
    livro_termos = [
        'livro',
        'book',
        'libro',
        'livre'
    ]
    anais_termos = [
        'anais',
        'reunión',
        # 'Proceedings',
    ]
    artigo_termos = [
        'artigo',
        'article',
        'artículo',
        'paper',
    ]
    artigo_revista_termos = [
        'journal',
        'journeé',
        'journées',
    ]
    revista_termos = [
        'revista',
        'journal',
    ]

    try:
        for page in pdf.pages[:5]:
            text = page.extract_text().lower()
            for termo in tcc_termos:
                if termo in text:
                    return 'TCC'
            for termo in tese_termos:
                if termo in text:
                    return 'Tese'
            for termo in dissertacao_termos:
                if termo in text:
                    return 'Dissertação'
            for termo in livro_termos:
                if termo in text:
                    return 'Livro'
            for termo in anais_termos:
                if termo in text:
                    return 'Anais'
            for termo in artigo_termos:
                if termo in text:
                    return 'Artigo'
            for termo in artigo_revista_termos:
                if termo in text and len(pdf.pages) < 30:
                    return 'Artigo'
            for termo in revista_termos:
                if termo in text and len(pdf.pages) > 30:
                    return 'Revista'
    except:
        pass

    if 'core.ac' in url:
        return 'TCC'
    if 'researchgate' in url:
        return 'Artigo'
    if 'scielo' in url:
        return 'Artigo'
    if 'academia.edu' in url:
        return 'Indisponível'
    if 'repositor' in url:
        return 'Tese'

    return ''

get_tipo_publicacao('https://www.nature.com/articles/s41598-023-27911-x')

#%%
df['Tipo de Documento'] = df['Publicação'].progress_apply(get_tipo_publicacao)

# Se não tiver publicação, é citação
sem_publicacao = df['Publicação'].isna()
df.loc[sem_publicacao, 'Tipo de Documento'] = 'Citação'

df.groupby('Idioma')['Tipo de Documento'].value_counts()

#%%

df.groupby('Idioma')['Tipo de Documento'].value_counts()

#%% Identificar Lingua

# from langdetect import detect
# df['Resumo'] = df['Resumo'].fillna('')
# df['Idioma'] = df['Resumo'].apply(lambda x: detect(x) if x else '')
# df

#%%
print('TRADUZINDO...')
from deep_translator import GoogleTranslator


df['Tradução'] = df['Titulo'].progress_apply(lambda x: GoogleTranslator(source='auto', target='pt').translate(x) if x else '')

print('TRADUÇÃO CONCLUÍDA')
df[['Titulo', 'Tradução']]
#%%
df
#%%
print('ORGANIZANDO COLUNAS...', end=' ')
columns = df.columns.tolist()
new_columns_order = [
    'Data',
    'Base de Dados',
    'Idioma',
    'Termos',
    'Titulo',
    'Tradução',
    'Resumo',
    'Ano de Publicação',
    'Autores',
    'Publicação',
    'Repositório',
    'Rank do Google',
    'Qtde de Citações',
    'Tipo de Documento',
]
print('OK!!')
df[new_columns_order]

# %%
df.to_excel('pesquisa_google_scholar_2.xlsx', index=False)

# %%
