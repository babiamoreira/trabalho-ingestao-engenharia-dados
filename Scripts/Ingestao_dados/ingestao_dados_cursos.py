import json
import pandas as pd
from azure.storage.blob import BlobClient
from datetime import datetime
import time

cadeia_conexao = ''
nome_container = 'datalake-dados-faculdade'

# Parametro de data para encerrar o loop se novas mensagens não forem visualizadas
last_message_time = time.time()

dados_cursos = []

def recupera_dados_cursos():
    return [
            '{"NomeCurso": "ENGENHARIA DA COMPUTAÇÃO" ,"CodigoCurso": 1,"Periodos": 10,"CargaHorariaPrevistaPorDisciplina": 92,"DisciplinasPorPeriodo": 8,"MediaDeAlunosPorTurma": 50}',
            '{"NomeCurso": "CIÊNCIA DA COMPUTAÇÃO","CodigoCurso": 2,"Periodos": 8,"CargaHorariaPrevistaPorDisciplina": 64,"DisciplinasPorPeriodo": 8,"MediaDeAlunosPorTurma": 62}',
            '{"NomeCurso": "SISTEMA DE INFORMAÇÕES","CodigoCurso": 3,"Periodos": 8,"CargaHorariaPrevistaPorDisciplina": 64,"DisciplinasPorPeriodo": 7,"MediaDeAlunosPorTurma": 48}',
            '{"NomeCurso": "JOGOS DIGITAIS","CodigoCurso": 4,"Periodos": 6,"CargaHorariaPrevistaPorDisciplina": 48,"DisciplinasPorPeriodo": 6,"MediaDeAlunosPorTurma": 32}',
            '{"NomeCurso": "TECNÓLOGO EM REDES DIGITAIS","CodigoCurso": 5,"Periodos": 4,"CargaHorariaPrevistaPorDisciplina": 48,"DisciplinasPorPeriodo": 6,"MediaDeAlunosPorTurma": 28}'
    ]

def monta_dados_curso():
    lista_curso = recupera_dados_cursos()

    for curso in lista_curso:
        json_data = json.loads(curso)
        dados_cursos.append(json_data)
            
    # Cria um DataFrame do Pandas com os dados lidos
    df = pd.DataFrame(dados_cursos)
    
    arquivo_local = f'/usr/local/datasets/dados_cursos.csv'
    arquivo_datalake = f'raw/dados_cursos/dados_cursos.csv'
    
    # Salva o DataFrame em um arquivo CSV
    df.to_csv(arquivo_local, index=False, sep=',')

    # Salva o arquivo no Data lake
    blob = BlobClient.from_connection_string(conn_str=cadeia_conexao, container_name=nome_container, blob_name=arquivo_datalake)
    with open(arquivo_local, "rb") as data:
      blob.upload_blob(data, overwrite = True)
  
monta_dados_curso()