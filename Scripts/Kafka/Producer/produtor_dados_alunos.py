from kafka import KafkaProducer
import json
from faker import Faker
from datetime import datetime
from dateutil.relativedelta import relativedelta
import random

server = '10.0.0.4:9092'  # Substitua pelo endereço IP e porta corretos do Apache Kafka
topic = 'dados-alunos'  # Substitua pelo nome do tópico

def gerar_dados_fake():
    # Função para gerar dados fake usando a biblioteca Faker
    fake = Faker('pt_BR')
    dados_fake = []
    for _ in range(100):
        data_nascimento = fake.date_between_dates(date_start=datetime(1980, 1, 1), date_end=datetime(2006, 12, 31)).strftime('%d/%m/%Y')
        data_ingresso = datetime.strptime(data_nascimento, '%d/%m/%Y') + relativedelta(years=18)
        codigo_curso = fake.random_int(min=1, max=5)
        previsao_conclusao = calcula_previsao_conclusao(codigo_curso, data_ingresso.strftime('%d/%m/%Y'))
        dado_fake = {
            'Matricula': fake.random_int(min=100000, max=999999),
            'Nome': fake.name(),
            'Sexo': fake.random_element(elements=('M', 'F')),
            'CadastroGeralPessoa': fake.cpf(),
            'DataNascimento': data_nascimento,
            'Email': fake.email(),
            'Endereco': fake.address(),
            'TelefoneCelular': fake.phone_number(),
            'Curso': codigo_curso,
            'DataIngresso': data_ingresso.strftime('%d/%m/%Y'),
            'DataPrevistaDeConclusao': previsao_conclusao,
            'DataConclusao': calcula_data_conclusao(previsao_conclusao),
            'StatusMatricula': fake.random_element(elements=('Ativo', 'Trancado', 'Cancelado'))
        }
        dados_fake.append(dado_fake)
    
    return dados_fake

def enviar_dados_kafka(dados):
    producer = KafkaProducer(bootstrap_servers=server, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for dado in dados:
        producer.send(topic, dado)
    producer.flush()

def calcula_data_conclusao(data_prevista_conclusao):
    meses_adicionais = random.choice(range(0, 37, 6))
    data_conclusao_dt = datetime.strptime(data_prevista_conclusao, "%d/%m/%Y") + relativedelta(months=meses_adicionais)
    return data_conclusao_dt.strftime("%d/%m/%Y")

def calcula_previsao_conclusao(codigo_curso, data_ingresso):
    cursos = get_dados_curso()
    if codigo_curso in cursos:
        curso = cursos[codigo_curso]
        periodos = curso["Periodos"]
        duracao_curso_meses = periodos * 6
        if isinstance(data_ingresso, str):
            data_ingresso = datetime.strptime(data_ingresso, "%d/%m/%Y")
        data_conclusao_dt = data_ingresso + relativedelta(months=duracao_curso_meses)
        return data_conclusao_dt.strftime("%d/%m/%Y")
    else:
        return "Código de curso inválido"

def get_dados_curso():
    return {
        1: {"NomeCurso": "ENGENHARIA DA COMPUTAÇÃO", "Periodos": 10, "CargaHorariaPrevistaPorDisciplina": 92, "DisciplinasPorPeriodo": 8, "MediaDeAlunosPorTurma": 50},
        2: {"NomeCurso": "CIÊNCIA DA COMPUTAÇÃO", "Periodos": 8, "CargaHorariaPrevistaPorDisciplina": 64, "DisciplinasPorPeriodo": 8, "MediaDeAlunosPorTurma": 62},
        3: {"NomeCurso": "SISTEMA DE INFORMAÇÕES", "Periodos": 8, "CargaHorariaPrevistaPorDisciplina": 64, "DisciplinasPorPeriodo": 7, "MediaDeAlunosPorTurma": 48},
        4: {"NomeCurso": "JOGOS DIGITAIS", "Periodos": 6, "CargaHorariaPrevistaPorDisciplina": 48, "DisciplinasPorPeriodo": 6, "MediaDeAlunosPorTurma": 32},
        5: {"NomeCurso": "TECNÓLOGO EM REDES DIGITAIS", "Periodos": 4, "CargaHorariaPrevistaPorDisciplina": 48, "DisciplinasPorPeriodo": 6, "MediaDeAlunosPorTurma": 28}
    }

