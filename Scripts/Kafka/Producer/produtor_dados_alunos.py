from kafka import KafkaProducer
import json
from faker import Faker
from datetime import datetime
from dateutil.relativedelta import relativedelta
import random

server = '10.0.0.4:9092'  # Substitua pelo endereço IP e porta corretos do Apache Kafka
topic = 'dados-alunos'  # Substitua pelo nome do tópico
topic_dados_alunos_cursos_disciplinas = 'dados-alunos-curso-disciplinas'  # Substitua pelo nome do tópico
topic_dados_alunos_cursos_disciplinas_atividade = 'dados-alunos-curso-disciplinas-atividades'  # Substitua pelo nome do tópico

dados_alunos = []
dados_alunos_cursos_disciplinas = []
dados_alunos_cursos_disciplinas_atividades = []
def gerar_dados_alunos():
    # Função para gerar dados fake usando a biblioteca Faker
    fake = Faker('pt_BR')
    for i in range(100):
        matricula = fake.random_int(min=100000, max=999999)
        data_nascimento = fake.date_between_dates(date_start=datetime(1997, 1, 1), date_end=datetime(2006, 12, 31)).strftime('%d/%m/%Y')
        data_ingresso = datetime.strptime(data_nascimento, '%d/%m/%Y') + relativedelta(years=18)
        codigo_curso = fake.random_int(min=1, max=5)
        previsao_conclusao = calcula_previsao_conclusao(codigo_curso, data_ingresso.strftime('%d/%m/%Y'))
        dado_fake = {
            'Matricula': matricula,
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
        print(f'dado_fake_aluno {i} {str(dado_fake)}')
        dados_alunos.append(dado_fake)

        ano_min = data_ingresso.year
        ano_max = datetime.strptime(previsao_conclusao, '%d/%m/%Y').year

        cursos = get_dados_curso()
        disciplinasPorPeriodo = cursos['DisciplinasPorPeriodo']
        periodos = cursos['Periodos']

        qtdDisciplinas = disciplinasPorPeriodo*periodos

        for d in range(qtdDisciplinas):
            codigo_disciplina = fake.random_int(min=1, max=173)
            periodo = str("%02d" % (fake.random_int(min=1, max=12),)) + '/' + str(fake.random_int(min=ano_min, max=ano_max))
            notaTotal = 0

            for a in range(4):
                nota = fake.random_int(min=0, max=25)
                dado_aluno_curso_disciplina_atividade = {
                    'CodigoCurso': codigo_curso,
                    'CodigoDisciplina': codigo_disciplina,
                    'Periodo': periodo,
                    'Matricula': matricula,
                    'Nota': nota,
                    'NomeAtividade': fake.random_element(elements=('Exercicio', 'Trabalho', 'Avaliação')),
                    'ValorAtividade': 25
                }
                notaTotal += nota
                dados_alunos_cursos_disciplinas_atividades.append(dado_aluno_curso_disciplina_atividade)

            
            dado_aluno_curso_disciplina = {
                'CodigoCurso': codigo_curso,
                'CodigoDisciplina': codigo_disciplina,
                'Periodo': periodo,
                'Matricula': matricula,
                'Nota': notaTotal,
                'Frequencia': fake.random_int(min=0, max=100),
                'Status': fake.random_element(elements=('Aprovado', 'Reprovado', 'Exame Especial')),
                'MotivoReprovacao': fake.random_element(elements=('Nota', 'Frequencia'))
            }
            dados_alunos_cursos_disciplinas.append(dado_aluno_curso_disciplina)
    
    return dados_alunos

def enviar_dados_kafka(dados):
    producer = KafkaProducer(bootstrap_servers=server, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for dado in dados:
        producer.send(topic, dado)
    producer.flush()
    print('dados enviados ao tópico')


def enviar_dados_alunos_cursos_disciplinas_kafka(dados):
    producer = KafkaProducer(bootstrap_servers=server, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for dado in dados:
        producer.send(topic_dados_alunos_cursos_disciplinas, dado)
    producer.flush()
    print('dados enviados ao tópico topic_dados_alunos_cursos_disciplinas')


def enviar_dados_alunos_cursos_disciplinas_atividade_kafka(dados):
    producer = KafkaProducer(bootstrap_servers=server, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for dado in dados:
        producer.send(topic_dados_alunos_cursos_disciplinas_atividade, dado)
    producer.flush()
    print('dados enviados ao tópico topic_dados_alunos_cursos_disciplinas')

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


dados_alunos = gerar_dados_alunos()
enviar_dados_kafka(dados_alunos)
enviar_dados_alunos_cursos_disciplinas_kafka(dados_alunos_cursos_disciplinas)
enviar_dados_alunos_cursos_disciplinas_atividade_kafka(dados_alunos_cursos_disciplinas_atividades)