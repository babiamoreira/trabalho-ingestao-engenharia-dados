import json
import pandas as pd
from azure.storage.blob import BlobClient
from datetime import datetime
import time
from faker import Faker

cadeia_conexao = ''
nome_container = 'datalake-dados-faculdade'

# Parametro de data para encerrar o loop se novas mensagens não forem visualizadas
last_message_time = time.time()

dados_disciplinas = []


def recupera_dados_disciplinas():
    return [
        'Introdução à Engenharia',
        'Cálculo Diferencial e Integral I',
        'Cálculo Diferencial e Integral II',
        'Álgebra Linear',
        'Física I',
        'Física II',
        'Química Geral',
        'Geometria Analítica',
        'Programação I',
        'Programação II',
        'Estruturas de Dados',
        'Algoritmos e Complexidade',
        'Arquitetura de Computadores',
        'Sistemas Operacionais',
        'Banco de Dados',
        'Redes de Computadores',
        'Engenharia de Software',
        'Teoria da Computação',
        'Compiladores',
        'Sistemas Digitais',
        'Circuitos Elétricos',
        'Eletrônica Analógica',
        'Eletrônica Digital',
        'Microcontroladores',
        'Processamento de Sinais',
        'Inteligência Artificial',
        'Aprendizado de Máquina',
        'Visão Computacional',
        'Robótica',
        'Sistemas Embarcados',
        'Computação Gráfica',
        'Análise de Sistemas',
        'Segurança da Informação',
        'Criptografia',
        'Projeto de Sistemas',
        'Automação Industrial',
        'Projeto de Circuitos Integrados',
        'Modelagem e Simulação',
        'Controle de Sistemas',
        'Desenvolvimento Web',
        'Desenvolvimento Mobile',
        'Computação em Nuvem',
        'Internet das Coisas (IoT)',
        'Big Data',
        'Engenharia de Requisitos',
        'Gestão de Projetos',
        'Empreendedorismo em Tecnologia',
        'Ética e Legislação em Informática',
        'Comunicação de Dados',
        'Métodos Numéricos',
        'Probabilidade e Estatística',
        'Análise de Algoritmos',
        'Teoria dos Grafos',
        'Linguagens de Programação',
        'Redes Neurais',
        'Algoritmos Genéticos',
        'Sistemas de Informação',
        'Engenharia de Controle e Automação',
        'Computação Quântica',
        'Interação Humano-Computador',
        'Programação Paralela e Distribuída',
        'Processamento de Imagens',
        'Computação Ubíqua',
        'Teoria da Informação',
        'Fundamentos de Eletromagnetismo',
        'Projeto de Sistemas de Comunicação',
        'Inteligência Computacional',
        'Engenharia de Protocolos',
        'Tópicos Especiais em Computação',
        'Análise e Projeto de Sistemas Digitais',
        'Projeto de Sistemas de Tempo Real',
        'Sistemas Multiagentes',
        'Computação Forense',
        'Redes Sem Fio',
        'Algoritmos Distribuídos',
        'Engenharia de Redes',
        'Computação Bioinspirada',
        'Computação Evolutiva',
        'Segurança em Redes de Computadores',
        'Gerenciamento de Redes',
        'Introdução à Ciência da Computação',
        'Algoritmos',
        'Matemática Discreta',
        'Lógica para Computação',
        'Complexidade de Algoritmos',
        'Pesquisa Operacional',
        'Sistemas Distribuídos',
        'Programação Paralela',
        'Data Mining',
        'Programação Funcional',
        'Programação Lógica',
        'Projeto de Software',
        'Verificação e Validação de Software',
        'Bioinformática',
        'Sistemas Inteligentes',
        'Introdução à Sistemas de Informação',
        'Fundamentos de Informática',
        'Banco de Dados I',
        'Banco de Dados II',
        'Projeto de Sistemas de Informação',
        'Gerenciamento de Projetos de TI',
        'Mineração de Dados',
        'Governança de TI',
        'Auditoria de Sistemas de Informação',
        'Inteligência de Negócios (BI)',
        'Sistemas de Suporte à Decisão',
        'Gestão de Processos de Negócios',
        'Sistemas ERP (Enterprise Resource Planning)',
        'E-commerce e E-business',
        'Gestão de Serviços de TI (ITIL)',
        'Gestão de Segurança da Informação',
        'Design de Interface e Experiência do Usuário (UI/UX)',
        'Processamento de Linguagem Natural',
        'Arquitetura Orientada a Serviços (SOA)',
        'Fundamentos de Marketing Digital',
        'Tópicos Especiais em Sistemas de Informação',
        'Introdução aos Jogos Digitais',
        'Fundamentos de Programação',
        'Estruturas de Dados e Algoritmos',
        'Matemática para Jogos',
        'Física para Jogos',
        'Design de Jogos',
        'Desenvolvimento de Jogos I',
        'Desenvolvimento de Jogos II',
        'Programação Gráfica',
        'Inteligência Artificial para Jogos',
        'Animação para Jogos',
        'Modelagem 3D',
        'Texturização e Iluminação',
        'Design de Interfaces e Experiência do Usuário (UI/UX)',
        'Narrativa e Roteiro para Jogos',
        'Áudio para Jogos',
        'Realidade Virtual e Aumentada',
        'Jogos Multijogador',
        'Design de Níveis',
        'Interface de Programação de Aplicativos (API) para Jogos',
        'Testes e Qualidade de Jogos',
        'Psicologia dos Jogos',
        'Projetos Interdisciplinares em Jogos',
        'História dos Jogos Digitais',
        'Empreendedorismo e Negócios de Jogos',
        'Economia e Monetização de Jogos',
        'Jogos Mobile',
        'Produção e Gestão de Projetos de Jogos',
        'Ética e Legislação em Jogos Digitais',
        'Design de Personagens',
        'Interface de Usuário e Experiência de Jogo',
        'Desenvolvimento de Prototótipos',
        'Desenvolvimento de Jogos em Realidade Virtual (VR)',
        'Desenvolvimento de Jogos em Realidade Aumentada (AR)',
        'Tópicos Especiais em Jogos Digitais',
        'Fundamentos de Redes de Computadores',
        'Arquitetura e Protocolos de Redes',
        'Cabeamento Estruturado',
        'Redes de Computadores I',
        'Redes de Computadores II',
        'Segurança de Redes',
        'Administração de Sistemas Operacionais',
        'Protocolos de Comunicação',
        'Tecnologias de Rede sem Fio',
        'Virtualização e Computação em Nuvem',
        'Redes Convergentes',
        'Servidores e Serviços de Rede',
        'Monitoramento e Diagnóstico de Redes',
        'Projeto e Implementação de Redes',
        'Redes Definidas por Software (SDN)',
        'Automação de Redes',
        'Criptografia e Segurança de Dados',
        'Governança de TI e Compliance',
        'Redes de Próxima Geração (5G)',
        'Implementação de VPNs e Acesso Remoto',
        'Gestão de Projetos de Redes',
        'Ética e Legislação em Redes de Computadores',

    ]

def monta_dados_disciplinas():
    lista_nome_disciplinas = recupera_dados_disciplinas()
    count = 0
    # Função para gerar dados fake usando a biblioteca Faker
    fake = Faker('pt_BR')
    for disciplina in lista_nome_disciplinas:
        count+=1
        carga_horaria = fake.random_int(min=16, max=92)
        tipo = fake.random_element(elements=('Teórica', 'Prática'))
        frequencia = carga_horaria*0.75
        dados = '{"CodigoDisciplina": count_, "NomeDisciplina": "disciplina_", "Tipo": "tipo_", "NotaDeCorte": 70, "CargaHorariaPrevista": carga_horaria,"FrequenciaMinima": frequencia_ }'
        
        dados = dados.replace('count_', str(count))
        dados = dados.replace('disciplina_', disciplina)
        dados = dados.replace('tipo_', tipo)
        dados = dados.replace('carga_horaria', str(carga_horaria))
        dados = dados.replace('frequencia_', str(frequencia))
        
        json_data = json.loads(dados)
        dados_disciplinas.append(json_data)
            
    # Cria um DataFrame do Pandas com os dados lidos
    df = pd.DataFrame(dados_disciplinas)
    
    arquivo_local = f'/usr/local/datasets/dados_disciplinas.csv'
    arquivo_datalake = f'raw/dados_disciplinas/dados_disciplinas.csv'
    
    # Salva o DataFrame em um arquivo CSV
    df.to_csv(arquivo_local, index=False, sep=',')

    # Salva o arquivo no Data lake
    blob = BlobClient.from_connection_string(conn_str=cadeia_conexao, container_name=nome_container, blob_name=arquivo_datalake)
    with open(arquivo_local, "rb") as data:
      blob.upload_blob(data, overwrite = True)
  
monta_dados_disciplinas()


