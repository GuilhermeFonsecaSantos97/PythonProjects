''' Essa biblioteca tem como objetivo registrar todas as principais funções que vou utilizar nos meus códigos py'''
''' 
O script atual vai pegar a data de atualização dos dados da ONU, depois vai comparar com a data atual 
e se estiver atualizado vai criar um arquivo XML e vai enviar esse arquivo via Slack. Caso não tenha atualizado 
vai mandar uma mensagem no slack avisando
'''

import selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import pandas as pd
from datetime import datetime
import csv
import slack
import os
from dotenv import load_dotenv
from pathlib import Path
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import html
import re
import psycopg2
from fuzzywuzzy import fuzz


#Configurando selenium
chrome_options = webdriver.ChromeOptions()
driver = webdriver.Chrome(options=chrome_options)

#Setando variáveis
env_path = r'C:\Users\guilherme.fonseca_aa\Documents\codigos_py\environment_variables.env'
load_dotenv(dotenv_path=env_path)
token = os.environ['TOKEN']
chrome_options.binary_location = r'C:\Program Files\Google\Chrome\Application\chrome.exe'
url_onu = 'https://www.un.org/securitycouncil/content/un-sc-consolidated-list'
url_api_slack = 'https://slack.com/api/files.upload'
headers = {
    'Authorization': f'Bearer {token}'
}
url_xml = 'https://scsanctions.un.org/resources/xml/en/consolidated.xml'
data_atual = datetime.now().date()
formato_data = "%d %B %Y"
output_path_xml = r'G:\Drives compartilhados\ONU\dados_onu_xml'
output_path_csv = r'G:\Drives compartilhados\ONU\dados_onu_csv'
# Defina as informações de conexão ao Amazon Redshift
host = 'data-redshift-01.czk0xq19itgi.us-east-1.redshift.amazonaws.com'
port = 5439
database = 'dl_bronze'
user = 'guilherme.fonseca'
password = '6YKOLyUOUP6gyXvdqj40EdWYHqUm7GFm'

def remover_caracteres_especiais(texto):
    return re.sub(r'[^A-Za-z ]', '', texto)

def encontrar_maior_data_no_diretorio(diretorio):
    # Lista todos os arquivos no diretório
    arquivos = os.listdir(diretorio)

    # Expressão regular para encontrar datas nos nomes dos arquivos
    padrao_data = r'dados_onu_(\d{4}-\d{2}-\d{2}).xml'

    maior_data = None

    for arquivo in arquivos:
        # Use a expressão regular para encontrar a data no nome do arquivo
        correspondencia = re.search(padrao_data, arquivo)
        if correspondencia:
            data_str = correspondencia.group(1)
            data = datetime.strptime(data_str, '%Y-%m-%d').date()
            if not maior_data or data > maior_data:
                maior_data = data

    print("A última data de atualização dos dados da ONU baixada foi: {}".format(maior_data))            

    return maior_data

def calcular_similaridades_e_salvar_csv(blacklist_onu_path, partners_file_path, caminho_saida_csv, limite_similaridade=70):
    # Função para calcular a similaridade entre dois nomes
    def calcular_similaridade(nome, parceiro):
        return fuzz.ratio(nome, parceiro)

    # Leitura dos arquivos CSV
    df_blacklist_onu = pd.read_csv(blacklist_onu_path)
    df_parceiros = pd.read_csv(partners_file_path)

    # Dicionário para armazenar a maior similaridade e o nome do parceiro correspondente
    maior_similaridade = {'nome_blacklist_onu': [], 'maior_similaridade': [], 'parceiro_aarin': []}

    # Loop para calcular a similaridade entre cada nome e parceiro
    for index_nomes, row_nomes in df_blacklist_onu.iterrows():
        nome = row_nomes['BLACKLIST_ONU']
        maior = 0
        nome_parceiro = ''
        
        for index_parceiros, row_parceiros in df_parceiros.iterrows():
            parceiro = row_parceiros['DEFAULT']
            similaridade = calcular_similaridade(nome, parceiro)
            if similaridade > maior:
                maior = similaridade
                nome_parceiro = parceiro

        # Adicione os resultados ao dicionário
        maior_similaridade['nome_blacklist_onu'].append(nome)
        maior_similaridade['maior_similaridade'].append(maior)
        maior_similaridade['parceiro_aarin'].append(nome_parceiro)

    # Crie um novo DataFrame com os resultados
    df_resultado = pd.DataFrame(maior_similaridade)

    # Filtrar resultados com base no limite de similaridade
    df_resultado_filtrado = df_resultado[df_resultado['maior_similaridade'] > limite_similaridade]

    # Salve o DataFrame em um arquivo CSV
    df_resultado_filtrado.to_csv(caminho_saida_csv, index=False)

    print("A similaridade foi calculada e o arquivo já está disponível em: {}".format(caminho_saida_csv))


def obter_data_onu(url_onu, formato_data):

    # Acesse a URL da ONU
    driver.get(url_onu)

    # Encontre o elemento desejado na página usando XPath
    elemento = driver.find_element(By.XPATH, '/html/body/div[3]/div/section/div/div/div/div/div/div/div/div/div/div/p[8]/strong')

    # Obtenha o texto do elemento
    data_ultima_atualizacao = elemento.text

    # Converta a data de atualização da ONU para o formato desejado
    data_atualizacao_onu = datetime.strptime(data_ultima_atualizacao, formato_data).date()

    # Feche o navegador
    driver.quit()

    print("A data de atualização da blacklist da onu foi extraída com sucesso: {}".format(data_atualizacao_onu))

    # Retorne um tupla com os valores
    return data_atualizacao_onu

def criar_arquivo_xml(output_path, data_atualizacao_onu):
    # Configure o driver do Selenium (por exemplo, para o Chrome)
    driver = webdriver.Chrome()

    url = 'https://scsanctions.un.org/resources/xml/en/consolidated.xml'

    # Abra a página da web
    driver.get(url)

    # Role a página para baixo para carregar todo o conteúdo
    driver.find_element(By.TAG_NAME, 'html').send_keys(Keys.END)

    # Aguarde um tempo para garantir que o conteúdo seja carregado
    import time
    time.sleep(15)  # Pode ajustar o tempo de espera conforme necessário

    # Obtenha o conteúdo da página
    data = driver.page_source

    # Feche o navegador
    driver.quit()

    # Crie o elemento raiz do XML
    root = ET.Element('conteudo')

    # Crie um elemento para armazenar o conteúdo da página
    content_element = ET.SubElement(root, 'pagina_content')
    content_element.text = data

    # Crie uma árvore XML
    tree = ET.ElementTree(root)

    # Especifique o nome do arquivo XML (incluindo a data atual)
    output_filename = 'dados_onu_{}.xml'.format(data_atualizacao_onu)

    # Crie o caminho completo para o arquivo
    output_xml_file_path = os.path.join(output_path, output_filename)

    # Salve a árvore XML em um arquivo
    tree.write(output_xml_file_path, encoding='utf-8', xml_declaration=True)

    print("Arquivo XML criado e populado com sucesso em:", output_xml_file_path)

    return output_xml_file_path
    
def extrair_nomes_e_salvar_csv(file_path_xml, file_path_to_create_csv):

    # Abra o arquivo TXT e leia o conteúdo
    with open(file_path_xml, 'r', encoding='utf-8') as file:
        data = file.read()

    # Decodifique os caracteres HTML
    decoded_data = html.unescape(data)

    # Use BeautifulSoup para analisar o texto
    soup = BeautifulSoup(decoded_data, 'html.parser')

    # Encontre todas as tags <INDIVIDUAL>
    individual_tags = soup.find_all('individual')
    # Encontre todas as tags <ENTITY>
    entity_tags = soup.find_all('entity')

    # Inicialize uma lista para armazenar os nomes concatenados
    concatenated_names = []

    # Itere pelas tags <INDIVIDUAL> para extrair os nomes e concatená-los
    for individual_tag in individual_tags:
        first_name_tag = individual_tag.find('first_name')
        second_name_tag = individual_tag.find('second_name')
        third_name_tag = individual_tag.find('third_name')
        fourth_name_tag = individual_tag.find('fourth_name')

        first_name = remover_caracteres_especiais(first_name_tag.get_text().upper()) if first_name_tag else ""
        second_name = remover_caracteres_especiais(second_name_tag.get_text().upper()) if second_name_tag else ""
        third_name = remover_caracteres_especiais(third_name_tag.get_text().upper()) if third_name_tag else ""
        fourth_name = remover_caracteres_especiais(fourth_name_tag.get_text().upper()) if fourth_name_tag else ""

        full_name = f"{first_name} {second_name} {third_name} {fourth_name}"
        if full_name.strip():  # Verifica se o nome completo não está vazio
            concatenated_names.append(full_name.replace(';', '').strip())

        alias_name_tags = individual_tag.find_all('alias_name')
        for alias_name_tag in alias_name_tags:
            alias_name = remover_caracteres_especiais(alias_name_tag.get_text().upper())
            if alias_name.strip():  # Verifica se o alias name não está vazio
                concatenated_names.append(alias_name.replace(';', '').strip())

    for entity_tag in entity_tags:
        first_name_tag = entity_tag.find('first_name')
        alias_name_tag = entity_tag.find('alias_name')

        first_name = remover_caracteres_especiais(first_name_tag.get_text().upper()) if first_name_tag else ""
        alias_name = remover_caracteres_especiais(alias_name_tag.get_text().upper()) if alias_name_tag else ""
        if first_name.strip():
            concatenated_names.append(first_name.replace(';', '').strip())
        if alias_name.strip():
            concatenated_names.append(alias_name.replace(';', '').strip())

    # Escreva os nomes concatenados em um arquivo CSV
    with open(file_path_to_create_csv, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['BLACKLIST_ONU'])
        csv_writer.writerows([[name] for name in concatenated_names])

    print(f'Nomes foram extraídos e salvos em "{file_path_to_create_csv}".')

def enviar_mensagem_slack(texto):

    load_dotenv(dotenv_path=env_path)

    client = slack.WebClient(token=token)

    # Envie a mensagem com o texto fornecido como argumento
    client.chat_postMessage(channel='#notif-consulta_onu', text=texto)

    print("Mensagem enviada para o slack com sucesso: {}".format(texto))


def enviar_arquivo_slack(comentario_inicial, caminho_arquivo):
    params = {
        'channels': '#notif-consulta_onu',
        'initial_comment': comentario_inicial,
    }    
    files = {
        'file': open(caminho_arquivo, 'rb')
    }    
    response = requests.post(url_api_slack, headers=headers, params=params, files=files)

    print("O arquivo foi enviado para o slack com sucesso")

def executar_consulta_e_salvar_csv(partner_file_path, query):
    # Defina as informações de conexão ao Amazon Redshift
    host = 'data-redshift-01.czk0xq19itgi.us-east-1.redshift.amazonaws.com'
    port = 5439
    database = 'dl_bronze'
    user = 'guilherme.fonseca'
    password = '6YKOLyUOUP6gyXvdqj40EdWYHqUm7GFm'

    # Conecte-se ao Redshift
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password
    )

    # Execute a consulta
    cursor = conn.cursor()
    cursor.execute(query)

    # Extraia os resultados para o arquivo CSV local
    with open(partner_file_path, 'w') as f:
        for row in cursor.fetchall():
            f.write(','.join(map(str, row)) + '\n')

    # Feche a conexão
    cursor.close()
    conn.close()
    print("arquivo com parceiros criado com sucesso na pasta: {}".format(partner_file_path))
