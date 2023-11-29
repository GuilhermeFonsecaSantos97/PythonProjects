''' 
O script atual vai pegar a data de atualização dos dados da ONU, depois vai comparar com a data atual 
e se estiver atualizado vai criar um arquivo XML e vai enviar esse arquivo via Slack. Caso não tenha atualizado 
vai mandar uma mensagem no slack avisando
'''

#importando as libs
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
import lib_do_pai as lp
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
output_path_partners = r'G:\Drives compartilhados\ONU\dados_parceiros_aarin'
query = 'select distinct upper(name) as name from aarinip_core.partners p'



data_atualizacao_onu = lp.obter_data_onu(url_onu, formato_data)
maior_data_baixada = lp.encontrar_maior_data_no_diretorio(diretorio = output_path_xml)

if data_atualizacao_onu > maior_data_baixada:
    output_xml_file_path = lp.criar_arquivo_xml(output_path = output_path_xml, data_atualizacao_onu = data_atualizacao_onu)
    csv_file_path = output_path_csv+'\\blacklist_onu_{}.csv'.format(data_atualizacao_onu)
    partner_file_path = output_path_partners+'\\partners_{}.csv'.format(data_atual)
    lp.extrair_nomes_e_salvar_csv(file_path_xml = output_xml_file_path, file_path_to_create_csv = csv_file_path)
    lp.executar_consulta_e_salvar_csv(partner_file_path = partner_file_path, query = query)
    caminho_saida_similaridade = r'G:\Drives compartilhados\ONU\similaridade_onu_parceiros\similaridades_{}.csv'.format(data_atual)
    lp.calcular_similaridades_e_salvar_csv(blacklist_onu_path = csv_file_path, partners_file_path = partner_file_path, caminho_saida_csv=caminho_saida_similaridade, limite_similaridade=70)
    comentario_xml = 'Os dados da ONU foram atualizados hoje ! Segue o arquivo em XML'
    comentario_csv = 'Segue a lista consolidada em csv:'
    comentario_similaridade = 'Segue as principais similaridades encontradas: '
    lp.enviar_arquivo_slack(comentario_inicial=comentario_xml, caminho_arquivo = output_xml_file_path)
    lp.enviar_arquivo_slack(comentario_inicial=comentario_csv, caminho_arquivo = csv_file_path)
    lp.enviar_arquivo_slack(comentario_inicial=comentario_similaridade, caminho_arquivo=caminho_saida_similaridade)
else:
    lp.enviar_mensagem_slack(texto='Os dados da ONU NÃO foram atualizados hoje. Data da última atualização: {}'.format(data_atualizacao_onu))



