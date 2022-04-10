# Aquisição, armazenamento e datar csv - Criar um arquivo para cada mês e ir alimentando.
# quando acabar o mês, criar outro arquivo e começar a alimentar ele. Separar em pastas (mês) e com arquivos csv (dias diferentes)


import random
from datetime import datetime
import json
from paho.mqtt import client as mqtt_client
import pandas as pd
from openpyxl import Workbook, load_workbook
import xlsxwriter
#import asposecells
import os.path
from os import getcwd


broker = 'broker.emqx.io'
port = 1883
topic = '/api/adminene03'
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'
# username = 'emqx'
# password = 'public'
cwd = getcwd()


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print('Connected to MQTT Broker!')
        else:
            print('Failed to connect, return code %d\n', rc)

    client = mqtt_client.Client(client_id)
    #client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    data = []
    def on_message(client, userdata, msg):
        data.append(json.loads(msg.payload.decode()))
        if len(data) == 4:
            print(datetime.today().strftime('%d-%m-%Y %H:%M:%S'))
            print(data)
            power = data[0]['value']
            voltage = data[1]['value']
            current = data[2]['value']
            consumption = data[3]['value']
            print(f'Potência: {power} W\nTensão: {voltage} V\nCorrente: {current} A\nConsumo: {consumption}')
            criar_pasta_ano()
            criar_arquivo(data)
            data.clear()


    client.subscribe(topic)
    client.on_message = on_message


def criar_pasta_ano():
    ano_atual = datetime.now().date().strftime('%Y')
    file_path = os.path.join(cwd, 'dados', str(ano_atual))
    if not os.path.isdir(file_path):
        os.mkdir(file_path)
    criar_pasta_mes(ano_atual)


def criar_pasta_mes(ano_atual):
    mes_atual = datetime.now().date().strftime('%B')
    file_path = os.path.join(cwd, 'dados', str(ano_atual), mes_atual)
    if not os.path.isdir(file_path):
        os.mkdir(file_path)


def criar_arquivo(data): # um arquivo para cada dia do mês
    ano_atual = datetime.now().date().strftime('%Y')
    mes_atual = datetime.now().date().strftime('%B')
    dia_atual = datetime.now().date().strftime('%d')
    folder_path = os.path.join(cwd, 'dados', str(ano_atual), mes_atual)
    file_path = os.path.join(folder_path, str(dia_atual) + '.xlsx')
    if not os.path.isfile(file_path):
        df = pd.DataFrame(data)
        df.insert(0, 'Date', datetime.today().strftime('%d-%m-%Y %H:%M:%S'))
        print(df)
        writer = pd.ExcelWriter(path=file_path, engine='xlsxwriter')
        df.to_excel(writer, sheet_name='dados_sensores', index=False)
        writer.save()
    else:
        append_arquivo(file_path, data, dia_atual)


def append_arquivo(file_path, data, dia_atual):
    df = pd.DataFrame(data)
    df.insert(0, 'Date', datetime.today().strftime('%d-%m-%Y %H:%M:%S'))
    print(df)
    writer = pd.ExcelWriter(path=file_path, engine='openpyxl', mode='a')
    writer.book = load_workbook(file_path)
    writer.sheets = dict((ws.title, ws) for ws in writer.book.worksheets)
    reader = pd.read_excel(file_path, sheet_name='dados_sensores')
    print(len(reader))
    df.to_excel(writer, sheet_name='dados_sensores', index=False, header=False, startrow=len(reader)+1)
    writer.close()


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':

    run()

