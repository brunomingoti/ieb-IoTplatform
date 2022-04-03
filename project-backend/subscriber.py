import random
from datetime import datetime
import json
from paho.mqtt import client as mqtt_client


broker = 'broker.emqx.io'
port = 1883
topic = '/api/mqtt'
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'
# username = 'emqx'
# password = 'public'


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
        #print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        data.append(json.loads(msg.payload.decode()))
        if len(data) == 4:
            print(datetime.today().strftime('%d-%m-%Y %H:%M:%S'))
            #print(data)
            power = data[0]['value']
            voltage = data[1]['value']
            current = data[2]['value']
            consumption = data[3]['value']

            print(f'Potência: {power} W\nTensão: {voltage} V\nCorrente: {current} A\nConsumo {consumption} kWh\n')

            data.clear()


    client.subscribe(topic)

    client.on_message = on_message

def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':

    run()

