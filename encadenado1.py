from paho.mqtt.client import Client
from paho.mqtt import publish
import traceback
from time import sleep
from math import sqrt

def on_message(mqttc, data, msg):
    print(msg.topic,msg.payload)
    try:
        if msg.topic in 'numbers':
            if int(sqrt(float(msg.payload)))**2 == float(msg.payload) and float(msg.payload) > 1:
                mqttc.publish('clients/raices', msg.payload)
            else: 
                mqttc.publish('clients/noraices', msg.payload)
        else:
            if msg.topic in 'clients/raices':
                data['tiempo'].append(int(sqrt(int(msg.payload))))
            else:
                data['media'].append(float(msg.payload))
    except Exception as e:
        print(e)
        traceback.print_exc()


def main(broker):
    data = {'media':[] , 'tiempo':[]}
    mqttc = Client(userdata=data)
    mqttc.on_message = on_message
    mqttc.enable_logger()
    mqttc.connect(broker)
    mqttc.subscribe('numbers')
    mqttc.subscribe('clients/raices')
    mqttc.loop_start()
    while True:        
        if data['tiempo'] != []:
            tiempo = data['tiempo'][0]
            data['tiempo'].pop(0)
            mqttc.subscribe('clients/noraices')
            sleep(tiempo)
            mqttc.unsubscribe('clients/noraices')
            media = sum(data['media'])/(max(1,len(data['media'])))
            mensaje = (f'Media en {tiempo} segundos en clients/noraices = {media}')
            publish.single('clients/medias', payload = mensaje, hostname = "picluster02.mat.ucm.es")
            data['media'] = []
    mqttc.loop_close()

if __name__ == "__main__":
    import sys
    if len(sys.argv)<2:
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1)
    broker = sys.argv[1]
    main(broker)