from paho.mqtt.client import Client
from time import time
from math import sqrt

def on_message1(mqttc, data, msg):
    try:
        print(msg.payload)
        if int(sqrt(float(msg.payload)))**2 == float(msg.payload):
            mqttc.publish('clients/raices', msg.payload)
        else: 
            mqttc.publish('clients/noraices', msg.payload)
    except:
        pass
    
def on_message2(mqttc, data, msg):
    try:
        if msg.topic in 'clients/raices':
            tiempo = time()
            raiz = int(sqrt(int(msg.payload)))
            mqttc.subscribe('clients/noraices')
            while time() - tiempo < raiz:
                print('intervalo')
            mqttc.unsubscribe('clients/noraices')
            media = sum(data['num'])/len(data['num'])
            data['num'] = []
            mqttc.publish('clients/medias',f'Media en clients/noraices desde la aparicion de {raiz**2} = {media}')
        else:
            data['num'].append(float(msg.payload))
    except:
        pass

def main(broker):
    mqttc1 = Client()
    mqttc1.on_message = on_message1
    mqttc1.enable_logger()
    mqttc1.connect(broker)
    mqttc1.subscribe('numbers')
    mqttc1.subscribe('clients/raices')
    mqttc1.loop_start()
    data = {}
    data['num'] = []
    mqttc1.loop_close()
    mqttc2 = Client()
    mqttc2.on_message = on_message2
    mqttc2.enable_logger()
    mqttc2.connect(broker)
    mqttc2.subscribe("clients/raices")
    mqttc2.loop_forever()
    
if __name__ == "__main__":
    import sys
    if len(sys.argv)<2:
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1)
    broker = sys.argv[1]
    main(broker)