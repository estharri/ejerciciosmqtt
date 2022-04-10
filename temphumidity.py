"""
4.- TEMPERATURA Y HUMEDAD
Elige un termómetro concreto al que escuchar, es decir, uno de los sensores que 
publican en temperature. Escribe ahora el código para un cliente mqtt cuya
misión es escuchar un termómetro y, si su valor supera una determinada 
temperatura, K0, entonces pase a escuchar también en el topic humidity. Si la 
temperatura baja de K 0 o el valor de humidity sube de K 1 entonces el cliente
dejará de escuchar en el topic humidity.
"""
from paho.mqtt.client import Client
TEMP = 'temperature'
HUMIDITY = 'humidity'

def on_message(mqttc, data, msg):
    print (f'message:{msg.topic}:{msg.payload}:{data}')
    if data['status'] == 0:
        temp = int(msg.payload) # we are only susbribed in temperature
        if temp>data['temp_threshold']:
            print(f'umbral superado {temp}, suscribiendo a humidity')
            mqttc.subscribe(HUMIDITY)
            data['status'] = 1
    elif data['status'] == 1:
        if msg.topic==HUMIDITY:
            humidity = int(msg.payload)
        if humidity>data['humidity_threshold']:
            print(f'umbral humedad {humidity} superado, cancelando suscripcion')
            mqttc.unsubscribe(HUMIDITY)
            data['status'] = 0
    elif TEMP in msg.topic:
        temp = int(msg.payload)
        if temp<=data['temp_threshold']:
            print(f'temperatura {temp} por debajo de umbral, cancelando suscripcion')
            data['status']=0
            mqttc.unsubscribe(HUMIDITY)

def on_log(mqttc, data, level, buf):
    print(f'LOG: {data}:{msg}')

def main(broker):
    data = {'temp_threshold':20,
    'humidity_threshold':80,
    'status': 0}
    mqttc = Client(userdata=data)
    mqttc.on_message = on_message
    mqttc.enable_logger()
    mqttc.connect(broker)
    mqttc.subscribe(f'{TEMP}/t1')
    mqttc.loop_forever()

if __name__ == "__main__":
    import sys
    if len(sys.argv)<2:
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1)
    broker = sys.argv[1]
    main(broker)