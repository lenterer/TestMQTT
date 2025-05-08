import time
import paho.mqtt.client as paho
from paho import mqtt
import threading

# setting callbacks for different events to see if it works, print the message etc.
def on_connect(client, userdata, flags, rc, properties=None):
    print("SUCCESS CONNACK received with code %s." % rc)

# with this callback you can see if your publish was successful
def on_publish(client, userdata, mid, properties=None):
    print("mid: " + str(mid))

# print which topic was subscribed to
def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

# print message, useful for checking if it was successful
def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
 
def publish_loop():
    num = 0
    while num <= 1000:
        # Initialize time with ms   
        tm = time.time()
        tm_ms = int(tm * 1000)
        formatted_time = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(tm))
        ms = tm_ms % 1000
        formatted_with_ms = f"{formatted_time}.{ms:03d}"
                
        payload = f" {num} & {formatted_with_ms} "
        client.publish("test/time", payload=payload, qos=1)
        num = num + 1
        time.sleep(0.05)
    print("SUCCESS !!!")  
    client.disconnect()

# using MQTT version 5 here, for 3.1.1: MQTTv311, 3.1: MQTTv31
# userdata is user defined data of any type, updated by user_data_set()
# client_id is the given name of the client
client = paho.Client(client_id="", userdata=None, protocol=paho.MQTTv5)
client.on_connect = on_connect

# enable TLS for secure connection
client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
# set username and password
client.username_pw_set("Testing.Rippers", "Bismillah1")
# connect to HiveMQ Cloud on port 8883 (default for MQTT)
client.connect("abda57f3c12743db931e9bed3781aca3.s1.eu.hivemq.cloud", 8883)

# setting callbacks, use separate functions like above for better visibility
client.on_subscribe = on_subscribe
client.on_message = on_message
client.on_publish = on_publish

# subscribe to all topics of encyclopedia by using the wildcard "#"
client.subscribe("test/123", qos=0)

# a single publish, this can also be done in loops, etc.
client.publish("test/time", payload="hot", qos=0)

# Jalankan loop publish di thread terpisah agar tidak mengganggu client.loop_forever()
threading.Thread(target=publish_loop, daemon=True).start()

# loop_forever for simplicity, here you need to stop the loop manually
# you can also use loop_start and loop_stop
client.loop_forever()
