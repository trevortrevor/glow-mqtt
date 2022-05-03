#!/usr/bin/python

import paho.mqtt.client as mqtt
import argparse
import json
import os
import yaml



# Parse config.yaml
with open("config.yml", 'r') as inputFile:
    try:
        configData = yaml.safe_load(inputFile)
        print(inputFile)
    except yaml.YAMLError as exc:
        print(exc)
 
glowData = configData['glow']
haData = configData['homeassistant']
debug = configData['debug']

s_mqtt_topic = "SMART/HILD/" + glowData['glow_device']
p_mqtt_topic = "glow" + "/" + glowData['glow_device']

def twos_complement(hexstr):
    value = int(hexstr,16)
    bits = len(hexstr) * 4
    
    if value & (1 << (bits-1)):
        value -= 1 << bits
        
    return value

def on_connect(client, obj, flags, rc):
    print("MQTT connected...")

def on_glow_connect(client, obj, flags, rc):
    print("Connected to Glow MQTT broker...")
    print("Connecting to: " + str(s_mqtt_topic) )
    client.subscribe(s_mqtt_topic, 0)

def process_msg(client, userdata, message):
    status = {}
    
    data = json.loads(message.payload)

    if(debug):
        print(data)

    if 'elecMtr' in data:
        if '00' in data['elecMtr']['0702']['00']:
            status["elec_imp"] = int(data['elecMtr']['0702']['00']['00'],16) * int(data['elecMtr']['0702']['03']['01'],16) / int(data['elecMtr']['0702']['03']['02'],16)

        if '00' in data['elecMtr']['0702']['04']:
            status["watt_now"] = twos_complement(data['elecMtr']['0702']['04']['00'])

        if '01' in data['elecMtr']['0702']['00']:
            status["elec_exp"] = int(data['elecMtr']['0702']['00']['01'],16) * int(data['elecMtr']['0702']['03']['01'],16) / int(data['elecMtr']['0702']['03']['02'],16)
        
    if 'gasMtr' in data:
        if '00' in data['gasMtr']['0702']['00']:
            status["gas_mtr"] = int(data['gasMtr']['0702']['00']['00'],16) * int(data['gasMtr']['0702']['03']['01'],16) / int(data['gasMtr']['0702']['03']['02'],16)

    print(status)

    mqttc.publish(p_mqtt_topic, json.dumps(status), retain=True)

# Create MQTT client
mqttc = mqtt.Client()
mqttc.on_connect = on_connect
mqttc.username_pw_set(haData['mqtt_username'],haData['mqtt_password'])
mqttc.connect(haData['mqtt_address'], haData['mqtt_port'], 60)
mqttc.loop_start()

# Home Assistant
if (haData['discovery']):
    print("Configuring Home Assistant...")

    discovery_msgs = []

    # Current power in watts
    watt_now_topic = "homeassistant/sensor/glow_" + glowData['glow_device'] + "/watt_now/config"
    watt_now_payload = {"device_class": "power", "state_class": "measurement", "device": {"identifiers": ["glow_" + glowData['glow_device']], "manufacturer": "Glow", "name": glowData['glow_device']}, "unique_id": "glow_" + glowData['glow_device'] + "_watt_now", "name": "glow_" + glowData['glow_device'] + "_current_power", "state_topic": p_mqtt_topic, "unit_of_measurement": "W", "value_template": "{{ value_json.watt_now}}" }
    mqttc.publish(watt_now_topic, json.dumps(watt_now_payload), retain=True)

    # Electricity import total kWH
    elec_imp_topic = "homeassistant/sensor/glow_" + glowData['glow_device'] + "/elec_imp/config"
    elec_imp_payload = {"device_class": "energy", "state_class": "total_increasing", "device": {"identifiers": ["glow_" + glowData['glow_device']], "manufacturer": "Glow", "name": glowData['glow_device']}, "unique_id": "glow_" + glowData['glow_device'] + "_elec_imp", "name": "glow_" + glowData['glow_device'] + "_electric_import", "state_topic": p_mqtt_topic, "unit_of_measurement": "kWh", "value_template": "{{ value_json.elec_imp}}"}
    mqttc.publish(elec_imp_topic, json.dumps(elec_imp_payload), retain=True)

    # Electricity export total kWH
    elec_exp_topic = "homeassistant/sensor/glow_" + glowData['glow_device'] + "/elec_exp/config"
    elec_exp_payload = {"device_class": "energy", "state_class": "total_increasing", "device": {"identifiers": ["glow_" + glowData['glow_device']], "manufacturer": "Glow", "name": glowData['glow_device']}, "unique_id": "glow_" + glowData['glow_device'] + "_elec_exp", "name": "glow_" + glowData['glow_device'] + "_electric_export", "state_topic": p_mqtt_topic, "unit_of_measurement": "kWh", "value_template": "{{ value_json.elec_exp}}"}
    mqttc.publish(elec_exp_topic, json.dumps(elec_exp_payload), retain=True)

    # Gas total m³
    gas_mtr_topic = "homeassistant/sensor/glow_" + glowData['glow_device'] + "/gas_mtr/config"
    gas_mtr_payload = {"device_class": "gas", "state_class": "total_increasing", "device": {"identifiers": ["glow_" + glowData['glow_device']], "manufacturer": "Glow", "name": glowData['glow_device']}, "unique_id": "glow_" + glowData['glow_device'] + "_gas_mtr", "name": "glow_" + glowData['glow_device'] + "_gas_meter", "state_topic": p_mqtt_topic, "unit_of_measurement": "m³", "value_template": "{{ value_json.gas_mtr}}"}
    mqttc.publish(gas_mtr_topic, json.dumps(gas_mtr_payload), retain=True)

# Create Glow MQTT client
mqttg = mqtt.Client()
mqttg.on_connect = on_glow_connect
mqttg.on_message = process_msg
mqttg.username_pw_set(glowData['glow_username'],glowData['glow_password'])
if glowData['use_tls']:
    mqttg.tls_set()
    mqttg.connect("glowmqtt.energyhive.com", 8883, 60)
else:
    mqttg.connect("glowmqtt.energyhive.com", 1883, 60)
mqttg.loop_forever()

