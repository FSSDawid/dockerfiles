import paho.mqtt.client as mqtt
import logging
import os
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
from pymodbus.constants import Defaults as Defaults
from pymodbus.transaction import ModbusSocketFramer as ModbusFramer
from threading import Timer

__author__ = 'dkubosze'

MBD = {}
WREG = {}


def createEmptyInstance():
    dic = {}
    dic['TKol'] = -12345
    dic['TZew'] = -12345
    dic['TBo1'] = -12345
    dic['TBo2'] = -12345
    dic['TPowCO'] = -12345
    dic['TCo1'] = -12345
    dic['TEko'] = -12345
    dic['TKmf'] = -12345
    dic['Tryb'] = -12345
    return dic


def dirtyFixForuint16Toint32(d):
    c = 32768 #last byte of int16
    if d >= c:
        return (d - c) * -1
    return d


def publishData(T):
    global MBD
    for key, value in T.iteritems():
        if not MBD[key] == value:
            value = dirtyFixForuint16Toint32(int(value))
            client.publish("Frisko/Temp/%s" % key, value / 10.0, 0, True)

def writeRegister(reg, val):
    global WREG
    WREG[reg] = val


def readHoldingRegistersLoop():
    #Check if we have anything to write if YES then write
    global WREG
    if len(WREG) > 0:
        for key, value in WREG.iteritems():
            mbc.write_register(key, value)
        WREG = {}

    rr = mbc.read_holding_registers(4070, 6)
    MBDT = {}
    MBDT['TKol'] = rr.registers[0] #4070
    MBDT['TZew'] = rr.registers[1] #4071
    MBDT['TBo1'] = rr.registers[2] #4072
    MBDT['TBo2'] = rr.registers[3] #4073
    MBDT['TPowCO'] = rr.registers[4] #4074
    MBDT['TCo1'] = rr.registers[5] #4075
    rr = mbc.read_holding_registers(4178, 2)
    MBDT['TEko'] = rr.registers[0] * 10 #4178
    MBDT['TKmf'] = rr.registers[1] * 10 #4179
    #rr = mbc.read_holding_registers(4178, 1)
    #print rr.registers

    rr = mbc.read_holding_registers(4173, 1)
    MBDT['Tryb'] = rr.registers[0]*10#4173
    #print rr.registers
    publishData(MBDT)
    global MBD
    MBD = MBDT
    Timer(0.5, readHoldingRegistersLoop).start()

# The callback for when the client receives a CONNACK response from the server.


def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    #client.publish("Frisko/Temp", "123", 0, True)

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("OH/Temp/TKmf")
    client.subscribe("OH/Temp/Tryb")

# The callback for when a PUBLISH message is received from the server.


def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    if msg.topic == "OH/Temp/TKmf":
        writeRegister(4179, int(float(str(msg.payload))))

    if msg.topic == "OH/Temp/Tryb":
        writeRegister(4173, int(float(str(msg.payload))))
        #mbc.write_register(4179, int(float(str(msg.payload))))


def on_publish(pahoClient, packet, mid):
# Once published, disconnect
        print "Published"
        #pahoClient.disconnect()

if __name__ == '__main__':
    logging.basicConfig()
    log = logging.getLogger()
    log.setLevel(logging.WARNING)
    Defaults.Timeout = 10
    Defaults.UnitId = 1
    modBusIP = os.environ.get('MODBUS_IP')
    modBusPort = os.getenv('MODBUS_PORT',502)
    mbc = ModbusClient(modBusIP, modBusPort, framer=ModbusFramer)
    mbc.connect()
    #readHoldingRegisters()
    client = mqtt.Client(client_id="Modbus2MQTT", clean_session=True, userdata=None)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_publish = on_publish
    mqttUser = os.environ.get('MQTT_USER')
    mqttPass = os.environ.get('MQTT_PASS')
    mqttIP = os.environ.get('MQTT_IP')
    mqttPort = os.getenv('MQTT_PORT',1883)
    client.username_pw_set(mqttUser,mqttPass)
    client.connect(mqttIP, mqttPort, 60)



# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
    MBD = createEmptyInstance()
    t = Timer(1, readHoldingRegistersLoop)
    t.start()
    client.loop_forever()
