
# export OPEN_A3XX_HARDWARE_PANEL_ID=1
from digitalio import Direction, Pull
from RPi import GPIO
from adafruit_mcp230xx.mcp23017 import MCP23017
from time import sleep
import board
import busio
import pika
import os
import json
from netaddr import IPNetwork
import socket
import requests
import coloredlogs, logging
import requests.exceptions
import threading




logger = logging.getLogger(__name__)
coloredlogs.install(level='info', logger=logger)

config_file_path = "opena3xx.config"
api_base_path = ""
api_connectivity_state = False
amqp_connectivity_state = False
local_config = {}
remote_config = {}
amqp_connection = None
amqp_channel = None
bus_1_pins = []
bus_2_pins = []

bus_1 = None
bus_2 = None

offline_queue = []

def write_config():
    logger.info("Updating Config")
    with open(config_file_path, 'w') as outfile:
        json.dump(local_config, outfile, indent=4)
    return

def read_config():
    logger.info("Reading config")
    with open(config_file_path) as json_file:
        global local_config
        local_config = json.load(json_file)


def ping_target(ip, port):
    try:
        r = requests.get(f'{local_config["opena3xx.perhiperal.api.scheme"]}://{ip}:{port}/core/heartbeat/ping', timeout=10)
        if r.status_code == 200:
            if r.text == "Pong from OpenA3XX":
                global api_connectivity_state
                api_connectivity_state = True
                logger.info("Received Valid Response from OpenA3XX API - Success")
                global api_base_path
                api_base_path = f"{local_config['opena3xx.perhiperal.api.scheme']}://{ip}:{port}"
                return True
        return False
    except Exception:
        return False

def scan_network():
    logger.info("Started Scanning Network")
    for ip in IPNetwork(local_config["opena3xx.network.scan-range.cidr"]):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket.setdefaulttimeout(0.1)
        target = socket.gethostbyname(str(ip))
        # returns an error indicator
        port = int(local_config["opena3xx.perhiperal.api.port"])
        result = s.connect_ex((target, port)) 
        if result ==0:
            local_config["opena3xx.perhiperal.api.ip"] = target
            logger.info(f"Found something on IP:{target} on Port: {port}")
            logger.info("Sending Ping to check if it is OpenA3XX API")
            if ping_target(target, port) == True:
                write_config()
                s.close()
                return True
            else:
                logger.warning("Continue scanning: Invalid Response")
        s.close()
    return False


def get_remote_configuration():
    logger.info("Fetching Remote Configuration from OpenA3XX Peripheral API")
    r = requests.get(f'{api_base_path}/configuration', timeout=10)
    global remote_config
    data = json.loads(r.text)
    remote_config = data["configuration"]

def bootstrap():
    logger.info("Bootstrapping...")
    read_config()
    if local_config['opena3xx.perhiperal.api.ip'] == "":
        logger.info(f"Started scanning on IP CIDR: {local_config['opena3xx.network.scan-range.cidr']}")
        if scan_network() == True:
            logger.info(f"Completed scanning on IP CIDR: {local_config['opena3xx.network.scan-range.cidr']}")
        else:
            logger.critical(f"Completed scanning on IP CIDR: {local_config['opena3xx.network.scan-range.cidr']}. No OpenA3XX Peripheral API Found")
            exit(100)
    else:
        logger.info(f"Configuration is already set: Validating started")
        if not ping_target(local_config["opena3xx.perhiperal.api.ip"], int(local_config["opena3xx.perhiperal.api.port"])):
            if scan_network() == True:
                logger.info(f"Completed scanning on IP CIDR: {local_config['opena3xx.network.scan-range.cidr']}")
            else:
                logger.critical(f"Completed scanning on IP CIDR: {local_config['opena3xx.network.scan-range.cidr']}. No OpenA3XX Peripheral API Found")
                exit(100)
        logger.info(f"Configuration is Valid")

    get_remote_configuration()

def api_keep_alive():
    logger.info("Keeping alive with API...")
    global api_connectivity_state
    if api_connectivity_state == True:
        threading.Timer(5.0, api_keep_alive).start()
    try:
        global api_base_path
        r = requests.post(f'{api_base_path}/hardware-panel/keep-alive/21973638-e33f-4bd8-88a6-7ca5d3c161d5', timeout=0.5)
        if r.status_code == 200:
            api_connectivity_state = True
        else:
            api_connectivity_state = False
    except Exception:
        api_connectivity_state = False
        logger.critical("Keeping alive failed")

def amqp_keep_alive():
    threading.Timer(30.0, amqp_keep_alive).start()
    publish_heartbeat()

def connect_amqp():
    logger.info("Connecting to RabbitMQ")
    global amqp_connection
    global amqp_channel
    global offline_queue

    amqp_connection = pika.BlockingConnection(parameters)
    amqp_channel = amqp_connection.channel()
    amqp_channel.queue_declare(queue='hardware_events')
    amqp_channel.queue_declare(queue='heartbeat')
    sleep(2)
    if offline_queue:
        logger("Processing offline queue due to disconnection")
        for message in offline_queue:
            logger("Publishing message from offline queue")
            publish("hardware_events",message)

def generate_payload(bus, io_no):
    logger.info("Generating Payload to RabbitMQ")
    hardware_panel_id = os.environ['OPEN_A3XX_HARDWARE_PANEL_ID']
    message = { 
        "hardware_panel_id": hardware_panel_id, 
        "bus": bus, 
        "signal_on": io_no 
    }
    return message

def publish(routing_key, body):
    logger.info("Publishing to RabbitMQ")
    global amqp_connection
    global amqp_channel
    amqp_channel.basic_publish(exchange='',routing_key=routing_key, body=body)

def publish_heartbeat():
    logger.info("Heartbeat to RabbitMQ")
    global amqp_connection
    global amqp_channel
    amqp_channel.basic_publish(exchange='',routing_key='heartbeat', body="")



def handle_interrup_bus_1(port):
    """Callback function to be called when an Interrupt occurs."""
    global amqp_connection
    global bus_1_pins
    global bus_1
    global amqp_channel

    for pin_flag in bus_1.int_flag:
        if bus_1_pins[pin_flag].value == False:
            logger.info("Interrupt connected to Pin: {}".format(port))
            logger.info("Pin number: {} changed to: {}".format(pin_flag, bus_1_pins[pin_flag].value))
            payload = json.dumps(generate_payload(1, pin_flag))
            logger.info(payload)
            try:
                publish("hardware_events", payload)
            except Exception as e:
                offline_queue.append(payload)
                connect_amqp()
    bus_1.clear_ints()

def handle_interrup_bus_2(port):
    """Callback function to be called when an Interrupt occurs."""
    global amqp_connection
    global bus_2_pins
    global bus_2
    global amqp_channel

    for pin_flag in bus_2.int_flag:
        if bus_2_pins[pin_flag].value == False:
            logger.info("Interrupt connected to Pin: {}".format(port))
            logger.info("Pin number: {} changed to: {}".format(pin_flag, bus_2_pins[pin_flag].value))
            payload = json.dumps(generate_payload(2, pin_flag))
            logger.info(payload)
            try:
                publish("hardware_events", payload)
            except Exception as e:
                offline_queue.append(payload)
                connect_amqp()
            
    bus_2.clear_ints()

#---------------------------------------------
#---------------------------------------------
#---------------------------------------------
#---------------------------------------------
#---------------------------------------------
#---------------------------------------------
#---------------------------------------------
#---------------------------------------------


if os.environ['OPEN_A3XX_HARDWARE_PANEL_ID'] == "":
        logger.critical("OPEN_A3XX_HARDWARE_PANEL_ID ENV VAR is not set. Please check.")
        exit(1)

logger.info("Application Started")
while True:
    bootstrap()
    api_keep_alive()
    
    credentials = pika.PlainCredentials(remote_config["opena3xx.amqp.username"], remote_config["opena3xx.amqp.password"])
    parameters = pika.ConnectionParameters(remote_config["opena3xx.amqp.host"],
                                        remote_config["opena3xx.amqp.port"],
                                        remote_config["opena3xx.amqp.vhost"],
                                        credentials,
                                        heartbeat=60)

    connect_amqp()

    #---------------------------------------------
    # Initialize the I2C bus:
    i2c = busio.I2C(board.SCL, board.SDA)

    #---------------------------------------------
    # Initialize the MCP23017 chips
    bus_1 = MCP23017(i2c, address=0x20)
    bus_2 = MCP23017(i2c, address=0x21)

    #---------------------------------------------

    for pin in range(0, 16):
        bus_1_pins.append(bus_1.get_pin(pin))

    for pin in range(0, 16):
        bus_2_pins.append(bus_2.get_pin(pin))

    #---------------------------------------------
    
    # Set all the pins to input
    for pin in bus_1_pins:
        pin.direction = Direction.INPUT
        pin.pull = Pull.UP

    for pin in bus_2_pins:
        pin.direction = Direction.INPUT
        pin.pull = Pull.UP

    #---------------------------------------------

    bus_1.interrupt_enable = 0xFFFF  # Enable Interrupts in all pins
    bus_1.interrupt_configuration = 0x0000  # interrupt on any change
    bus_1.io_control = 0x44  # Interrupt as open drain and mirrored
    bus_1.clear_ints()  # Interrupts need to be cleared initially
    bus_1.default_value = 0xFFFF

    bus_2.interrupt_enable = 0xFFFF  # Enable Interrupts in all pins
    bus_2.interrupt_configuration = 0x0000  # interrupt on any change
    bus_2.io_control = 0x44  # Interrupt as open drain and mirrored
    bus_2.clear_ints()  # Interrupts need to be cleared initially
    bus_2.default_value = 0xFFFF


    GPIO.setmode(GPIO.BCM)
    bus_1_interrupt = 24
    GPIO.setup(bus_1_interrupt, GPIO.IN, GPIO.PUD_UP)
    GPIO.add_event_detect(bus_1_interrupt, GPIO.FALLING, callback=handle_interrup_bus_1, bouncetime=105)

    GPIO.setmode(GPIO.BCM)
    bus_2_interrupt = 23
    GPIO.setup(bus_2_interrupt, GPIO.IN, GPIO.PUD_UP)
    GPIO.add_event_detect(bus_2_interrupt, GPIO.FALLING, callback=handle_interrup_bus_2, bouncetime=105)


    logger.info("MCDU Started.")
    amqp_keep_alive()
    while api_connectivity_state:
        if not amqp_connection or amqp_connection.is_closed:
            logger.critical("Connection to AMQP is closed. Connecting again.")
            connect_amqp()
