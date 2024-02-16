#!/usr/bin/python3
# -*- coding: utf-8 -*-
# read out EMH eHZB SML messages from serial port and publish via MQTT
#
# version of 16.02.2024

import sys
import os
import serial
import time
import math
import paho.mqtt.publish as publish
import rrdtool
import logging
from crccheck.crc import Crc16X25

# Einstellungen

serialport = '/dev/ttyAMA0'
bezug_rrd = "%s/bezug-emh.rrd" % (os.path.dirname(os.path.abspath(__file__)))
einspeisung_rrd = "%s/einspeisung-emh.rrd" % (os.path.dirname(os.path.abspath(__file__)))
data_hex = ''
reading_ok = False

# Funktionen

def hexstr2signedint(hexval):
    uintval = int(hexval,16)
    if uintval > 0x7FFFFFFF:
        uintval -= 0x100000000
    return uintval

def parseSML(message_hex, obis_string, pos, length):
    obis_value = 0
    position = message_hex.find(obis_string)
    if position <= 0:
        return 0
    hex_value = message_hex[position+pos:position+pos+length]
    obis_value = hexstr2signedint(hex_value)
    return obis_value

def main():
    global logging
    logging.basicConfig(filename='/home/pi/strom/sml.log',
    level = logging.DEBUG,
    format = '%(asctime)s %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S')
    ser = serial.Serial(
        port = serialport,
        baudrate = 9600,
        parity = serial.PARITY_NONE,
        stopbits = serial.STOPBITS_ONE,
        bytesize = serial.EIGHTBITS,
        timeout = 2)
    ser.flushInput()
    ser.flushOutput()

    read_start = 0 # Schleife, um sich auf erste Hälfte der Start- bzw. Stopp-Sequenz zu synchronisieren
    while read_start < 4:
        data_start = ser.read(1).hex()
        if data_start == '1b':
            read_start += 1
        else:
            read_start = 0
    ser.read(4) # nach erster Hälfte der Start- bzw. Stopp-Sequenz noch die zweite Hälfte lesen

    while True:
        try:
            while True:
                data_raw = ser.read(8)
                data_hex = data_raw.hex()
                if data_hex == '1b1b1b1b01010101': # Startsequenz SML 1b1b1b1b01010101
                    data_raw += ser.read(752) # Nachrichtenlänge 760 Zeichen, Rest nach Startsequenz einlesen
                    reading_ok = True
                    break
        except serial.serialutil.SerialException as e:
            reading_ok = False
            logging.debug("Fehler serielle Schnittstelle: %s" % (e,))
        if reading_ok:
            message_hex = data_raw[0:-2].hex() # Nachricht ohne CRC-Prüfsumme
            crc_rx = data_raw[-2:].hex() # CRC-Prüfsumme mit vertauschter Byte-Reihenfolge
            message_bytes = bytes.fromhex(message_hex)
            crc_calc = Crc16X25.calc(message_bytes)
            crc_hex = crc_calc.to_bytes(2, byteorder='little').hex() # Byte-Reihenfolge der berechneten Prüfsumme tauschen
            if crc_hex == crc_rx:
                sml180 = parseSML(message_hex, '070100010800ff', 48, 16) # Wirkenergie in Wh (Bezug)
                sml280 = parseSML(message_hex, '070100020800ff', 42, 16) # Wirkenergie in Wh (Einspeisung)
                sml3670 = parseSML(message_hex, '070100240700ff', 42, 8) # Wirkleistung L1 in W
                sml5670 = parseSML(message_hex, '070100380700ff', 42, 8) # Wirkleistung L2 in W
                sml7670 = parseSML(message_hex, '0701004c0700ff', 42, 8) # Wirkleistung L3 in W
                sml1670 = parseSML(message_hex, '070100100700ff', 42, 8) # Wirkleistung in W
#                sml3270 = parseSML(message_hex, '070100200700ff', 42, 8) # Spannung L1
#                sml5270 = parseSML(message_hex, '070100340700ff', 42, 8) # Spannung L2
#                sml7270 = parseSML(message_hex, '070100480700ff', 42, 8) # Spannung L3
#                sml3170 = parseSML(message_hex, '0701001f0700ff', 42, 8) # Strom L1
#                sml5170 = parseSML(message_hex, '070100330700ff', 42, 8) # Strom L2
#                sml7170 = parseSML(message_hex, '070100470700ff', 42, 8) # Strom L3
#                sml8171 = parseSML(message_hex, '070100510701ff', 42, 8) # Phasenwinkel U-L2 zu U-L1
#                sml8172 = parseSML(message_hex, '070100510702ff', 42, 8) # Phasenwinkel U-L3 zu U-L1
#                sml8174 = parseSML(message_hex, '070100510704ff', 42, 8) # Phasenwinkel I-L1 zu U-L1
#                sml81715 = parseSML(message_hex, '07010051070fff', 42, 8) # Phasenwinkel I-L2 zu U-L2
#                sml81726 = parseSML(message_hex, '07010051071aff', 42, 8) # Phasenwinkel I-L3 zu U-L3
#                sml1470 = parseSML(message_hex, '0701000e0700ff', 42, 8) # Frequenz in Hz
                msgs = [{'topic':"stromzaehler/bezug", 'payload':(sml180/10000.0000)},
                    ("stromzaehler/einspeisung", sml280/10000.0000),
                    ("stromzaehler/leistung", float(sml1670)),
                    ("stromzaehler/leistung-L1", float(sml3670)),
                    ("stromzaehler/leistung-L2", float(sml5670)),
                    ("stromzaehler/leistung-L3", float(sml7670)),
#                    ("stromzaehler/spannung-L1", sml3270/10.0),
#                    ("stromzaehler/spannung-L2", sml5270/10.0),
#                    ("stromzaehler/spannung-L3", sml7270/10.0),
#                    ("stromzaehler/strom-L1", sml3170/100.00),
#                    ("stromzaehler/strom-L2", sml5170/100.00),
#                    ("stromzaehler/strom-L3", sml7170/100.00),
#                    ("stromzaehler/8171", sml8171),
#                    ("stromzaehler/8172", sml8172),
#                    ("stromzaehler/8174", sml8174),
#                    ("stromzaehler/81715", sml81715),
#                    ("stromzaehler/81726", sml81726),
#                    ("stromzaehler/frequenz", sml1470/10.0),
                    ]
                try:
                    publish.multiple(msgs, hostname='10.9.11.60', port=1883)
                except:
                    logging.debug("MQTT-Fehler")
                try:
                    rrdtool.update(bezug_rrd, 'N:%s:%s:%s:%s:%s' % ((sml180/10000.0000),(max(0, sml1670)),(max(0, sml3670)),(max(0, sml5670)),(max(0, sml7670))))
                    rrdtool.update(einspeisung_rrd, 'N:%s:%s:%s:%s:%s' % ((sml280/10000.0000),(abs(min(0, sml1670))),(abs(min(0, sml3670))),(abs(min(0, sml5670))),(abs(min(0, sml7670)))))
                except rrdtool.OperationalError as e:
                    logging.debug("RRDtool-Fehler:" % (e,))
            else:
                logging.debug("CRC-Fehler")

if __name__ == "__main__":
    main()
