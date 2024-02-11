#!/usr/bin/python
# -*- coding: utf-8 -*-
# read out EMH eHZB SML messages from serial port and publish via MQTT
#
# version of 09.02.2024

import sys
import os
import serial
import time
import math
import paho.mqtt.publish as publish
import rrdtool
import logging

# Einstellungen

serialport = '/dev/ttyAMA0'
broker = '10.9.11.60'
mqttport = '1883'
bezug_rrd = "%s/bezug-emh.rrd" % (os.path.dirname(os.path.abspath(__file__)))
einspeisung_rrd = "%s/einspeisung-emh.rrd" % (os.path.dirname(os.path.abspath(__file__)))
data_hex = ''
reading_ok = False

crc16_x25_table = [
    0x0000, 0x1189, 0x2312, 0x329B, 0x4624, 0x57AD, 0x6536, 0x74BF,
    0x8C48, 0x9DC1, 0xAF5A, 0xBED3, 0xCA6C, 0xDBE5, 0xE97E, 0xF8F7,
    0x1081, 0x0108, 0x3393, 0x221A, 0x56A5, 0x472C, 0x75B7, 0x643E,
    0x9CC9, 0x8D40, 0xBFDB, 0xAE52, 0xDAED, 0xCB64, 0xF9FF, 0xE876,
    0x2102, 0x308B, 0x0210, 0x1399, 0x6726, 0x76AF, 0x4434, 0x55BD,
    0xAD4A, 0xBCC3, 0x8E58, 0x9FD1, 0xEB6E, 0xFAE7, 0xC87C, 0xD9F5,
    0x3183, 0x200A, 0x1291, 0x0318, 0x77A7, 0x662E, 0x54B5, 0x453C,
    0xBDCB, 0xAC42, 0x9ED9, 0x8F50, 0xFBEF, 0xEA66, 0xD8FD, 0xC974,
    0x4204, 0x538D, 0x6116, 0x709F, 0x0420, 0x15A9, 0x2732, 0x36BB,
    0xCE4C, 0xDFC5, 0xED5E, 0xFCD7, 0x8868, 0x99E1, 0xAB7A, 0xBAF3,
    0x5285, 0x430C, 0x7197, 0x601E, 0x14A1, 0x0528, 0x37B3, 0x263A,
    0xDECD, 0xCF44, 0xFDDF, 0xEC56, 0x98E9, 0x8960, 0xBBFB, 0xAA72,
    0x6306, 0x728F, 0x4014, 0x519D, 0x2522, 0x34AB, 0x0630, 0x17B9,
    0xEF4E, 0xFEC7, 0xCC5C, 0xDDD5, 0xA96A, 0xB8E3, 0x8A78, 0x9BF1,
    0x7387, 0x620E, 0x5095, 0x411C, 0x35A3, 0x242A, 0x16B1, 0x0738,
    0xFFCF, 0xEE46, 0xDCDD, 0xCD54, 0xB9EB, 0xA862, 0x9AF9, 0x8B70,
    0x8408, 0x9581, 0xA71A, 0xB693, 0xC22C, 0xD3A5, 0xE13E, 0xF0B7,
    0x0840, 0x19C9, 0x2B52, 0x3ADB, 0x4E64, 0x5FED, 0x6D76, 0x7CFF,
    0x9489, 0x8500, 0xB79B, 0xA612, 0xD2AD, 0xC324, 0xF1BF, 0xE036,
    0x18C1, 0x0948, 0x3BD3, 0x2A5A, 0x5EE5, 0x4F6C, 0x7DF7, 0x6C7E,
    0xA50A, 0xB483, 0x8618, 0x9791, 0xE32E, 0xF2A7, 0xC03C, 0xD1B5,
    0x2942, 0x38CB, 0x0A50, 0x1BD9, 0x6F66, 0x7EEF, 0x4C74, 0x5DFD,
    0xB58B, 0xA402, 0x9699, 0x8710, 0xF3AF, 0xE226, 0xD0BD, 0xC134,
    0x39C3, 0x284A, 0x1AD1, 0x0B58, 0x7FE7, 0x6E6E, 0x5CF5, 0x4D7C,
    0xC60C, 0xD785, 0xE51E, 0xF497, 0x8028, 0x91A1, 0xA33A, 0xB2B3,
    0x4A44, 0x5BCD, 0x6956, 0x78DF, 0x0C60, 0x1DE9, 0x2F72, 0x3EFB,
    0xD68D, 0xC704, 0xF59F, 0xE416, 0x90A9, 0x8120, 0xB3BB, 0xA232,
    0x5AC5, 0x4B4C, 0x79D7, 0x685E, 0x1CE1, 0x0D68, 0x3FF3, 0x2E7A,
    0xE70E, 0xF687, 0xC41C, 0xD595, 0xA12A, 0xB0A3, 0x8238, 0x93B1,
    0x6B46, 0x7ACF, 0x4854, 0x59DD, 0x2D62, 0x3CEB, 0x0E70, 0x1FF9,
    0xF78F, 0xE606, 0xD49D, 0xC514, 0xB1AB, 0xA022, 0x92B9, 0x8330,
    0x7BC7, 0x6A4E, 0x58D5, 0x495C, 0x3DE3, 0x2C6A, 0x1EF1, 0x0F78]

# Funktionen

def crc16_x25(Buffer):
    crcsum = 0xffff
    global crc16_x25_table
    for byte in Buffer:
        crcsum = crc16_x25_table[(ord(byte) ^ crcsum) & 0xff] ^ (crcsum >> 8 & 0xff)
    crcsum ^= 0xffff
    return crcsum

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
        data_start = ser.read(1)
        if data_start == '\x1b' >= 0:
            read_start += 1
        else:
            read_start = 0
    ser.read(4) # nach erster Hälfte der Start- bzw. Stopp-Sequenz noch die zweite Hälfte lesen

    while True:
        try:
            while True:
                data_raw = ser.read(8)
                if data_raw == '\x1b\x1b\x1b\x1b\x01\x01\x01\x01' >= 0: # Startsequenz SML 1b1b1b1b01010101
                    data_raw += ser.read(752) # Nachrichtenlänge 760 Zeichen, Rest nach Startsequenz einlesen
                    reading_ok = True
                    break
        except serial.serialutil.SerialException as e:
            reading_ok = False
            logging.debug("Fehler serielle Schnittstelle: %s" % (e,))
        if reading_ok:
            message = data_raw[0:-2] # Nachricht ohne CRC-Prüfsumme
            crc_calc = crc16_x25(message)
            crc_rx = int((data_raw[-1] + data_raw[-2]).encode('hex'), 16) # Letzte 2 Zeichen sind CRC-Prüfsumme in umgekehrter Reihenfolge
            if crc_rx == crc_calc:
                message_hex = message.encode('hex')
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
                    publish.multiple(msgs, hostname=broker, port=mqttport)
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
