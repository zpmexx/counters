import random
from paho.mqtt import client as mqtt_client
import time
from dotenv import load_dotenv
import pyodbc 
import os

load_dotenv()
broker = os.getenv('broker')
port = int(os.getenv('port'))
username = os.getenv('usernames')
password = os.getenv('password')
topicIn = os.getenv('topicIn')
topicOut = os.getenv('topicOut')
client_id = os.getenv('client_id')
sqlserver = os.getenv('sqlserver')
sqlcounterdatabase = os.getenv('sqlcounterdatabase')

recivedCode = 'a123'
recivedType = 'entrance'
recivedDate = '2022-10-11'
recivedTime = '20:20:00'
flag = 0
while True:
    input()
    try:
        conn = pyodbc.connect(driver='SQL Server', server=sqlserver, database=sqlcounterdatabase,
                trusted_connection='yes')   
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO storage (salon,type,date,time)
        VALUES (?, ?, ?, ?)
        """,
        (recivedCode,recivedType,recivedDate,recivedTime)
        )
        conn.commit()
        conn.close()
        finalList = []
        print("WGRANO DO BAZY")
        if flag == 1:
            with open ('localdata.txt','r') as file:
                for line in file.readlines():
                    splited_line = line.split(",")
                    splited_line[-1] = splited_line[-1].strip()
                    finalList.append(splited_line)
            print(finalList)
            try:
                conn = pyodbc.connect(driver='SQL Server', server=sqlserver, database=sqlcounterdatabase,
                trusted_connection='yes')   
                cursor = conn.cursor()
                cursor.executemany("""
                INSERT INTO storage (salon,type,date,time)
                VALUES (?, ?, ?, ?)
                """,
                finalList
                )
                conn.commit()
                conn.close()
                print("Wgrano do bazy zaległe pliki, usunięte zawartosć lokalną.")
                open('localdata.txt', 'w').close()
                flag = 0
            except Exception as e:
                print(e)
                with open ('localdata.txt', 'a') as file:
                    file.write(f'{recivedCode},{recivedType},{recivedDate},{recivedTime}\n')

    except Exception as e:
        print(e)
        print("Problem z połączeniem z bazą danych...")
        flag = 1
        with open ('localdata.txt', 'a') as file:
            file.write(f'{recivedCode},{recivedType},{recivedDate},{recivedTime}\n')