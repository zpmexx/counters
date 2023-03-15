import random
from paho.mqtt import client as mqtt_client
import time
from dotenv import load_dotenv
import pyodbc 
import os
import re

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
#sqlcountertable = os.getenv('sqlcountertable')


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Udane połaczenie!")
        else:
            print("Brak połaczenia, numer błędu: %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

global i
i = 0
global flag 
flag = 0
def subscribe(client: mqtt_client):

    def on_message(client, userdata, msg):
        global i
        global finalList
        global flag
        if i < 0:
            i += 1
        else:
            # print(client,userdata)
            # print(f"Otrzymana wiadomość {msg.payload.decode()} od salonu {msg.topic}")
            recivedTime, recivedDate = msg.payload.decode().split(" ")
            print(msg.topic)
            _, recivedCode, recivedType = msg.topic.split("/")
            print("Otrzymana wiadomośc")
            print(recivedCode,recivedType,recivedDate,recivedTime)
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
                print(recivedCode,recivedType,recivedDate,recivedTime)
                if flag == 1:
                    try:
                        with open ('localdata.txt','r') as file:
                            for line in file.readlines():
                                splited_line = line.split(",")
                                splited_line[-1] = splited_line[-1].strip()
                                finalList.append(splited_line)
                    except Exception as e:
                        print(e)
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
          
            except Exception as e:
                print("Problem z połączeniem z bazą danych...")
                print(e)
                flag = 1
                with open ('localdata.txt', 'a') as file:
                    file.write(f'{recivedCode},{recivedType},{recivedDate},{recivedTime}\n')
                    
    # client.subscribe(topicIn)
    # client.subscribe(topicOut)
    # client.subscribe('A123/entrance')
    x = client.subscribe('#')



    # print(x)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()
