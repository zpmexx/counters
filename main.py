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
global finalList
finalList = []
def subscribe(client: mqtt_client):

    def on_message(client, userdata, msg):
        global i
        global finalList
        if i < 2:
            i += 1
        else:
            print(f"Otrzymana wiadomość {msg.payload.decode()} od salonu {msg.topic}")
            recivedTime, recivedDate = msg.payload.decode().split(" ")
            recivedCode, recivedType = msg.topic.split("/")
            finalList.append([recivedCode,recivedType,recivedDate,recivedTime])
            if len(finalList) > 9:
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
                finalList = []
                print("WGRANO DO BAZY")

    client.subscribe(topicIn)
    client.subscribe(topicOut)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()
