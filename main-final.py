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

databaseuserpassword = os.getenv('databaseuserpassword')
databaseuserlogin = os.getenv('databaseuserlogin')

topicIn = os.getenv('topicIn')
topicOut = os.getenv('topicOut')
client_id = os.getenv('client_id')

sqlserver = os.getenv('sqlserver')
sqlcounterdatabase = os.getenv('sqlcounterdatabase')
#sqlcountertable = os.getenv('sqlcountertable')

sql_folder_path = os.listdir(path='/opt/microsoft/msodbcsql17/lib64/')

for file in sql_folder_path:
    if file.startswith("libmsodbcsql"):
        sql_file = file
        break

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
finalListMain = []
def subscribe(client: mqtt_client):

    def on_message(client, userdata, msg):
        global flag
    
        # print(client,userdata)
        # print(f"Otrzymana wiadomość")
        # print(msg.payload.decode())
        # print(msg.topic)
        
        onlineOfflineList = msg.topic.split("/")
        
        

        if len(onlineOfflineList) == 2:
            recivedTime, recivedDate = msg.payload.decode().split("/")
            recivedDate = recivedDate.strip()
            _, recivedCode = msg.topic.split("/")
            # print(msg.topic)
            # print(msg.payload.decode())


        #do testow
        # salonList = ['A500','A069','A100','A122','A154','C043','D068']
        # recivedCode = random.choice(salonList)
    
        # print("Po podziale na kod, date, czas")
        # print(f'kod: {recivedCode}\ndata: {recivedDate}\nczas: {recivedTime}')
        #finalListMain.append([recivedCode,recivedDate,recivedTime])
        # print(len(finalListMain))
            try:
                #conn = pyodbc.connect(DRIVER='/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.0.so.1.1', server=sqlserver, database=sqlcounterdatabase,
                #       trusted_connection='yes')   
                #conn = pyodbc.connect('DRIVER={/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.5.1};SERVER='+sqlserver+';DATABASE='+sqlcounterdatabase+';UID='+databaseuserlogin+';PWD='+databaseuserpassword)
                conn = pyodbc.connect('DRIVER={/opt/microsoft/msodbcsql17/lib64/'+sql_file+'};SERVER='+sqlserver+';DATABASE='+sqlcounterdatabase+';UID='+databaseuserlogin+';PWD='+databaseuserpassword)
                cursor = conn.cursor()
                cursor.execute("""
                        INSERT INTO storage (salon,date,time,dateFinish,timeFinish,entersCount)
                        VALUES (?, ?, ?,?,?,?)
                        """,
                        (recivedCode,recivedDate,recivedTime,recivedDate,recivedTime,1)
                        )
                conn.commit()
                conn.close()
                finalList = []
                # print("WGRANO DO BAZY po podziale na kod, data, czas")
                # print(recivedCode,recivedDate,recivedTime)
                if flag == 1:
                    try:
                        with open ('localdata.txt','r') as file:
                            for line in file.readlines():
                                splited_line = line.split(",")
                                splited_line[-1] = splited_line[-1].strip()
                                finalList.append(splited_line)
                    except Exception as e:
                        print("problem z otwarciem pliku localdata")
                        print(e)
                    try:
                        #conn = pyodbc.connect('DRIVER={/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.5.1};SERVER='+sqlserver+';DATABASE='+sqlcounterdatabase+';UID='+databaseuserlogin+';PWD='+databaseuserpassword)
                        conn = pyodbc.connect('DRIVER={/opt/microsoft/msodbcsql17/lib64/'+sql_file+'};SERVER='+sqlserver+';DATABASE='+sqlcounterdatabase+';UID='+databaseuserlogin+';PWD='+databaseuserpassword)
                        cursor = conn.cursor()
                        cursor.executemany("""
                        INSERT INTO storage (salon,date,time,dateFinish,timeFinish,entersCount)
                        VALUES (?, ?, ?,?,?,?)
                        """,
                        finalList
                        )
                        conn.commit()
                        conn.close()
                        open('localdata.txt', 'w').close()
                        print("Wgrano do bazy zaległe pliki, usunięte zawartosć lokalną.")
                        flag = 0
                    except Exception as e:
                        print("Problem z wgraniem z pliku localdata")
                        print(e)
                        pass
        
            except Exception as e:
                print("Problem z połączeniem z bazą danych...")
                print(e)
                flag = 1
                with open ('localdata.txt', 'a') as file:
                    file.write(f'{recivedCode},{recivedDate},{recivedTime},{recivedDate},{recivedTime},1\n')
                    
        else: 
            # print("tu wrzucamy offline")
            # print("topic")
            # print(msg.topic)
            _,_, recivedCode = msg.topic.split("/")
            # print("wiadomosc")
            # print(msg.payload.decode())
            datesEntersList = msg.payload.decode().split(" ")
            # print("lista po podziale")
            # print(datesEntersList)
            entryTime, entryDate = datesEntersList[1].split('/')
            finishTime, finishDate = datesEntersList[0].split('/')
            entersCount = datesEntersList[2]
            entryDate = entryDate.strip()
            finishDate = finishDate.strip()
            # print("dane wejsciowe")
            # print(entryDate,entryTime)
            # print("dane wyjsciowe")
            # print(finishDate,finishTime)
            # print("liczba wejsc")
            # print(entersCount)
            try:
                #conn = pyodbc.connect(DRIVER='/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.0.so.1.1', server=sqlserver, database=sqlcounterdatabase,
                #       trusted_connection='yes')   
                #conn = pyodbc.connect('DRIVER={/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.5.1};SERVER='+sqlserver+';DATABASE='+sqlcounterdatabase+';UID='+databaseuserlogin+';PWD='+databaseuserpassword)
                conn = pyodbc.connect('DRIVER={/opt/microsoft/msodbcsql17/lib64/'+sql_file+'};SERVER='+sqlserver+';DATABASE='+sqlcounterdatabase+';UID='+databaseuserlogin+';PWD='+databaseuserpassword)
                cursor = conn.cursor()
                cursor.execute("""
                        INSERT INTO storage (salon,date,time,dateFinish,timeFinish,entersCount)
                        VALUES (?, ?, ?,?,?,?)
                        """,
                        (recivedCode,entryDate,entryTime,finishDate,finishTime,entersCount)
                        )
                conn.commit()
                conn.close()
                finalList = []
            except Exception as e:
                print("Problem z połączeniem z bazą danych...")
                print(e)
                #tu wrzucenie do localdaty
                flag = 1
                with open ('localdata.txt', 'a') as file:
                    file.write(f'{recivedCode},{entryDate},{entryTime},{finishDate},{finishTime},{entersCount}\n')
          
            
    x = client.subscribe('counters/#')
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    if os.stat('localdata.txt').st_size != 0:
        firstList = []
        try:
            with open ('localdata.txt','r') as file:
                for line in file.readlines():
                    splited_line = line.split(",")
                    splited_line[-1] = splited_line[-1].strip()
                    firstList.append(splited_line)
            #conn = pyodbc.connect('DRIVER={/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.5.1};SERVER='+sqlserver+';DATABASE='+sqlcounterdatabase+';UID='+databaseuserlogin+';PWD='+databaseuserpassword)
            conn = pyodbc.connect('DRIVER={/opt/microsoft/msodbcsql17/lib64/'+sql_file+'};SERVER='+sqlserver+';DATABASE='+sqlcounterdatabase+';UID='+databaseuserlogin+';PWD='+databaseuserpassword)
            cursor = conn.cursor()
            cursor.executemany("""
            INSERT INTO storage (salon,date,time,dateFinish,timeFinish,entersCount)
            VALUES (?, ?, ?,?,?,?)
            """,
            firstList
            )
            conn.commit()
            conn.close()
            print("Wgrano do bazy zaległe pliki po otwarciu uslugi, usunięte zawartosć lokalną.")
            open('localdata.txt', 'w').close()
            flag = 0
        except Exception as e:
            print(e)
    run()
