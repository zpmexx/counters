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

if not sql_file:
    print("Nie można znaleźć pliku sql w mqtt.")

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Udane połaczenie!")
        else:
            print("Brak połaczenia, numer błędu: %d\n", rc)
    try:
        client = mqtt_client.Client(client_id)
        client.username_pw_set(username, password)
        client.on_connect = on_connect
        client.connect(broker, port)
        return client
    except exception as e:
        print("Problem z połaczeniem z mqtt.")
        print(e)

global i
i = 0
global flag 
flag = 0
finalListMain = []
def subscribe(client: mqtt_client):

    def on_message(client, userdata, msg):
        global flag
    

        try:
            onlineOfflineList = msg.topic.split("/")
        except exception as e:
            print("Problem z podziałem danych przy odbieraniu wiadomsoci (podział przez /). Poniżej otrzymana wiadomość")
            print(msg.topic)
            print(e)
        
        

        if len(onlineOfflineList) == 2:
            try:
                recivedTime, recivedDate = msg.payload.decode().split("/")
                recivedDate = recivedDate.strip()
                _, recivedCode = msg.topic.split("/")
            except exception as e:
                print("problem z podziałem danych online")
                print(e)
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
            ### print("tu wrzucamy offline")
            try:
                _,_, recivedCode = msg.topic.split("/")

                datesEntersList = msg.payload.decode().split(" ")
                entryTime, entryDate = datesEntersList[1].split('/')
                finishTime, finishDate = datesEntersList[0].split('/')
                entersCount = datesEntersList[2]
                entryDate = entryDate.strip()
                finishDate = finishDate.strip()
            except exception as e:
                print("Problem z podziałem danych online.")
                print(e)
                
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
          
    try:        
        x = client.subscribe('counters/#')
        client.on_message = on_message
    except exception as e:
        print("problem z nasluchiwaniem")
        print(e)


def run():
    try:
        client = connect_mqtt()
        subscribe(client)
        client.loop_forever()
    except exception as e:
        print("problem z nawiazaniem pierwszego polaczenia z mqtt")
        print(e)


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
