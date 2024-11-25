# Counters data import connection

# Script main-final.py:

1. Connect to Mqqt broker 
2. Subscribe specific topcis
3. Insert into SQL Server database data
4. In case of db connection failure save data to local file
5. Local data is attempted to be uploaded either at the script's startup or upon successfully connecting topic data to db
