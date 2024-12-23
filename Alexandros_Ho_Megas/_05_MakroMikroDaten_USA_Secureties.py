from fredapi import Fred
import requests, os, pandas as pd, mysql.connector, numpy as np, time, yfinance as yf, sys
from datetime import datetime,timedelta, date


Δconnection = mysql.connector.connect(host='localhost',user='root',password='--',port='3306')
Δdatabase   = '01_usa_securities_extra'
Δtabelle    =  '_03_fred_interest_rates'


def Interest_Rate_Anfangstabelle_erstellen():
    def Tabelle_erstellen_mit_Datentyp_und_Primärschlüssel(Connection, Database, Tabellenname, Spaltenname, Spaltendatentyp):
        #from _108_MySQL_Funktionen import Datenbankverbindung
        Connection, Cursor  = Connection, Connection.cursor()

        create_table_query = f"""CREATE TABLE {Database}.{Tabellenname} ({Spaltenname} {Spaltendatentyp} PRIMARY KEY)"""
        Cursor.execute(create_table_query)
        Connection.commit()
    #Tabelle_erstellen_mit_Datentyp_und_Primärschlüssel(Δconnection, '01_usa_securities_extra', '_03_FRED_Interest_Rates', 'date', 'DATE')

    def Spalte_Tabelle_hinzufügen(Connection,Database,Tabelle,Neuer_Spaltenname,Neuer_Spaltentyp):
        from _108_MySQL_Funktionen import Spaltennamen_anzeigen
        Connection, Cursor  = Connection, Connection.cursor()

        if Neuer_Spaltenname not in Spaltennamen_anzeigen(Connection, Database, Tabelle):    #21.11.23 statt in, not in gesetzt
            Command = f'ALTER TABLE {Database}.{Tabelle} ADD COLUMN {Neuer_Spaltenname} {Neuer_Spaltentyp}'
            Cursor.execute(Command)
            Connection.commit()
    #Spalte_Tabelle_hinzufügen(Δconnection,'01_usa_securities_extra','_03_FRED_Interest_Rates','fred_interest_rate','DECIMAL(4,2)')

    return 'Anfangstabelle Interest Rate erstellt'
#print(Interest_Rate_Anfangstabelle_erstellen())

def Fred_Interest_Rate_Tabelle_initialisieren(Connection, Database, Tabelle):
    #Tabelle mit zwei spalten muss schon da sein
    fred                          = Fred(api_key='--')
    data                          = fred.get_series('FEDFUNDS')
    Fred_interest_Rate_df         = data.reset_index()
    Fred_interest_Rate_df.columns = ['date', 'fred_interest_rate']

    Connection, Cursor  = Connection, Connection.cursor()

    for index, row in Fred_interest_Rate_df.iterrows():
        sql = f"INSERT INTO {Database}.{Tabelle} (date, fred_interest_rate) VALUES (%s, %s)"
        Cursor.execute(sql, (row['date'], row['fred_interest_rate']))
    Connection.commit()
    return f'FRED Daten aktualisiert bis {Fred_interest_Rate_df.iloc[-1,0]}'
#print(Fred_Interest_Rate_Tabelle_initialisieren(Δconnection, Δdatabase, Δtabelle))

def Fred_Interest_Rate_Tabelle_aktualisieren(Connection, Database, Tabelle, Spalte, Primärschlüssel):
    Cursor  = Connection.cursor()

    fred       = Fred(api_key='--')
    data       = fred.get_series('FEDFUNDS')
    Fred_interest_Rate_df         = data.reset_index()
    Fred_interest_Rate_df.columns = ['date', 'fred_interest_rate']
    Anzahl_letzte_monate = 5

    for Index, df_data in Fred_interest_Rate_df.tail(Anzahl_letzte_monate).iterrows():
        date = df_data.iloc[0].to_pydatetime()
        Interest_Rate = df_data.iloc[1]
        command = f'''INSERT INTO {Database}.{Tabelle} ({Primärschlüssel}, {Spalte}) VALUES (%s, %s) ON DUPLICATE KEY UPDATE {Spalte} = %s;'''
        Cursor.execute(command, (date, Interest_Rate, Interest_Rate))
        Connection.commit()

    Connection.close()
    return f'Fred Interest Rates von {date} bis {Anzahl_letzte_monate} Monate in Vergangenheit aktualisiert'
#print(Fred_Interest_Rate_Tabelle_aktualisieren(Δconnection, Δdatabase, Δtabelle, "fred_interest_rate", "date"))


def Friedof():
    fred       = Fred(api_key='--')
    data       = fred.get_series('FEDFUNDS')
    Fred_interest_Rate_df         = data.reset_index()
    Fred_interest_Rate_df.columns = ['date', 'fred_interest_rate']
    ΔFred_interest_Rate_df = Fred_interest_Rate_df