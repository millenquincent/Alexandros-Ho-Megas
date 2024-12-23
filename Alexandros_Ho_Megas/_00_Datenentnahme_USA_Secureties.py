import requests, os, pandas as pd, mysql.connector, numpy as np, time, yfinance as yf

from _108_MySQL_Funktionen import Format_Spalte_ändern,Primärschlüssel_Tabelle_setzen,Spalte_Tabelle_hinzufügen






#Hauptvariablen________________________________________________________________________________________________________
Δdatabase             = '01_usa_securities'
Δtabelle              = 'neu_vz'

#Verbindung____________________________________________________________________________________________________________
Δconnection           = mysql.connector.connect(host='localhost',user='root',password='--',port='3306')

#Nebenvariablen________________________________________________________________________________________________________
ΔFormatierte_Spalte   = 'Volume'
ΔNeues_Format         = 'int'
ΔPK_Spalte            = 'date'
ΔNeuer_Spaltenname    = 'Renditen'
ΔNeuer_Spaltentyp     = 'DECIMAL(6,3)'
ΔTage_str             = '100'
ΔSpaltenname_Renditen = 'Renditen'
ΔSpaltenname_Close    = 'Close'
Δpfad                 = 'C:/01_usa_secureties'

#Spezifische_Ausgabe_Funktionen_____________________________________________________________________________________________________

def Namensliste_Ordnerdateien(folder_path):
    import os    #IT funktion
    #from _108_MySQL_Funktionen import Letzte_vier_Ziffer_löschen

    file_list = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_list.append(Letzte_vier_Ziffer_löschen(file))
    return file_list
#print(Namensliste_Ordnerdateien(Δpfad))

def Tabellen_Abkürzung(Tabelle):
    #Tabelle = 'us0258161092_axp.csv'
    #Tabelle = 'us0258161092_axp' mit und ohne .csv geht!

    if len(Tabelle) == 3 or len(Tabelle) == 2:
        return Tabelle
    letzte_4_Ziffer = Tabelle[-4]+Tabelle[-3]+Tabelle[-2]+Tabelle[-1]
    if letzte_4_Ziffer == '.csv':
        Tabelle = Tabelle[:-4]
        Abbreviation = ''
        for t in range(1,len(Tabelle)):
            if Tabelle[-t] != '_': Abbreviation = Abbreviation+Tabelle[-t]
            else: break
        return Abbreviation[::-1]
    else:
        Abbreviation = ''
        for t in range(1,len(Tabelle)):
            if Tabelle[-t] != '_': Abbreviation = Abbreviation+Tabelle[-t]
            else: break
        return Abbreviation[::-1]
#print(Tabellen_Abkürzung(Δtabelle))

def Letzte_vier_Ziffer_löschen(Tabelle):
    #Tabelle = 'vfverver_axp.csv'
    return Tabelle[:-4]
#print(Letzte_vier_Ziffer_löschen('vfverver_axp.csv'))

#Spezifische_Tabellenänderung_Funktionen_____________________________________________________________________________________________________

def Spalten_mit_Daten_befüllen(Connection, Database, Tabelle, Spalte, Spalte2, Liste, Liste2):
    #spalten sind spalten namen und die Listen pythonlisten
    Connection, Cursor  = Connection, Connection.cursor()

    insert_query = f"INSERT INTO {Database}{Tabelle} ({Spalte2}, {Spalte}) VALUES (%s, %s)"

    for i in range(0,len(Liste)):
        Cursor.execute(insert_query, (Liste2[i],Liste[i]))
    Connection.commit()
#Spalten_mit_Daten_befüllen(Δconnection, Δdatabase, Δtabelle, Δspalte, Δspalte2, Δliste, Δliste2,)

def Fehlerhafte_Zellen_überschreiben(Connection,Database,Tabelle):
    pass
#print(Fehlerhafte_Zellen_überschreiben(Δconnection,Δdatabase,Δtabelle))

def Daten_Tabelle_mit_yfinance_aktualisieren(Connection,Database,Tabelle,Tage_str):
    #from _108_MySQL_Funktionen import Tabellen_Abkürzung
    #Tage_str='10' und Tabelle = 'us0258161092_axp'

    Abkürzung = Tabellen_Abkürzung(Tabelle)
    Wertpapierdaten = yf.Ticker(Abkürzung).history(period=Tage_str+'d')
    for i in range(0,len(Wertpapierdaten)):
        Date   = Wertpapierdaten.index[i].date().strftime('%Y-%m-%d')  #Datenentnahme yfinance
        High   = round(Wertpapierdaten.iloc[i,1],3)
        Low    = round(Wertpapierdaten.iloc[i,2],3)
        Close  = round(Wertpapierdaten.iloc[i,3],3)
        Volume = int  (Wertpapierdaten.iloc[i,4]  )

        Connection, Cursor  = Connection, Connection.cursor()                                       #in mysql Tabelle hinzufügen
        Command1 = f"""
            INSERT INTO {Database}.{Tabelle} (Date, High, Low, Close, Volume)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            Date = %s, High = %s, Low = %s, Close = %s, Volume = %s;
        """
        values = (Date, High, Low, Close, Volume, Date, High, Low, Close, Volume)
        
        Cursor.execute(Command1, values)
        Connection.commit()
#Daten_Tabelle_mit_yfinance_aktualisieren(Δconnection,Δdatabase,Δtabelle,ΔTage_str)

def Renditen_Spalte_berechnen_einsetzen(Connection,Database,Tabelle,Spaltenname_Renditen,Spaltenname_Close,PK_Spalte):
    Connection, Cursor  = Connection, Connection.cursor()

    Command1_Renditenwerte_hinzufügen = f"""
    UPDATE {Database}.{Tabelle} AS t
    JOIN (
    SELECT 
        {PK_Spalte},
        (({Spaltenname_Close} / LAG({Spaltenname_Close}) OVER (ORDER BY {PK_Spalte})) - 1) AS {Spaltenname_Renditen}
    FROM {Database}.{Tabelle}
    ) AS subquery
    ON t.{PK_Spalte} = subquery.{PK_Spalte}
    SET t.{Spaltenname_Renditen} = subquery.{Spaltenname_Renditen};
    """
    Cursor.execute(Command1_Renditenwerte_hinzufügen)
    Connection.commit()

    Command2_Nullwerte_ersetzen = f"UPDATE {Database}.{Tabelle} SET {Spaltenname_Renditen} = 0 WHERE {Spaltenname_Renditen} IS NULL"
    Cursor.execute(Command2_Nullwerte_ersetzen)
    Connection.commit()
#Renditen_Spalte_berechnen_einsetzen(Δconnection,Δdatabase,Δtabelle,ΔSpaltenname_Renditen,ΔSpaltenname_Close,ΔPK_Spalte)

#Hauptfunktion_Eine_Tabelle______________________________________________________________________________________________________________
#in axp wurde der PK nicht gesetzt und das format von text zu date nicht geändert
p = 'US0258161092_AXP'

def Tabelle_01_usa_secureties_verarbeiten_initialisieren(Connection,Database,Tabelle,Formatierte_Spalte,Neues_Format,PK_Spalte,Neuer_Spaltenname,Neuer_Spaltentyp,Spaltenname_Renditen,Spaltenname_Close):
    #from _108_MySQL_Funktionen import Fehlerhafte_Zellen_überschreiben, Format_Spalte_ändern,Primärschlüssel_Tabelle_setzen,Spalte_Tabelle_hinzufügen,Renditen_Spalte_berechnen_einsetzen

    Fehlerhafte_Zellen_überschreiben     (Connection,Database,Tabelle)
    Format_Spalte_ändern                 (Connection,Database,Tabelle,Formatierte_Spalte,Neues_Format)
    Primärschlüssel_Tabelle_setzen       (Connection,Database,Tabelle,PK_Spalte)
    Spalte_Tabelle_hinzufügen            (Connection,Database,Tabelle,Neuer_Spaltenname,Neuer_Spaltentyp)
    Renditen_Spalte_berechnen_einsetzen  (Connection,Database,Tabelle,Spaltenname_Renditen,Spaltenname_Close,PK_Spalte)
#Tabelle_01_usa_secureties_verarbeiten_initialisieren(Δconnection,Δdatabase,Δtabelle,ΔFormatierte_Spalte,ΔNeues_Format,ΔPK_Spalte,ΔNeuer_Spaltenname,ΔNeuer_Spaltentyp,ΔSpaltenname_Renditen,ΔSpaltenname_Close)

def Tabelle_01_usa_secureties_verarbeiten_aktualisieren(Connection,database,Tabelle,Tage_str,Spaltenname_Renditen,Spaltenname_Close,PK_Spalte):

    #from _108_MySQL_Funktionen import Daten_Tabelle_mit_yfinance_aktualisieren, Renditen_Spalte_berechnen_einsetzen

    Daten_Tabelle_mit_yfinance_aktualisieren  (Connection,database,Tabelle,Tage_str)
    Renditen_Spalte_berechnen_einsetzen       (Connection,database,Tabelle,Spaltenname_Renditen,Spaltenname_Close,PK_Spalte)
#Tabelle_01_usa_secureties_verarbeiten_aktualisieren(Δconnection,Δdatabase,Δtabelle,ΔTage_str,ΔSpaltenname_Renditen,ΔSpaltenname_Close,ΔPK_Spalte)

#Ende_________________________________________________________________________________________________________________________
