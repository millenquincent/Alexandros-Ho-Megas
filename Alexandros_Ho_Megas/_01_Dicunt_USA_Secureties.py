import pandas as pd
import mysql.connector
import numpy as np
import yfinance as yf
import sys

from _00_Datenentnahme_USA_Secureties import Namensliste_Ordnerdateien
from _103_Dicunt_Funktionen import _2002, _2003
from _103_Dicunt_Funktionen import Wörterbuch_str_Funktionen, Wörterbuch_func_Funktionen, func_Funktionen
from _104_String_Funktionen import List_Str_to_List_Funktion
from _108_MySQL_Funktionen import Tabellen_Info, Tabelle_ausgeben, PK_Name_wiedergeben, Tabellennamen_Database_aufrufen, Tabelle_erstellen_mit_Datentyp_und_Primärschlüssel, Spalte_ausgeben, Spaltennamen_anzeigen

#Nebenvariablen_____________________________________________________________________________________________________________
Δperioden   = 10

Δwertpapier = 'VZ'
Δuser       = 'root'
Δpassword   = '--'
Δport       = '3306'
Δdatabase   = '01_usa_securities'
Δtabelle    = 'neu_vz'
Δtabelle_Close    = 'neu_vz'
Δtabelle_Handelstage = '_00_handelstage'
Δspalte_Handelstage = 'handelstage'

Δdatabase_Close    = '01_usa_securities'
Δdatabase_Dicunt   = '01_usa_securities_dicunt'
Δdatabase_Extra    = '01_usa_securities_extra'

ΔPK_Spalte_Close   = 'date'
ΔPK_Spalte_Dicunt  = 'date'
ΔClose_Spalte_Close = 'close'
ΔClose_Spalte_Dicunt = 'close'

ΔTabelle_Dicuntfunktionen = '_01_dicuntfunktionen'
ΔDicunt_Codes = 'Dicunt_Codes_00' #liste der Dicunts die verwendet werden


ΔListe_Dicunt_Funktionen = Wörterbuch_func_Funktionen[ΔDicunt_Codes]   #_1002 gibt falsches format raus sql tabelle erneuern
ΔListe_Dicunt_Funktionen = [_2002, _2003]
ΔDicunt_Funktion         = ΔListe_Dicunt_Funktionen[0]
Δpfad       = 'C:/01_usa_secureties'
ΔListe_Tabellen       = Namensliste_Ordnerdateien(Δpfad)

#Datenverbindung_______________________________________________________________________________________________________
Δconnection      = mysql.connector.connect(host='localhost',user='root',password='--',port='3306')

#Hilfsfunktionen_________________________________________________________________________________________________________________

def Für_jede_tabelle_in_DB1_Tabelle_in_DB2_erstellen(Connection, Database1, Database2):
    #from _108_MySQL_Funktionen import Tabellennamen_Database_aufrufen, Datenbankverbindung, Tabelle_erstellen_mit_Datentyp_und_Primärschlüssel

    Liste_Tabellen_aus_DB1                = Tabellennamen_Database_aufrufen(Connection, Database1)
    Liste_Tabellen_aus_DB1_mit_dicunt     = [i + '_dicunt' for i in Liste_Tabellen_aus_DB1]
    Liste_Tabellen_aus_DB2                = Tabellennamen_Database_aufrufen(Connection, Database2)
    Noch_nicht_erstellte_Tabellen_inDB2   = list(set(Liste_Tabellen_aus_DB1_mit_dicunt) ^ set(Liste_Tabellen_aus_DB2))

    Tabellennamen_für_DB2 = [i for i in Noch_nicht_erstellte_Tabellen_inDB2]
    for i in range(0,len(Tabellennamen_für_DB2)):
        Tabelle_erstellen_mit_Datentyp_und_Primärschlüssel(Connection, Database2, Tabellennamen_für_DB2[i],'date','DATE')
#Für_jede_tabelle_in_DB1_Tabelle_in_DB2_erstellen(Connection, Database_Close, Database_Dicunt)

def Nächste_Handelstage_finden(Connection, Database_Close, Tabelle_Close, Database_Extra, Tabelle_Handelstage, Spalte_Handelstage, Anzahl_nächster_Handelstage):
    #from _108_MySQL_Funktionen import Tabelle_ausgeben
    Connection, Cursor  = Connection, Connection.cursor()

    Anzahl_nächster_Handelstage = Anzahl_nächster_Handelstage+1
    Letzter_Tag                 = Tabelle_ausgeben(Connection, Database_Close, Tabelle_Close)[0][-1]
    Command1                    = f"SELECT {Spalte_Handelstage} FROM {Database_Extra}.{Tabelle_Handelstage} WHERE {Spalte_Handelstage} > {Letzter_Tag} LIMIT {Anzahl_nächster_Handelstage}"
    Cursor.execute                (Command1)
    Folge_Datum                 = Cursor.fetchall()
    Folge_Datum                 = [Folge_Datum[i][0] for i in range(1,Anzahl_nächster_Handelstage)]

    return Folge_Datum
#Handelstage_Dicunt    = Nächste_Handelstage_finden(Connection, Database_Close, Tabelle_Close, Database_Extra, Tabelle_Handelstage, Spalte_Handelstage, Perioden)


def Eine_liste_in_Tabelle_hinzufügen(Connection,Database,Tabelle,Liste,Spaltenname):
    for i in range(0,len(Liste)):
        Wert    = Liste[i]

        Connection, Cursor  = Connection, Connection.cursor()
        Command1 = f"""
            INSERT INTO {Database}.{Tabelle} ({Spaltenname})
            VALUES (%s)
            ON DUPLICATE KEY UPDATE
            {Spaltenname} = %s;
        """
        values = (Wert, Wert)
        
        Cursor.execute(Command1, values)
        Connection.commit()
#Eine_liste_in_Tabelle_hinzufügen(Connection, Database_Dicunt, Tabelle_Dicunt, Handelstage_Dicunt, PK_Spalte_Dicunt)

def Spalte_von_Tabelle1_zu_Tabelle2_mittels_PK_hinzufügen(Connection,Database1,Tabelle1,PKSpalte1,Spalte1,Database2,Tabelle2,PKSpalte2,Spalte2):
    from _108_MySQL_Funktionen import Spaltennamen_anzeigen
    Connection, Cursor  = Connection, Connection.cursor()
    Spaltentyp2 = 'DECIMAL (8,3)'

    if Spalte2 not in Spaltennamen_anzeigen(Connection, Database2, Tabelle2):
        Command1        = f"ALTER TABLE {Database2}.{Tabelle2} ADD COLUMN {Spalte2} {Spaltentyp2};"
        Cursor.execute   (Command1)
        Command2        = f"UPDATE {Database2}.{Tabelle2} JOIN {Database1}.{Tabelle1} ON {Database2}.{Tabelle2}.{PKSpalte1} = {Database1}.{Tabelle1}.{PKSpalte1} SET {Database2}.{Tabelle2}.{Spalte2} = {Database1}.{Tabelle1}.{Spalte1};"
        Cursor.execute   (Command2)
        Connection.commit()
    else:
        Command1        = f"UPDATE {Database2}.{Tabelle2} JOIN {Database1}.{Tabelle1} ON {Database2}.{Tabelle2}.{PKSpalte1} = {Database1}.{Tabelle1}.{PKSpalte1} SET {Database2}.{Tabelle2}.{Spalte2} = {Database1}.{Tabelle1}.{Spalte1};"
        Cursor.execute   (Command1)
        Connection.commit()
#Spalte_von_Tabelle1_zu_Tabelle2_mittels_PK_hinzufügen(Connection, Database_Close, Tabelle_Close, PK_Spalte_Close, Close_Spalte_Close, Database_Dicunt, Tabelle_Dicunt, PK_Spalte_Dicunt, Close_Spalte_Dicunt)

def Handelstag_in_Periode_X2(Connection, Database_Close, Tabelle_Close, Database_Extra, Tabelle_Handelstage, Spalte_Handelstage, Anzahl_nächster_Handelstage):
    #from _108_MySQL_Funktionen import Tabelle_ausgeben
    Connection, Cursor  = Connection, Connection.cursor()

    Anzahl_nächster_Handelstage = Anzahl_nächster_Handelstage+1
    Letzter_Tag                 = Tabelle_ausgeben(Connection, Database_Close, Tabelle_Close)[0][-1]
    Letzter_Tag                 = '2023-11-27'
    Command1 = f"SELECT {Spalte_Handelstage} FROM {Database_Extra}.{Tabelle_Handelstage} WHERE {Spalte_Handelstage} > {Letzter_Tag} ORDER BY {Spalte_Handelstage} LIMIT 1 OFFSET 10"
    Cursor.execute(Command1)
    Tag_in_Periode = [Cursor.fetchone()[0]]
    return Tag_in_Periode
#Handelstage_Dicunt = Handelstag_in_Periode_X2(Δconnection, Δdatabase_Close, 'US58933Y1055_MRK', Δdatabase_Extra, Δtabelle_Handelstage, Δspalte_Handelstage, Δperioden)
#print(Handelstage_Dicunt)

def Handelstag_in_Periode_X(Connection, Database_Close, Tabelle_Close, Database_Extra, Tabelle_Handelstage, Spalte_Handelstage, Anzahl_nächster_Handelstage):
    #from _108_MySQL_Funktionen import Tabelle_ausgeben
    Connection, Cursor  = Connection, Connection.cursor()

    Anzahl_nächster_Handelstage = Anzahl_nächster_Handelstage+1
    Letzter_Tag                 = Tabelle_ausgeben(Connection, Database_Close, Tabelle_Close)[0][-1]
    Handelstage                 = Tabelle_ausgeben(Connection, Database_Extra, Tabelle_Handelstage)[1]
    Wert_gefunden               = np.where(Handelstage == Letzter_Tag)[0][0]
    Index_für_Periode           = Wert_gefunden+Anzahl_nächster_Handelstage-1

    Command1        = f"SELECT * FROM {Database_Extra}.{Tabelle_Handelstage} WHERE index_handelstage = {Index_für_Periode};"
    Cursor.execute  (Command1)
    result         = [Cursor.fetchone()[1]]

    return result
#Handelstage_Dicunt = Handelstag_in_Periode_X(Δconnection, Δdatabase_Close, 'US58933Y1055_MRK', Δdatabase_Extra, Δtabelle_Handelstage, Δspalte_Handelstage, Δperioden)
#print(Handelstage_Dicunt)

def Dicunt_berechnen(Connection, Database, Tabelle, Perioden, Dicunt_Funktion):
    #from _103_Dicunt_Funktionen import Lin_Regression_Gerade_vom_letzten_Wert_Dicunt
    #from _108_MySQL_Funktionen import Tabelle_ausgeben
    Connection, Cursor  = Connection, Connection.cursor()

    Close = Tabelle_ausgeben(Connection, Database, Tabelle)[3]
    Dicunt = Dicunt_Funktion(Close,Perioden)
    return Dicunt
#Dicunt_Funktion = Dicunt_berechnen(Δconnection, Δdatabase_Close, Δtabelle_Close, Δperioden, ΔListe_Dicunt_Funktionen[0])

#Dicunt_Liste          = Dicunt_Funktion[0]
#Dicunt_Code           = Dicunt_Funktion[1]
#Dicunt_Liste_Datentyp = 'DECIMAL(8,3)'
#Tabelle_Dicunt        = Tabelle_Close + '_dicunt'

def Spalte_erstellen_wenn_nicht_gibt(Connection, Database, Tabelle, Neuer_Spaltenname, Neuer_Spaltentyp): 
    from _108_MySQL_Funktionen import Spaltennamen_anzeigen
    Connection, Cursor  = Connection, Connection.cursor()

    if Neuer_Spaltenname not in Spaltennamen_anzeigen(Connection, Database, Tabelle):
        Command = f'ALTER TABLE {Database}.{Tabelle} ADD COLUMN {Neuer_Spaltenname} {Neuer_Spaltentyp}'
        Cursor.execute(Command)
        Connection.commit()
#Spalte_erstellen_wenn_nicht_gibt(Connection, Database_Dicunt, Tabelle_Dicunt, Dicunt_Code, Dicunt_Liste_Datentyp)

def Liste_zu_Tabelle_mittels_PK_hinzufügen(Connection,Database,Tabelle,PK_Liste,PK_Name,Liste,Liste_Name):
    #from _108_MySQL_Funktionen import Spaltennamen_anzeigen
    Connection, Cursor  = Connection, Connection.cursor()
    Liste_Datentyp = 'DECIMAL (8,4)'

    for i in range(0,len(Liste)):
        PK_Wert      = PK_Liste[i]
        PK_Wert      = PK_Wert.strftime('%Y-%m-%d')
        Liste_Wert   = Liste[i]

        if Liste_Name not in Spaltennamen_anzeigen(Connection, Database, Tabelle):
            Command1        = f"ALTER TABLE {Database}.{Tabelle} ADD COLUMN {Liste_Name} {Liste_Datentyp};"
            Cursor.execute   (Command1)
            Command2        = f"UPDATE {Database}.{Tabelle} SET {Liste_Name} = {Liste_Wert} where {PK_Name} = '{PK_Wert}';"
            Cursor.execute   (Command2)
            Connection.commit()
        else:
            Command1        = f"UPDATE {Database}.{Tabelle} SET {Liste_Name} = {Liste_Wert} where {PK_Name} = '{PK_Wert}';"
            Cursor.execute   (Command1)
            Connection.commit()
#Liste_zu_Tabelle_mittels_PK_hinzufügen(Connection, Database_Dicunt, Tabelle_Dicunt, Handelstage_Dicunt, PK_Spalte_Dicunt, Dicunt_Liste, Dicunt_Code)

#Eine_Tabelle______________________________________________________________________________________________________________________

def Dicunt_Tabellen_Initialisieren(Connection,          Database_Close,     Database_Dicunt, Database_Extra,   Tabelle_Close, 
                                   Tabelle_Handelstage, Spalte_Handelstage, PK_Spalte_Close, PK_Spalte_Dicunt, Close_Spalte_Close,
                                   Close_Spalte_Dicunt, Perioden):
    #from _01_Dicunt_USA_Secureties import Für_jede_tabelle_in_DB1_Tabelle_in_DB2_erstellen, Nächste_Handelstage_finden, Eine_liste_in_Tabelle_hinzufügen, Spalte_von_Tabelle1_zu_Tabelle2_mittels_PK_hinzufügen

    Tabelle_Dicunt = Tabelle_Close + '_dicunt'

    Für_jede_tabelle_in_DB1_Tabelle_in_DB2_erstellen(Connection, Database_Close, Database_Dicunt)

    Handelstage_Dicunt    = Handelstag_in_Periode_X(Connection, Database_Close, Tabelle_Close, Database_Extra, Tabelle_Handelstage, Spalte_Handelstage, Perioden)

    Eine_liste_in_Tabelle_hinzufügen(Connection, Database_Dicunt, Tabelle_Dicunt, Handelstage_Dicunt, PK_Spalte_Dicunt)

    Spalte_von_Tabelle1_zu_Tabelle2_mittels_PK_hinzufügen(Connection, Database_Close, Tabelle_Close, PK_Spalte_Close, Close_Spalte_Close, Database_Dicunt, Tabelle_Dicunt, PK_Spalte_Dicunt, Close_Spalte_Dicunt)
#print(Dicunt_Tabellen_Initialisieren(Δconnection,          Δdatabase_Close,     Δdatabase_Dicunt, Δdatabase_Extra,   Δtabelle_Close, 
#                                     Δtabelle_Handelstage, Δspalte_Handelstage, ΔPK_Spalte_Close, ΔPK_Spalte_Dicunt, ΔClose_Spalte_Close, 
#                                     ΔClose_Spalte_Dicunt, Δperioden))

def Dicunt_Tabellen_Aktualisieren(Connection,          Database_Close,     Database_Dicunt,  Database_Extra, Tabelle_Close, 
                                  Tabelle_Handelstage, Spalte_Handelstage, PK_Spalte_Dicunt, Perioden,       Liste_Dicunt_Funktionen): 
    #from _01_Dicunt_USA_Secureties import Handelstag_in_Periode_X, Dicunt_berechnen, Spalte_erstellen_wenn_nicht_gibt, Liste_zu_Tabelle_mittels_PK_hinzufügen

    for Funktion_i in Liste_Dicunt_Funktionen:
        Handelstage_Dicunt = Handelstag_in_Periode_X(Connection, Database_Close, Tabelle_Close, Database_Extra, Tabelle_Handelstage, Spalte_Handelstage, Perioden)

        Dicunt_Funktion = Dicunt_berechnen(Connection, Database_Close, Tabelle_Close, Perioden, Funktion_i)

        Dicunt_Liste          = Dicunt_Funktion[0]
        Dicunt_Code           = Dicunt_Funktion[1]
        Dicunt_Liste_Datentyp = 'DECIMAL(8,3)'
        Tabelle_Dicunt        = Tabelle_Close + '_dicunt'

        Spalte_erstellen_wenn_nicht_gibt(Connection, Database_Dicunt, Tabelle_Dicunt, Dicunt_Code, Dicunt_Liste_Datentyp)

        Liste_zu_Tabelle_mittels_PK_hinzufügen(Connection, Database_Dicunt, Tabelle_Dicunt, Handelstage_Dicunt, PK_Spalte_Dicunt, Dicunt_Liste, Dicunt_Code)
#print(Dicunt_Tabellen_Aktualisieren(Δconnection,          Δdatabase_Close,     Δdatabase_Dicunt,  Δdatabase_Extra,  Δtabelle_Close, 
#                                    Δtabelle_Handelstage, Δspalte_Handelstage, ΔPK_Spalte_Dicunt, Δperioden,        ΔListe_Dicunt_Funktionen))

#Eine_Database__________________________________________________________________________________________________________________________________

def Database_01_usa_securities_dicunt_initialisieren(Connection,          Database_Close,     Database_Dicunt, Database_Extra,  
                                                     Tabelle_Handelstage, Spalte_Handelstage, PK_Spalte_Close, PK_Spalte_Dicunt, Close_Spalte_Close,
                                                     Close_Spalte_Dicunt, Perioden,           Liste_Tabellen):
    #from _01_Dicunt_USA_Secureties import Dicunt_Tabellen_Initialisieren

    for i in range(0,len(Liste_Tabellen)):
        Tabelle_Close = Liste_Tabellen[i]
        Dicunt_Tabellen_Initialisieren(Connection,          Database_Close,     Database_Dicunt, Database_Extra,   Tabelle_Close, 
                                       Tabelle_Handelstage, Spalte_Handelstage, PK_Spalte_Close, PK_Spalte_Dicunt, Close_Spalte_Close,
                                       Close_Spalte_Dicunt, Perioden)
        print('Initialisiert',Liste_Tabellen[i],i)
    return 'end Database_01_usa_securities_dicunt_initialisieren'
#print(Database_01_usa_securities_dicunt_initialisieren(Δconnection,          Δdatabase_Close,     Δdatabase_Dicunt, Δdatabase_Extra,
#                                                       Δtabelle_Handelstage, Δspalte_Handelstage, ΔPK_Spalte_Close, ΔPK_Spalte_Dicunt, ΔClose_Spalte_Close, 
#                                                       ΔClose_Spalte_Dicunt, Δperioden,           ΔListe_Tabellen))


def Database_01_usa_securities_dicunt_aktualisieren(Liste_Tabellen,      Connection,         Database_Close,   Database_Dicunt, Database_Extra, 
                                                    Tabelle_Handelstage, Spalte_Handelstage, PK_Spalte_Dicunt, Perioden,        Liste_Dicunt_Funktionen):
    #from _01_Dicunt_USA_Secureties import Dicunt_Tabellen_Aktualisieren

    for i in range(0,len(Liste_Tabellen)):
        Tabelle_Close = Liste_Tabellen[i]
        Dicunt_Tabellen_Aktualisieren(Connection,          Database_Close,     Database_Dicunt,  Database_Extra, Tabelle_Close, 
                                      Tabelle_Handelstage, Spalte_Handelstage, PK_Spalte_Dicunt, Perioden,       Liste_Dicunt_Funktionen)
        print('aktualisiert', Tabelle_Close, i)
    return 'end Database_01_usa_securities_dicunt_aktualisieren'
#print(Database_01_usa_securities_dicunt_aktualisieren(ΔListe_Tabellen,      Δconnection,         Δdatabase_Close,   Δdatabase_Dicunt, Δdatabase_Extra, 
#                                    Δtabelle_Handelstage, Δspalte_Handelstage, ΔPK_Spalte_Dicunt, Δperioden,        ΔListe_Dicunt_Funktionen))

data_2 = pd.read_csv('C:/Users/mille/OneDrive/Desktop/Macbook Daten/Quincent Mathematik/8_Alexandos_Ho_Megas/00_Daten/00_Datenbank/00_Aktien/00_Aktien_CSV/01_usa_secureties/US7427181091_PG.csv').Close.to_numpy()
#print(Wörterbuch_func_Funktionen['Dicunt_Codes_00'][0](data_2,10))

#______________________________________________________________________________________________________________________
def friedhof():

    def Nächste_Handelstage_finden(Connection, Database_Close, Tabelle_Close, Database_Extra, Tabelle_Handelstage, Spalte_Handelstage, Anzahl_nächster_Handelstage):
        #from _108_MySQL_Funktionen import Tabelle_ausgeben
        Connection, Cursor  = Connection, Connection.cursor()

        Anzahl_nächster_Handelstage = Anzahl_nächster_Handelstage+1
        Letzter_Tag                 = Tabelle_ausgeben(Connection, Database_Close, Tabelle_Close)[0][-1]
        Command1                    = f"SELECT {Spalte_Handelstage} FROM {Database_Extra}.{Tabelle_Handelstage} WHERE {Spalte_Handelstage} > {Letzter_Tag} LIMIT {Anzahl_nächster_Handelstage}"
        Cursor.execute                (Command1)
        Folge_Datum                 = Cursor.fetchall()
        Folge_Datum                 = [Folge_Datum[i][0] for i in range(1,Anzahl_nächster_Handelstage)]

        return Folge_Datum
    #Nächste_Handelstage_finden(Δconnection, Δdatabase_Close, Δtabelle, Δdatabase_Extra, Δtab_Handelstage, Δspa_Handelstage, Δperioden)

    def Dicunt_berechnen(Connection, Database, Tabelle, Perioden, Dicunt_Funktion):
        #from _103_Dicunt_Funktionen import Lin_Regression_Gerade_vom_letzten_Wert_Dicunt
        #from _108_MySQL_Funktionen import Tabelle_ausgeben
        Connection, Cursor  = Connection, Connection.cursor()

        Close = Tabelle_ausgeben(Connection, Database, Tabelle)[3]
        Dicunt = Dicunt_Funktion(Close,Perioden)
        return Dicunt
    #Dicunt_berechnen(Δconnection, Δdatabase_Close, Δtabelle, Δperioden, ΔDicunt_Funktion)

    ΔDicunt_Funktion       = Dicunt_berechnen(Δconnection, Δdatabase_Close, Δtabelle, Δperioden, ΔDicunt_Funktion)
    ΔDicunt_Liste          = ΔDicunt_Funktion[0]
    ΔDicunt_Code           = ΔDicunt_Funktion[1]
    ΔDicunt_Liste_Datentyp = 'DECIMAL(8,3)'
    ΔHandelstage_Dicunt    = Nächste_Handelstage_finden(Δconnection, Δdatabase_Close, Δtabelle, Δdatabase_Extra, Δtabelle_Handelstage, Δspalte_Handelstage, Δperioden)

    def Spalte_erstellen_wenn_nicht_gibt(Connection, Database, Tabelle, Neuer_Spaltenname, Neuer_Spaltentyp): 
        from _108_MySQL_Funktionen import Spaltennamen_anzeigen
        Connection, Cursor  = Connection, Connection.cursor()

        if Neuer_Spaltenname not in Spaltennamen_anzeigen(Connection, Database, Tabelle):
            Command = f'ALTER TABLE {Database}.{Tabelle} ADD COLUMN {Neuer_Spaltenname} {Neuer_Spaltentyp}'
            Cursor.execute(Command)
            Connection.commit()
    #Spalte_erstellen_wenn_nicht_gibt(Δconnection, Δdatabase_Dicunt, Δtabelle_Dicunt, ΔDicunt_Code,ΔDicunt_Liste_Datentyp)

    def Liste_zu_Tabelle_mittels_PK_hinzufügen(Connection,Database,Tabelle,PK_Liste,PK_Name,Liste,Liste_Name):
        from _108_MySQL_Funktionen import Spaltennamen_anzeigen
        Connection, Cursor  = Connection, Connection.cursor()
        Liste_Datentyp = 'DECIMAL (8,4)'

        for i in range(0,len(Liste)):
            PK_Wert      = PK_Liste[i]
            PK_Wert      = PK_Wert.strftime('%Y-%m-%d')
            Liste_Wert   = Liste[i]

            if Liste_Name not in Spaltennamen_anzeigen(Connection, Database, Tabelle):
                Command1        = f"ALTER TABLE {Database}.{Tabelle} ADD COLUMN {Liste_Name} {Liste_Datentyp};"
                Cursor.execute   (Command1)
                Command2        = f"UPDATE {Database}.{Tabelle} SET {Liste_Name} = {Liste_Wert} where {PK_Name} = '{PK_Wert}';"
                Cursor.execute   (Command2)
                Connection.commit()
            else:
                Command1        = f"UPDATE {Database}.{Tabelle} SET {Liste_Name} = {Liste_Wert} where {PK_Name} = '{PK_Wert}';"
                Cursor.execute   (Command1)
                Connection.commit()
    #Liste_zu_Tabelle_mittels_PK_hinzufügen(Δconnection, Δdatabase_Dicunt, Δtabelle_Dicunt, ΔHandelstage_Dicunt,ΔPK_Name, ΔDicunt_Liste,ΔDicunt_Code)


    #_________________________________________________________________________________________________________________________________________________________________________


