import requests, os, pandas as pd, mysql.connector, numpy as np, time, yfinance as yf
from datetime import datetime,timedelta, date






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
ΔTage_str             = '10'
ΔSpaltenname_Renditen = 'Renditen'
ΔSpaltenname_Close    = 'Close'
Δpfad                 = 'C:/01_usa_secureties'

#Ausgabe_Funktionen_____________________________________________________________________________________________________

def Tabellen_Info(Connection,Database,Tabelle):
    Connection, Cursor  = Connection, Connection.cursor()

    query               = f"DESCRIBE {Database}.{Tabelle}"
    Cursor.execute        (query)
    column_info         = Cursor.fetchall()
    return column_info
#print(Tabellen_Info(Δconnection,Δdatabase,Δtabelle))

def PK_Name_wiedergeben(Connection, Database_Close, Tabelle):
    #from _108_MySQL_Funktionen import Tabellen_Info
    Tabelleninfo = Tabellen_Info(Connection, Database_Close, Tabelle)

    for i in range(0,len(Tabelleninfo)):
        if Tabelleninfo[i][3] == 'PRI':
            return Tabelleninfo[i][0]
#print(PK_Name_wiedergeben(Δconnection,Δdatabase,Δtabelle))

def Spaltennamen_anzeigen(Connection, Database, Tabellenname):
    Connection, Cursor  = Connection, Connection.cursor()

    sql_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{Database}' AND TABLE_NAME = '{Tabellenname}'"
    Cursor.execute(sql_query)
    result = Cursor.fetchall()
    column_names = [row[0] for row in result]
    return column_names
#print(Spaltennamen_anzeigen(Δconnection, Δdatabase, Δtabelle))

def Name_und_Spalten_ausgeben(Connection, Database, Tabelle):
    import warnings
    warnings.simplefilter("ignore", UserWarning)
    Connection, Cursor = Connection, Connection.cursor()

    Cursor.execute(f"DESCRIBE {Database}.{Tabelle}")
    column_info = Cursor.fetchall()
    columns = [info[0] for info in column_info]

    df = pd.read_sql(f"SELECT * FROM {Database}.{Tabelle}", Connection)
    Spaltenliste = [np.array(columns)]
    Tabellenliste = [df[col].to_numpy() for col in columns]

    return Spaltenliste + Tabellenliste
#print(Name_und_Spalten_ausgeben(Δconnection, '01_usa_securities', 'us0970231058_ba'))

def Tabelle_ausgeben(Connection,Database,Tabelle):
    #import numpy as np
    Connection, Cursor  = Connection, Connection.cursor()

    Cursor.execute (f'SELECT * FROM {Database}.{Tabelle}')
    CSCO          = Cursor.fetchall()
    Tabellenliste = []
    for i in range(0,len(CSCO[0])): Tabellenliste.append(np.array([row[i] for row in CSCO]))
    return Tabellenliste
#print(Tabelle_ausgeben(Δconnection,Δdatabase,Δtabelle))

def Tabelle_ausgeben_pddf(Connection, Database, Tabelle):
    Cursor  = Connection.cursor()
    query   = f"SELECT * FROM {Database}.{Tabelle}"
    Cursor  .execute(query)
    columns = [desc[0] for desc in Cursor.description]
    data    = Cursor.fetchall()
    Tabelle = pd.DataFrame(data, columns=columns)

    return Tabelle
#print(Tabelle_ausgeben_pddf(Δconnection,Δdatabase_Close,Δtabelle_Close))

def Tabelle_Spaltennamen_ausgeben(Connection, Database, Tabelle):
    Connection, Cursor  = Connection, Connection.cursor()

    Cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{Database}' AND TABLE_NAME = '{Tabelle}'")
    Spalten = Cursor.fetchall()
    Tabellenliste = [np.array([row[0] for row in Spalten])]

    Cursor.execute (f'SELECT * FROM {Database}.{Tabelle}')
    Inhalt          = Cursor.fetchall()
    for i in range(0,len(Inhalt[0])): Tabellenliste.append(np.array([row[i] for row in Inhalt]))
    
    return Tabellenliste
#print(Tabelle_Spaltennamen_ausgeben(Δconnection, Δdatabase, Δtabelle))

def Spalte_ausgeben(Connection,Database,Tabelle, Spalte):
    Connection, Cursor  = Connection, Connection.cursor()

    Cursor.execute (f'SELECT {Spalte} FROM {Database}.{Tabelle}')
    Spaltenwerte_komisch = Cursor.fetchall()
    Spaltenwerte = np.array([value for tup in Spaltenwerte_komisch for value in tup])

    return Spaltenwerte
#print(Spalte_ausgeben(Δconnection,Δdatabase,Δtabelle,ΔSpaltenname_Close))

def Tabellennamen_Database_aufrufen(Connection, Database):
    #gibt alle Tabellennamen der Database wieder in einer Liste
    Connection, Cursor  = Connection, Connection.cursor()

    Command1            = "SELECT table_name FROM information_schema.tables WHERE table_schema = %s"
    Cursor.execute        (Command1 , (Database,))

    Tabellennamen       = Cursor.fetchall()
    Liste_Tabellennamen = []
    for i in range(0,len(Tabellennamen)): Liste_Tabellennamen.append(Tabellennamen[i][0]) 

    return Liste_Tabellennamen
#print(Tabellennamen_Database_aufrufen(Δconnection,Δdatabase))

def Spaltenname_und_Spaltentyp_vom_index(Connection, Database, Tabelle):
    #from _108_MySQL_Funktionen import Tabellen_Info

    TabellenInformation = Tabellen_Info(Connection, Database, Tabelle)
    for i in range(0,len(TabellenInformation)):
        if TabellenInformation[i][3] == 'PRI':
            Spaltenname = TabellenInformation[i][0]
            Spaltentyp = TabellenInformation[i][1].decode('utf-8')
            return Spaltenname, Spaltentyp
#ΔSpaltenname_Spaltentyp = Spaltenname_und_Spaltentyp_vom_index(Δconnection, ΔDatabase_Dicunt, ΔTabelle_Dicunt)

def Alle_Spaltennamen_und_Spaltentypen_ausgeben(Connection, Database, Tabelle):
    Tabelleninfo = Tabellen_Info(Connection, Database, Tabelle)
    Liste_Spaltennamen = [i[0] for i in Tabelleninfo]
    Liste_Spaltentypen = [i[1].decode('utf-8') for i in Tabelleninfo]
    return Liste_Spaltennamen, Liste_Spaltentypen
#print(Alle_Spalten_und_Spaltentypen_ausgeben(Δconnection, ΔDatabase_Dicunt, ΔTabelle_Dicunt))

#Tabellenänderung_Funktionen_______________________________________________________________________________________________________

def Einen_Wert_Tabelle_hinzufügen(Connection, Database, Tabelle, Wert, Index, Index_Wert, Spalten_Wert):
    Connection, Cursor  = Connection, Connection.cursor()
    Command             = f"UPDATE {Database}.{Tabelle} SET {Spalten_Wert} = '{Wert}' WHERE {Index} = {Index_Wert}"
    Cursor.execute(Command); Connection.commit(); Cursor.close(); Connection.close() 
#Einen_Wert_Tabelle_hinzufügen(Δconnection, 'mydb', 'employees', 'Pferd', 'employee_id', '2', 'first_name')

def Einen_Wert_Tabelle_hinzufügen2(Connection, Database, Tabelle, Wert, Spalte, Primärschlüssel, PK_wert):
    Connection, Cursor  = Connection, Connection.cursor()

    command = f'''INSERT INTO {Database}.{Tabelle} ({Primärschlüssel}, {Spalte}) VALUES ({PK_wert}, '{Wert}') ON DUPLICATE KEY UPDATE {Spalte} = '{Wert}';'''
    Cursor.execute(command)
    Connection.commit()
#Einen_Wert_Tabelle_hinzufügen2(Δconnection, 'mydb', 'employees', 'Goldfisch','first_name', 'employee_id', '9')

def Tabelle_erstellen_mit_Datentyp_und_Primärschlüssel(Connection, Database, Tabellenname, Spaltenname, Spaltendatentyp):
    #from _108_MySQL_Funktionen import Datenbankverbindung
    Connection, Cursor  = Connection, Connection.cursor()

    create_table_query = f"""CREATE TABLE {Database}.{Tabellenname} ({Spaltenname} {Spaltendatentyp} PRIMARY KEY)"""
    Cursor.execute(create_table_query)
    Connection.commit()
#Tabelle_erstellen_mit_Datentyp_und_Primärschlüssel(Δconnection, '01_usa_securities_extra', 'xx', 'pp', 'DATE')

def Format_Spalte_ändern(Connection,Database,Tabelle,Formatierte_Spalte,Neues_Format):
    #from _108_MySQL_Funktionen import Tabellen_Abkürzung
    Connection, Cursor  = Connection, Connection.cursor()

    Command1            = f'alter table {Database}.{Tabelle} modify {Formatierte_Spalte} {Neues_Format};'
    Cursor.execute       (Command1)
    Connection.commit    ()
#Format_Spalte_ändern(Δconnection,Δdatabase,Δtabelle,ΔFormatierte_Spalte,ΔNeues_Format)

def Primärschlüssel_Tabelle_setzen(Connection,Database,Tabelle,PK_Spalte):
    #from _108_MySQL_Funktionen import Tabelle_Info
    Connection, Cursor  = Connection, Connection.cursor()

    Tabelle_Info = Tabellen_Info(Connection,Database,Tabelle)
    if all(item[3] == '' for item in Tabelle_Info):
        for i in range(0,len(Tabelle_Info)):
            if Tabelle_Info[i][0] == PK_Spalte:
                Command        = f'alter table {Database}.{Tabelle} add primary key ({PK_Spalte});'
                Cursor.execute    (Command)
                Connection.commit()
    else: pass
#Primärschlüssel_Tabelle_setzen(Δconnection,Δdatabase,Δtabelle,ΔPK_Spalte)

def Primärschlüssel_Tabelle_setzen_EINFACH(Connection,Database,Tabelle,PK_Spalte):
    Connection, Cursor  = Connection, Connection.cursor()

    Command        = f'alter table {Database}.{Tabelle} add primary key ({PK_Spalte});'
    Cursor.execute    (Command)
    Connection.commit()
#Primärschlüssel_Tabelle_setzen_EINFACH(Δconnection,Δdatabase,Δtabelle,'index_handelstage')
   
def Spalte_Tabelle_hinzufügen(Connection,Database,Tabelle,Neuer_Spaltenname,Neuer_Spaltentyp):
    #from _108_MySQL_Funktionen import Spaltennamen_anzeigen
    Connection, Cursor  = Connection, Connection.cursor()

    if Neuer_Spaltenname not in Spaltennamen_anzeigen(Connection, Database, Tabelle):    #21.11.23 statt in, not in gesetzt
        Command = f'ALTER TABLE {Database}.{Tabelle} ADD COLUMN {Neuer_Spaltenname} {Neuer_Spaltentyp}'
        Cursor.execute(Command)
        Connection.commit()
#Spalte_Tabelle_hinzufügen(Δconnection,Δdatabase,Δtabelle,ΔNeuer_Spaltenname,ΔNeuer_Spaltentyp)

def Mehrere_Spalten_Tabelle_hinzufügen(Connection,Database,Tabelle,Liste_Neuer_Spaltenname,Liste_Neuer_Spaltentyp):
    #from _108_MySQL_Funktionen import Spaltennamen_anzeigen
    #Die Listen müssen 1zu1 passen für Namen und Datentyp
    Connection, Cursor  = Connection, Connection.cursor()

    for i in range(0,len(Liste_Neuer_Spaltenname)):
        if Liste_Neuer_Spaltenname[i] not in Spaltennamen_anzeigen(Connection, Database, Tabelle):
            Command = f'ALTER TABLE {Database}.{Tabelle} ADD COLUMN {Liste_Neuer_Spaltenname[i]} {Liste_Neuer_Spaltentyp[i]}'
            Cursor.execute(Command)
            Connection.commit()
#Mehrere_Spalten_Tabelle_hinzufügen(Δconnection, ΔDatabase_Warscheinlichkeit, ΔTabelle_Warscheinlichkeit, ΔAlle_Spaltennamen_und_Spaltentypen[0], ΔAlle_Spaltennamen_und_Spaltentypen[1])

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
#Eine_liste_in_Tabelle_hinzufügen(Δconnection, ΔDatabase_Warscheinlichkeit, ΔTabelle_Warscheinlichkeit, ΔIndex_Spalte_Liste, ΔSpaltenname)

def Zwei_listen_in_Tabelle_hinzufügen(Connection,Database,Tabelle,Liste1,Liste2,Spaltenname1,Spaltenname2):
    for i in range(0,len(Liste1)):
        Date    = Liste1[i]
        Dicunt  = Liste2[i]

        Connection, Cursor  = Connection, Connection.cursor()
        Command1 = f"""
            INSERT INTO {Database}.{Tabelle} ({Spaltenname1},{Spaltenname2})
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
            {Spaltenname1} = %s, {Spaltenname2} = %s;
        """
        values = (Date, Dicunt, Date, Dicunt)
        
        Cursor.execute(Command1, values)
        Connection.commit()
#Zwei_listen_in_Tabelle_hinzufügen(Δconnection,Δdatabase_Dicunt,Δtabelle_Dicunt,Tage,Dicunt,'date','_1000')

#Etwas_spezifische_Funktionen_________________________________________________________________________________________________________________________

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
#Handelstage_Dicunt = Nächste_Handelstage_finden(Δconnection, Δdatabase_Close, Δtabelle_Close, Δdatabase_Extra, Δtabelle_Handelstage, Δspalte_Handelstage, Δperioden)

def Daten_Tabelle_mit_yfinance_aktualisieren(Connection,Database,Tabellenname,Wertpapierkürzel,Tage_str):
    #Tage_str='10' und Wertpapierkürzel = 'dbk.de'

    Wertpapierdaten = yf.Ticker(Wertpapierkürzel).history(period=Tage_str+'d')
    for i in range(0,len(Wertpapierdaten)):
        Date   = Wertpapierdaten.index[i].date().strftime('%Y-%m-%d')  #Datenentnahme yfinance
        Close  = round(Wertpapierdaten.iloc[i,3],3)

        Connection, Cursor  = Connection, Connection.cursor()                                       #in mysql Tabelle hinzufügen
        Command1 = f"""
            INSERT INTO {Database}.{Tabellenname} (Date, Close)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
            Date = %s, Close = %s;
        """
        values = (Date, Close, Date, Close)
        
        Cursor.execute(Command1, values)
        Connection.commit()
#Daten_Tabelle_mit_yfinance_aktualisieren(Δconnection,'01_usa_securities_extra','_02_USmarket',10)

#Friedhof______________________________________________________________________________________________________________
def Friedhof():
    pass

    
def add_values_to_table(connection, database, table, values, column, primary_key, pk_values, date_format='%Y-%m-%d'):
    cursor = connection.cursor()

    # Convert date values to strings with the specified format
    formatted_pk_values = [date.strftime(date_format) for date in pk_values]

    # Generate the placeholders for the values
    value_placeholders = ', '.join(['%s' for _ in range(len(values[0]))])

    # SQL statement using executemany
    command = f'''
        INSERT INTO {database}.{table} ({primary_key}, {column})
        VALUES (%s, {value_placeholders}) ON DUPLICATE KEY UPDATE {column} = VALUES({column});
    '''

    # Combine the primary key and values into a list of tuples
    data = list(zip(formatted_pk_values, *values))

    # Execute the statement with executemany
    cursor.executemany(command, data)

    # Commit the changes
    connection.commit()
date_liste = [datetime(2023,1,5),datetime(2023,1,6)]
#add_values_to_table(Δconnection, '01_usa_securities_extra', '_02_usmarket', np.array([10000.111,20000.222]),'Dow_Jones_Industrial_Average','date',np.array(['2023-01-05','2023-01-06']))


def Einen_Wert_Tabelle_hinzufügen2(Connection, Database, Tabelle, Wert, Spalte, Primärschlüssel, PK_wert):
    Connection, Cursor  = Connection, Connection.cursor()

    command = f'''INSERT INTO {Database}.{Tabelle} ({Primärschlüssel}, {Spalte}) VALUES ({PK_wert}, '{Wert}') ON DUPLICATE KEY UPDATE {Spalte} = '{Wert}';'''
    Cursor.execute(command)
    Connection.commit()
#Einen_Wert_Tabelle_hinzufügen2(Δconnection, '01_usa_securities_extra', '_02_usmarket', '10000.111','Dow_Jones_Industrial_Average', 'date', datetime(2023,1,5))
#Einen_Wert_Tabelle_hinzufügen2(Δconnection, 'mydb', 'employees', 'Gold','first_name', 'employee_id', '9')


# dji_data = yf.download('^'+'dji', start='2023-01-01', end='2024-01-01')
# #print(dji_data.iloc[0])
# #print((dji_data.index).tolist())
# dji_close = []
# for i in range(0,len(dji_data)):
#     dji_close.append(round(dji_data.at[dji_data.index[i], dji_data.columns[3]],3))

# print(dji_close)

# Eine_liste_in_Tabelle_hinzufügen(Δconnection, '01_usa_securities_extra', '_02_usmarket', dji_close, 'Dow_Jones_Industrial_Average')