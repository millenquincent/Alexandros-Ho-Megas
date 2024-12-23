import numpy as np
import datetime
import mysql.connector
Close  = np.array([3,4,4,6,7,8,7,7,7])
Dicunt = np.array([3.05,14,7,6,6,7,8,9,9])
Obere_Schranke = 0.5
Untere_Schranke = 0.5

from _101_Listen_Funktionen import Tabelle_auf_SpalteX_gekürzt, Tabelle_auf_SpalteX_kürzen_mit_PK
from _108_MySQL_Funktionen  import Mehrere_Spalten_Tabelle_hinzufügen, Spaltennamen_anzeigen, Tabelle_ausgeben, Tabellennamen_Database_aufrufen
from _108_MySQL_Funktionen  import Spaltenname_und_Spaltentyp_vom_index, Spalte_ausgeben, Eine_liste_in_Tabelle_hinzufügen, Alle_Spaltennamen_und_Spaltentypen_ausgeben
from _108_MySQL_Funktionen  import Tabelle_Spaltennamen_ausgeben, Name_und_Spalten_ausgeben

def Warscheinlichkeit_dass_Dicunt_Eintrifft_4Punkteverfahren(Liste_Close, Liste_Dicunt, Obere_Schranke, Untere_Schranke):
    # Obere und Untere Schranke müssen positiv sein

    Liste_Obere_Dicunt = Liste_Dicunt * (1+Obere_Schranke)
    Liste_Untere_Dicunt = Liste_Dicunt / (1+Untere_Schranke)
    Liste_Muster = np.zeros([len(Liste_Close)])

    Liste_Muster[(Liste_Obere_Dicunt >= Liste_Close) & (Liste_Untere_Dicunt <= Liste_Close)] = 2

    Liste_Muster[(Liste_Dicunt * (1+Obere_Schranke*0.5) >= Liste_Close) & (Liste_Dicunt / (1+Untere_Schranke*0.5) <= Liste_Close)] = 3

    Liste_Muster[(Liste_Obere_Dicunt  < Liste_Close)] = 1
    Liste_Muster[(Liste_Untere_Dicunt > Liste_Close)] = 1

    Liste_Muster[(Liste_Dicunt  * (1+Obere_Schranke*2)  < Liste_Close)] = 0
    Liste_Muster[(Liste_Dicunt  / (1+Untere_Schranke*2) > Liste_Close)] = 0

    Eintreten_Prozent = np.sum(Liste_Muster)/(len(Liste_Muster)*3)
    return Eintreten_Prozent
#print(Warscheinlichkeit_dass_Dicunt_Eintrifft_4Punkteverfahren(Close, Dicunt, Obere_Schranke, Untere_Schranke))

def Warscheinlichkeit_dass_Dicunt_Eintrifft_1Punkteverfahren(Liste_Close, Liste_Dicunt, Obere_Schranke, Untere_Schranke):
    # Obere und Untere Schranke müssen positiv sein

    Liste_Obere_Dicunt = Liste_Dicunt * (1+Obere_Schranke)
    Liste_Untere_Dicunt = Liste_Dicunt / (1+Untere_Schranke)
    wahr_1_unwahr_0 = np.where(np.logical_and(Liste_Close <= Liste_Obere_Dicunt, Liste_Close >= Liste_Untere_Dicunt), 1, 0)
    Eintreten_Prozent = np.mean(wahr_1_unwahr_0)
    return Eintreten_Prozent
#print(Warscheinlichkeit_dass_Dicunt_Eintrifft_1Punkteverfahren(Close, Dicunt, Obere_Schranke, Untere_Schranke))

def Konfidenzintervall_5prozent_der_Warscheinlichkeit_dass_Dicunt_Eintrifft(Liste_Close, Liste_Dicunt, Obere_Schranke, Untere_Schranke):
    import numpy as ap, scipy.stats as stats
    # Obere und Untere Schranke müssen positiv sein

    Liste_Unterschied_Close_Dicunt = np.abs(Liste_Close-Liste_Dicunt)
    Eintreten_Prozent = np.mean(Liste_Unterschied_Close_Dicunt)
    Std_Unterschied_Close_Dicunt = np.std(Liste_Unterschied_Close_Dicunt)
    lower, upper = stats.norm.interval(0.05, loc=Eintreten_Prozent, scale=Std_Unterschied_Close_Dicunt/np.sqrt(len(Liste_Unterschied_Close_Dicunt)))
    return lower, upper, Eintreten_Prozent
#print(Konfidenzintervall_5prozent_der_Warscheinlichkeit_dass_Dicunt_Eintrifft(Close, Dicunt, Obere_Schranke, Untere_Schranke))

#____________________________________________________________________________________________________________________________________________________
ΔDatabase_Close             = '01_usa_securities'
ΔDatabase_Warscheinlichkeit = '01_usa_securities_warscheinlichkeit'
ΔDatabase_Dicunt            = '01_usa_securities_dicunt'
ΔTabelle_Close              = 'us58933y1055_mrk'
#ΔTabelle_Close              = 'neu_vz'
ΔTabelle_Dicunt             = ΔTabelle_Close+'_dicunt'
ΔTabelle_Warscheinlichkeit  = ΔTabelle_Close+'_warscheinlichkeit'
ΔSpalte_Close               = 'close'
ΔSpalte_Dicunt              = 'date'

Δconnection           = mysql.connector.connect(host='localhost',user='root',password='--',port='3306')

#Tabellen in Warscheinlichkeitsdatabase erstellt_______________________________________________________________________
def Für_jede_tabelle_in_DB1_Tabelle_in_DB2_erstellen(Connection, Database1, Database2):
    #from _108_MySQL_Funktionen import Tabellennamen_Database_aufrufen, Tabelle_erstellen_mit_Datentyp_und_Primärschlüssel
    Connection, Cursor  = Connection, Connection.cursor()

    Liste_Tabellen_aus_DB1                = Tabellennamen_Database_aufrufen(Connection, Database1)
    Liste_Tabellen_aus_DB1_mit_dicunt     = [i[:-7] + '_warscheinlichkeit' for i in Liste_Tabellen_aus_DB1]
    Liste_Tabellen_aus_DB2                = Tabellennamen_Database_aufrufen(Connection, Database2)
    Noch_nicht_erstellte_Tabellen_inDB2   = list(set(Liste_Tabellen_aus_DB1_mit_dicunt) ^ set(Liste_Tabellen_aus_DB2))

    for i in range(0,len(Noch_nicht_erstellte_Tabellen_inDB2)):
        Tabelle_erstellen_mit_Datentyp_und_Primärschlüssel(Connection, Database2, Noch_nicht_erstellte_Tabellen_inDB2[i],'date','DATE')
#Für_jede_tabelle_in_DB1_Tabelle_in_DB2_erstellen(Δconnection, ΔDatabase_Dicunt, ΔDatabase_Warscheinlichkeit)

#Indexhinzufügen_______________________________________________________________________________________________________
ΔSpaltenname_Spaltentyp              = Spaltenname_und_Spaltentyp_vom_index(Δconnection, ΔDatabase_Dicunt, ΔTabelle_Dicunt)
ΔIndex_Spalte_Liste                  = Spalte_ausgeben(Δconnection, ΔDatabase_Dicunt, ΔTabelle_Dicunt, ΔSpaltenname_Spaltentyp[0])
Eine_liste_in_Tabelle_hinzufügen     (Δconnection, ΔDatabase_Warscheinlichkeit, ΔTabelle_Warscheinlichkeit, ΔIndex_Spalte_Liste, ΔSpaltenname_Spaltentyp[0])

#Spalte hinzufügen_____________________________________________________________________________________________________
ΔAlle_Spaltennamen_und_Spaltentypen  = Alle_Spaltennamen_und_Spaltentypen_ausgeben(Δconnection, ΔDatabase_Dicunt, ΔTabelle_Dicunt)
Mehrere_Spalten_Tabelle_hinzufügen   (Δconnection, ΔDatabase_Warscheinlichkeit, ΔTabelle_Warscheinlichkeit, ΔAlle_Spaltennamen_und_Spaltentypen[0], ΔAlle_Spaltennamen_und_Spaltentypen[1])

#Warscheinlichkeiten berechnen_________________________________________________________________________________________
def Für_alle_Dicuntcodes_Warscheinlichkeiten_berechnen(Connection, Database_Dicunt, Tabelle_Dicunt):
    #import numpy as np
    #from _108_MySQL_Funktionen import Name_und_Spalten_ausgeben
    #Gibt Warscheinlichkeit, den Indexwert wozu der Zaählt und zu welchen Dicunt Code
    Connection, Cursor  = Connection, Connection.cursor()

    Tabelle                   = Name_und_Spalten_ausgeben(Δconnection, ΔDatabase_Dicunt, ΔTabelle_Dicunt)
    Tabelle_bereinigt         = Tabelle_auf_SpalteX_kürzen_mit_PK(Tabelle,'close')
    List_Close                = Tabelle_bereinigt[2]
    Obere_Schranke            = 0.1
    Untere_Schranke           = 0.1
    List_Warscheinlichkeiten  = []
    Zugehöriger_Indexwert     = Tabelle[1][-1]

    try:
        for i in range(2,len(Tabelle[0])):
            Dicuntcodename = Tabelle[0][i]
            List_Dicunt = np.array(Tabelle[3][i]).astype(float)
            #List_Close  = np.array([100.000,100.000,100.000])
            Warscheinlichkeit = Warscheinlichkeit_dass_Dicunt_Eintrifft_4Punkteverfahren(List_Close, List_Dicunt, Obere_Schranke, Untere_Schranke)
            List_Warscheinlichkeiten.append([Warscheinlichkeit,Zugehöriger_Indexwert, Dicuntcodename])

        return List_Warscheinlichkeiten
    except: 
        Exception
        return []
Warscheinlichkeit = Für_alle_Dicuntcodes_Warscheinlichkeiten_berechnen(Δconnection, ΔDatabase_Dicunt, ΔTabelle_Dicunt)

#Warscheinlichkeit in Tabelle hinzufügen_______________________________________________________________________________
ΔWert          = Warscheinlichkeit[0][0]
ΔIndex_Wert    = Warscheinlichkeit[0][1]
ΔSpalten_Wert  = Warscheinlichkeit[0][2]

def Einen_Wert_Tabelle_hinzufügen(Connection, Database, Tabelle, Wert, Index, Index_Wert, Spalten_Wert):
    Connection, Cursor  = Connection, Connection.cursor()
    Command             = f"UPDATE {Database}.{Tabelle} SET {Spalten_Wert} = '{Wert}' WHERE {Index} = {Index_Wert}"
    Cursor.execute(Command); Connection.commit(); Cursor.close(); Connection.close()
#Warscheinlichkeit_hinzugefügt = Einen_Wert_Tabelle_hinzufügen(Δconnection, 'mydb'                     , 'employees'               ,'Affe', 'employee_id','2'         , 'first_name' )
Warscheinlichkeit_hinzugefügt = Einen_Wert_Tabelle_hinzufügen(Δconnection, ΔDatabase_Warscheinlichkeit, ΔTabelle_Warscheinlichkeit, ΔWert, 'date'       , ΔIndex_Wert, ΔSpalten_Wert)


def Einen_Wert_Tabelle_hinzufügen2(Connection, Database, Tabelle, Wert, Spalte, Primärschlüssel, PK_wert):
    Connection, Cursor  = Connection, Connection.cursor()

    command = f'''INSERT INTO {Database}.{Tabelle} ({Primärschlüssel}, {Spalte}) VALUES ({PK_wert}, '{Wert}') ON DUPLICATE KEY UPDATE {Spalte} = '{Wert}';'''
    Cursor.execute(command)
    Connection.commit()
#Einen_Wert_Tabelle_hinzufügen(Δconnection, 'mydb', 'employees', 'Goldfisch','first_name', 'employee_id', '9')
#Warscheinlichkeit_hinzugefügt = Einen_Wert_Tabelle_hinzufügen2(Δconnection, ΔDatabase_Warscheinlichkeit, ΔTabelle_Warscheinlichkeit, ΔWert,ΔSpalten_Wert, 'date', ΔIndex_Wert)

#friedhof______________________________________________________________________________________________________________

