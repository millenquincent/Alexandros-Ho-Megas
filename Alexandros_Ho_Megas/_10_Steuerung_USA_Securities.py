import requests, os, pandas as pd, mysql.connector, numpy as np, time, yfinance as yf

from   _103_Dicunt_Funktionen         import _2002, _2003
from   _103_Dicunt_Funktionen         import Wörterbuch_str_Funktionen, Wörterbuch_func_Funktionen, func_Funktionen
import _108_MySQL_Funktionen
from _00_Datenentnahme_USA_Secureties import Tabelle_01_usa_secureties_verarbeiten_initialisieren, Tabelle_01_usa_secureties_verarbeiten_aktualisieren, Namensliste_Ordnerdateien
from _01_Dicunt_USA_Secureties        import Dicunt_Tabellen_Initialisieren, Dicunt_Tabellen_Aktualisieren





#Hauptvariablen________________________________________________________________________________________________________
Δdatabase   = '01_usa_securities'
Δtabelle    = 'neu_vz'

#Verbindung____________________________________________________________________________________________________________
Δconnection           = mysql.connector.connect(host='localhost',user='root',password='--',port='3306')

#Nebenvariablen1________________________________________________________________________________________________________
ΔFormatierte_Spalte   = 'Volume'
ΔNeues_Format         = 'int'
ΔPK_Spalte            = 'date'
ΔNeuer_Spaltenname    = 'Renditen'
ΔNeuer_Spaltentyp     = 'DECIMAL(6,3)'
ΔTage_str             = '500'
ΔSpaltenname_Renditen = 'Renditen'
ΔSpaltenname_Close    = 'Close'
Δpfad       = 'C:/01_usa_secureties'

#Funktionsvariablen___________________________________________________________________________________________________________
ΔListe_Tabellen       = Namensliste_Ordnerdateien(Δpfad)


#Hauptfunktion_Ein_Database_Datenentnahme________________________________________________________________________________________

def Database_01_usa_secureties_verarbeiten_initialisieren(Connection,Database,Liste_Tabellen,Formatierte_Spalte,Neues_Format,PK_Spalte,Neuer_Spaltenname,Neuer_Spaltentyp,Spaltenname_Renditen,Spaltenname_Close):
    #from _00_Datenentnahme_USA_Secureties import Tabelle_01_usa_secureties_verarbeiten_initialisieren
    for i in range(1,len(Liste_Tabellen)):
        Tabelle_01_usa_secureties_verarbeiten_initialisieren(Connection,Database,Liste_Tabellen[i],Formatierte_Spalte,Neues_Format,PK_Spalte,Neuer_Spaltenname,Neuer_Spaltentyp,Spaltenname_Renditen,Spaltenname_Close)
        print('Initialisiert:', Liste_Tabellen[i],i)
    return 'end Database_01_usa_secureties_verarbeiten_initialisieren'
#print(Database_01_usa_secureties_verarbeiten_initialisieren(Δconnection,Δdatabase,ΔListe_Tabellen,ΔFormatierte_Spalte,ΔNeues_Format,ΔPK_Spalte,ΔNeuer_Spaltenname,ΔNeuer_Spaltentyp,ΔSpaltenname_Renditen,ΔSpaltenname_Close))

def Database_01_usa_secureties_verarbeiten_aktualisieren(Connection,database,Liste_Tabellen,Tage_str,Spaltenname_Renditen,Spaltenname_Close,PK_Spalte):
    #from _00_Datenentnahme_USA_Secureties import Tabelle_01_usa_secureties_verarbeiten_aktualisieren
    for i in range(1,len(Liste_Tabellen)):
        Tabelle_01_usa_secureties_verarbeiten_aktualisieren(Connection,database,Liste_Tabellen[i],Tage_str,Spaltenname_Renditen,Spaltenname_Close,PK_Spalte)
        print('Aktualisiert:', Liste_Tabellen[i],i)
    return 'end Database_01_usa_secureties_verarbeiten_aktualisieren'
#print(Database_01_usa_secureties_verarbeiten_aktualisieren(Δconnection,Δdatabase,ΔListe_Tabellen,ΔTage_str,ΔSpaltenname_Renditen,ΔSpaltenname_Close,ΔPK_Spalte))

#Nebenvariablen2_______________________________________________________________________________________________________
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
#Hauptfunktion_Ein_Database_Dicunt_______________________________________________________________________________________________

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

