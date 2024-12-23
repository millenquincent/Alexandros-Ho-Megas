
#____________________________________________________________________________________________________________________________________________________________________________________________
from _01_Dicunt_USA_Secureties import *
from _103_Dicunt_Funktionen import _1002, _2002
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

ΔListe_Dicunt_Funktionen = [_1002]
ΔDicunt_Funktion         = ΔListe_Dicunt_Funktionen[0]

#Datenverbindung_______________________________________________________________________________________________________
Δconnection      = mysql.connector.connect(host='localhost',user='root',password='--',port='3306')
#______________________________________________________________________________________________________________________

import yfinance as yf
from datetime import datetime
import math
from collections import Counter
import _108_MySQL_Funktionen

def Dividenden_Auszahlungsdatum_bekommen(ticker):

    Dividenden_Tage_Auszahlung = yf.Ticker(ticker).dividends
    Dividenden_Tage            = list(Dividenden_Tage_Auszahlung.index.date)
    Anzahl_80prozent           = math.floor(len(Dividenden_Tage)*0.8)

    Tagesliste            = [datetime(2024, date.month, date.day) for date in Dividenden_Tage]
    Tages_Auftreten_liste = sorted(Counter(Tagesliste).items(), key=lambda item: item[1], reverse=True)

    Anzahl_liste, Tag_liste = [], []
    for i in range(0,len(Tages_Auftreten_liste)):
        Tag_liste   .append(Tages_Auftreten_liste[i][0])
        Anzahl_liste.append(Tages_Auftreten_liste[i][1])

    Zähler = 0
    for i in range(0,len(Anzahl_liste)):
        Zähler = Zähler + Anzahl_liste[i]
        if Zähler >= Anzahl_80prozent:
            New_list = Tag_liste[:i]
    Dividendentag = min(New_list)

    return Dividendentag
#print(Dividenden_Auszahlungsdatum_bekommen("BAYN.DE"))



import pandas as pd

def Tabelle_ausgeben_pddf(Connection, Database, Tabelle):
    Cursor  = Connection.cursor()
    query   = f"SELECT * FROM {Database}.{Tabelle}"
    Cursor  .execute(query)
    columns = [desc[0] for desc in Cursor.description]
    data    = Cursor.fetchall()
    Tabelle = pd.DataFrame(data, columns=columns)

    return Tabelle
#print(Tabelle_ausgeben_pddf(Δconnection,Δdatabase_Close,Δtabelle_Close))

#Trading Account

#Account ID Großtabelle mit allen optionen
#Anzahl kauf, kaufpreis, kaufgebühr, differenz, netto differenz,Anzahl Verkauf, verkaufpreis, verkaufgebühr,
#aktuelle differenz, akt netto differenz, akt anzahl verkauf=kaufanzahl, akt verkaufpreis, akt verkaufgebühr


Kaufanzahl = 5
Kaufpreis  = 20
Kaufgebühr = 0.01

Akt_Verkaufanzahl = Kaufanzahl
Akt_Verkaufpreis  = 23
Akt_Verkaufgebühr = 0.01

Verkaufanzahl = 3
Verkaufpreis  = 25
Verkaufgebühr = 0.01 

Kontostand = 500
Konto_ein_ausgaben = 0


#_____________________________________________________________________

def _2003(npli):
    from _100_Grund_Funktionen  import Delete_List_Wert
    from _101_Listen_Funktionen import Liste_in_Teillisten_bringen_unkonsolidiert
    #from _103_Dicunt_Funktionen import Renditenliste
    Obere_Schranke           =  0.01
    Untere_Schranke          = -0.01
    Berücksichtigte_Perioden =  252
    Dicunt_Code              = '_2003'

    if len(npli) >= Berücksichtigte_Perioden: 

        def Absolute_Häufigkeit_Auftreten_von_x_y(Liste,Zweiermuster_0_1_2):
            Len_Liste = len(Liste)
            Anzahl_X_X = 0
            Del_liste = []
            for i in range(0,Len_Liste):
                if np.array_equal(Liste[i], Zweiermuster_0_1_2):
                    Anzahl_X_X += 1
                    Del_liste.append(i)
            Gekürzte_Liste = Delete_List_Wert(Liste,Del_liste)
            return Anzahl_X_X, Gekürzte_Liste

        npli_gekürzt                = npli[-Berücksichtigte_Perioden:]
        Rendite_liste               = Renditenliste(npli_gekürzt)
        Renditenliste_gemustert     = np.where(Rendite_liste > Obere_Schranke, 2, np.where(Rendite_liste > Untere_Schranke, 1, 0))
        Renditenteilliste_gemustert = Liste_in_Teillisten_bringen_unkonsolidiert(Renditenliste_gemustert,2)
        Anzahl_0_0,Anzahl_0_1,Anzahl_0_2,Anzahl_1_0,Anzahl_1_1,Anzahl_1_2,Anzahl_2_0,Anzahl_2_1,Anzahl_2_2 = 0,0,0,0,0,0,0,0,0

        Anzahl_0_0 = Absolute_Häufigkeit_Auftreten_von_x_y(Renditenteilliste_gemustert,[0,0])
        Anzahl_0_1 = Absolute_Häufigkeit_Auftreten_von_x_y(Anzahl_0_0[1],[0,1])
        Anzahl_0_2 = Absolute_Häufigkeit_Auftreten_von_x_y(Anzahl_0_1[1],[0,2])
        Anzahl_1_0 = Absolute_Häufigkeit_Auftreten_von_x_y(Anzahl_0_2[1],[1,0])
        Anzahl_1_1 = Absolute_Häufigkeit_Auftreten_von_x_y(Anzahl_1_0[1],[1,1])
        Anzahl_1_2 = Absolute_Häufigkeit_Auftreten_von_x_y(Anzahl_1_1[1],[1,2])
        Anzahl_2_0 = Absolute_Häufigkeit_Auftreten_von_x_y(Anzahl_1_2[1],[2,0])
        Anzahl_2_1 = Absolute_Häufigkeit_Auftreten_von_x_y(Anzahl_2_0[1],[2,1])
        Anzahl_2_2 = Absolute_Häufigkeit_Auftreten_von_x_y(Anzahl_2_1[1],[2,2])

        Dictionary                = {'0,0':Anzahl_0_0[0],'0,1':Anzahl_0_1[0],'0,2':Anzahl_0_2[0],'1,0':Anzahl_1_0[0],'1_1':Anzahl_1_1[0],'1_2':Anzahl_1_2[0],'2_0':Anzahl_2_0[0],'2_1':Anzahl_2_1[0],'2_2':Anzahl_2_2[0]}
        Warscheinlichste_Bewegung = max(Dictionary, key = Dictionary.get)
        Letztes_Muster            = str(Renditenliste_gemustert[-1:][0])
        if Warscheinlichste_Bewegung[0] == Letztes_Muster:
            Nächste_Bewegung = int(Warscheinlichste_Bewegung[2])
            if Nächste_Bewegung == 0:
                Nächste_Bewegung = 0
            if Nächste_Bewegung == 1:
                Nächste_Bewegung = Obere_Schranke
            if Nächste_Bewegung == 2:
                Nächste_Bewegung = Untere_Schranke
            Letzter_Wert = npli[-1]
            Dicunt_nächster_Close = np.array([Letzter_Wert*(1+Nächste_Bewegung)]) # hier wird lediglich die Untere oder Obere Schranke als Bewegung angenommen, potentiell höheres wird nicht berügsichtigt
        else: return np.array([]), Dicunt_Code


        return Dicunt_nächster_Close, Dicunt_Code
    else: return np.array([]), Dicunt_Code
#print(_2003(data_2))


def _2004(npli):

    def Lin_Regression_Funktion(npli):
        x = np.arange(len(npli))
        Koeffizienten = np.polyfit(x, npli, 1)
        Steigung, Achsenabschnitt = Koeffizienten
        return Achsenabschnitt, Steigung

    Dicunt_Code = '_2004'
    Berücksichtigte_Perioden = 25
    if len(npli) >= Berücksichtigte_Perioden:

        Ida     = npli[-Berücksichtigte_Perioden:]
        Average = np.average(npli)
        Größer  = Ida[Ida >= Average]
        Kleiner = Ida[Ida <  Average]

        A_Größer, S_Größer  = Lin_Regression_Funktion(Größer)
        A_Kleiner,S_Kleiner = Lin_Regression_Funktion(Kleiner)
        Hannah              = npli[-1]

        if npli[-1] >= Average: Linnea = Hannah + S_Größer 
        if npli[-1] < Average:  Linnea = Hannah + S_Kleiner
        
        return np.array([Linnea]), Dicunt_Code
    else: return np.array([]), Dicunt_Code
#print(_2004(data_2))



#Aufgaben
# 1 Datum der SQL Funktion umschreiben
# 2 _2004 einfügen, anpassen
# 3 Regel der Zinssätze, Phasenteiler eind Dividendenzahlungstag hinzufügen
# 4 Trading regel beginnen


