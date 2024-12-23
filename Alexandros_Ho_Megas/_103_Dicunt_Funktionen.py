import numpy as np
from scipy.stats import linregress
import matplotlib.pyplot as plt
import time
import random
import multiprocessing as mp
from multiprocessing import Pool
from numba import njit
import pandas as pd
import mysql

random_numbers = [random.random() for _ in range(10)]
data_0 = np.array([2,8,8,7,6,5,12,11,12,9,5,1,1,1,1])
data_1 = np.array([1,2,3,4,5])
data_2 = pd.read_csv('C:/01_usa_secureties/US7427181091_PG.csv').Close.to_numpy()


#Hilfsfunktionen_______________________________________________________________________________________________________________________________________________
def Renditenliste(npli):
    Renditenliste = (npli[1:] - npli[:-1]) / npli[:-1]
    Renditenliste = np.insert(Renditenliste, 0, Renditenliste[0])
    Renditenliste = np.delete(Renditenliste, 0)
    return Renditenliste
#print(Renditenliste(data_2))

def Lin_Regression_Funktion(npli):
    x = np.arange(len(npli))
    Koeffizienten = np.polyfit(x, npli, 1)
    Steigung, Achsenabschnitt = Koeffizienten
    return Achsenabschnitt, Steigung
#print(Lin_Regression_Funktion(data_0))

def Lin_Regression_Gerade(npli):
    x = np.arange(len(npli))
    Koeffizienten = np.polyfit(x, npli, 1)
    Steigung, Achsenabschnitt = Koeffizienten
    Regressionsgerade = x*Steigung+Achsenabschnitt
    return Regressionsgerade
#print(Lin_Regression_Gerade(data_0))

#Stufe_0_Dicunt_Funktionen_____________________________________________________________________________________________________________________________________
def _1000(npli, Perioden):
    #Lin_Regression_Gerade_vom_letzten_Wert_ der npli liste ausnstartend
    #from _103_Dicunt_Funktionen import Lin_Regression_Funktion
    Dicunt_Code     = '_1000'
    #steigung der Regression auf Dicuntperioden fortgeführt

    Steigung        = Lin_Regression_Funktion(npli)[1]
    letzter_Wert    = npli[-1]
    Dicunt_liste    = np.arange(1,Perioden+1)
    Dicunt_liste    = Dicunt_liste*Steigung+letzter_Wert
    return Dicunt_liste, Dicunt_Code
#print(_1000(data_0,2))

def _2000(npli, Perioden):
    #gibt den Wert des Forecasts nur für Periode x
    #from _103_Dicunt_Funktionen import Lin_Regression_Funktion
    Dicunt_Code     = '_2000'

    Steigung        = Lin_Regression_Funktion(npli)[1]
    letzter_Wert    = npli[-1]
    Wert_für_letzte_Periode = np.array([Perioden*Steigung+letzter_Wert])
    return Wert_für_letzte_Periode, Dicunt_Code
#print(_2000(data_0,2))

def _1001(npli, Perioden):
    #Lin_Regression_Gerade_Dicunt_1001
    #from _103_Dicunt_Funktionen import Lin_Regression_Funktion
    Dicunt_Code     = '_1001'
    #Regressionsgerade fortgeführt

    Steigung        = Lin_Regression_Funktion(npli)[0]
    letzter_Wert    = npli[-1]
    Dicunt_liste    = np.arange(letzter_Wert+1,letzter_Wert+Perioden+1)
    Dicunt_liste    = Dicunt_liste*Steigung
    return Dicunt_liste, Dicunt_Code
#print(_1001(data_0,2))

def Dreier_Folgemuster(npli,Obere_Schranke,Untere_Schranke):
    #from _103_Dicunt_Funktionen import Renditenliste

    def Absolute_Häufigkeit_Auftreten_von_x_y(Liste,Zweiermuster_0_1_2):
        Len_Liste = len(Liste)
        Anzahl_X_X = 0
        for i in range(0,Len_Liste-1):
            if np.array_equal([Liste[i],Liste[i+1]], Zweiermuster_0_1_2):
                Anzahl_X_X += 1
        return Anzahl_X_X, Liste

    Rendite_liste               = Renditenliste(npli)
    Renditenliste_gemustert     = np.where(Rendite_liste > Obere_Schranke, 2, np.where(Rendite_liste < Untere_Schranke, 1, 0))
    Anzahl_0_0,Anzahl_0_1,Anzahl_0_2,Anzahl_1_0,Anzahl_1_1,Anzahl_1_2,Anzahl_2_0,Anzahl_2_1,Anzahl_2_2 = 0,0,0,0,0,0,0,0,0

    Anzahl_0_0 = Absolute_Häufigkeit_Auftreten_von_x_y(Renditenliste_gemustert,[0,0])
    Anzahl_0_1 = Absolute_Häufigkeit_Auftreten_von_x_y(Renditenliste_gemustert,[0,1])
    Anzahl_0_2 = Absolute_Häufigkeit_Auftreten_von_x_y(Renditenliste_gemustert,[0,2])
    Anzahl_1_0 = Absolute_Häufigkeit_Auftreten_von_x_y(Renditenliste_gemustert,[1,0])
    Anzahl_1_1 = Absolute_Häufigkeit_Auftreten_von_x_y(Renditenliste_gemustert,[1,1])
    Anzahl_1_2 = Absolute_Häufigkeit_Auftreten_von_x_y(Renditenliste_gemustert,[1,2])
    Anzahl_2_0 = Absolute_Häufigkeit_Auftreten_von_x_y(Renditenliste_gemustert,[2,0])
    Anzahl_2_1 = Absolute_Häufigkeit_Auftreten_von_x_y(Renditenliste_gemustert,[2,1])
    Anzahl_2_2 = Absolute_Häufigkeit_Auftreten_von_x_y(Renditenliste_gemustert,[2,2])

    Dictionary = {'0,0':Anzahl_0_0[0],'0,1':Anzahl_0_1[0],'0,2':Anzahl_0_2[0],'1,0':Anzahl_1_0[0],'1_1':Anzahl_1_1[0],'1_2':Anzahl_1_2[0],'2_0':Anzahl_2_0[0],'2_1':Anzahl_2_1[0],'2_2':Anzahl_2_2[0]}
    return Dictionary
#print(Dreier_Folgemuster(data_2,0.004,-0.004))

def Dreier_Folgemuster2(npli,Obere_Schranke,Untere_Schranke):
    from _100_Grund_Funktionen  import Delete_List_Wert
    from _101_Listen_Funktionen import Liste_in_Teillisten_bringen_unkonsolidiert
    #from _103_Dicunt_Funktionen import Renditenliste

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

    Rendite_liste               = Renditenliste(npli)
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

    Dictionary = {'0,0':Anzahl_0_0[0],'0,1':Anzahl_0_1[0],'0,2':Anzahl_0_2[0],'1,0':Anzahl_1_0[0],'1_1':Anzahl_1_1[0],'1_2':Anzahl_1_2[0],'2_0':Anzahl_2_0[0],'2_1':Anzahl_2_1[0],'2_2':Anzahl_2_2[0]}
    return Dictionary
#print(Dreier_Folgemuster2(data_2,0.01,-0.01)) # unterschied ist dass es in [1,2,1,0]drei statt zwei abschnitte berücksichtigt




#Stufe_1_Dicunt_Funktionen_____________________________________________________________________________________________________________________________________
def _1002(npli, Perioden):
    #Lin_Regression_Gerade_vom_letzten_Wert_Dicunt_mit_Bedingungen
    #from _103_Dicunt_Funktionen import Lin_Regression_Gerade_vom_letzten_Wert_Dicunt
    Dicunt_Code = '_1002'

    Berücksichtigte_Perioden = 25
    if len(npli) >= Berücksichtigte_Perioden: return _1000(npli[-Berücksichtigte_Perioden:], Perioden)[0], Dicunt_Code
    else:                                     return np.array([]), Dicunt_Code
#print(_1002(data_2,2))

def _2002(npli, Perioden):
    #Lin_Regression_Gerade_vom_letzten_Wert_dann nur den wert der periode x mit Bedingung
    #from _103_Dicunt_Funktionen import Lin_Regression_Gerade_vom_letzten_Wert_Dicunt
    Dicunt_Code = '_2002'
    Für_Wann = 10

    Berücksichtigte_Perioden = 25
    if len(npli) >= Berücksichtigte_Perioden: return _2000(npli[-Berücksichtigte_Perioden:], Perioden)[0], Dicunt_Code, Für_Wann
    else:                                     return np.array([]), Dicunt_Code, Für_Wann
#print(_2002(data_2,2))

def _2003(npli,Perioden):
    from _100_Grund_Funktionen  import Delete_List_Wert
    from _101_Listen_Funktionen import Liste_in_Teillisten_bringen_unkonsolidiert
    #from _103_Dicunt_Funktionen import Renditenliste
    Obere_Schranke           =  0.01
    Untere_Schranke          = -0.01
    Berücksichtigte_Perioden =  252
    Dicunt_Code              = '_2003'
    Für_Wann                 = 1

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
        else: return np.array([]), Dicunt_Code, Für_Wann


        return Dicunt_nächster_Close, Dicunt_Code, Für_Wann
    else: return np.array([]), Dicunt_Code, Für_Wann
#print(_2003(data_2,'Platzhalter'))



#Funktionen_Wörterbuch_________________________________________________________________________________________________________________________________________
def Dicunt_Codes_in_str_abrufen():
    from _108_MySQL_Funktionen import Tabelle_ausgeben, Tabellen_Info
    Δconnection           = mysql.connector.connect(host='localhost',user='root',password='--',port='3306')
    #Gesuchte_Dicunt_Codes = 'Dicunt_Codes_01'

    Dicunt_Codes          = Tabelle_ausgeben(Δconnection, '01_usa_securities_extra', '_01_dicuntfunktionen')
    info                  = Tabellen_Info(Δconnection, '01_usa_securities_extra', '_01_dicuntfunktionen')
    info                  = [info[i][0] for i in range(0,len(info))]
    o                     = []
    for i in Dicunt_Codes: o.append([x for x in i if x is not None])
    Wörterbuch     = dict(zip(info, o))
    del Wörterbuch['index']
    #Gesuchte_Codes = Wörterbuch[Gesuchte_Dicunt_Codes]
    return Wörterbuch
#print(Dicunt_Codes_in_str_abrufen())
Wörterbuch_str_Funktionen   = Dicunt_Codes_in_str_abrufen()
Wörterbuch_func_Funktionen  = {'Dicunt_Codes_00': [_1002], 'Dicunt_Codes_01': [_1002, _1001]}
func_Funktionen             = Wörterbuch_func_Funktionen.values()


