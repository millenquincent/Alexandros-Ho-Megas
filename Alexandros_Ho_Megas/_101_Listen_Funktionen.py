import numpy as np
import mysql.connector, math




data_1 = np.array([1,3,4,5,7,12,20,5,4,3,3,3,3,3,1])
data_2 = np.array([2,8,8,7,6,5,12,11,12,9,5,1,1,1,1])
Δconnection           = mysql.connector.connect(host='localhost',user='root',password='--',port='3306')
#Umstrukturierung__________________________________________________________________________________
def Liste_in_Teillisten_bringen_unkonsolidiert(npli,länge):
    if länge <= 0 or länge > len(npli): return []
    Teillisten = [npli[i:i + länge] for i in range(len(npli) - länge + 1)]
    return Teillisten
#print(Liste_in_Teillisten_bringen_unkonsolidiert(data_1,4))

def Tabelle_auf_SpalteX_gekürzt(Tabelle, SpalteX):
    #Tabelle der Form = [['spalte1','spalte2','spalte3'],[1,2,3,4],[None,None,100,200],['m','w','l','s']]
    #spalte1 1,2,3 und spalte2 1,2,3,4 aber nicht spalte1 2,3,4 und spalte2 1,2,3,4    von Anfangan auffüllen

    Spaltennummer                    = [i for i in range(0,len(Tabelle[0])) if Tabelle[0][i] == SpalteX][0]+1
    Spaltenlänge_Spalte1_ohne_None   = len([i for i in Tabelle[Spaltennummer] if i != None])
    New_Tabelle                      = [Tabelle[0]]
    for i in range(1,len(Tabelle)):  New_Tabelle.append(Tabelle[i][:Spaltenlänge_Spalte1_ohne_None])

    return New_Tabelle
#print(Tabelle_auf_SpalteX_gekürzt([['spalte1','spalte2','spalte3'],[1,2,3,4],[100,200],['m','w','l','s']],'spalte2'))

def Tabelle_auf_SpalteX_kürzen_mit_PK(Tabelle, SpalteX):

    Spaltennummer = np.where(Tabelle[0] == SpalteX)[0][0] + 1
    gebrauchte_Index = np.where(~np.isnan(Tabelle[Spaltennummer].astype(float)))[0]
    Neue_Tabelle = [Tabelle[0]] + [Tabelle[i][gebrauchte_Index].tolist() for i in range(1, len(Tabelle))]
    
    return Neue_Tabelle
#print(Tabelle_auf_SpalteX_kürzen_mit_PK([np.array(['spalte1', 'spalte2', 'spalte3']), np.array([1, 2, 3, 4]), np.array([np.nan, None, 100, np.nan]), np.array(['m', 'w', 'l', 's'])], 'spalte2'))




def Friedhof():
        def Tabelle_auf_SpalteX_kürzen_mit_PK(Tabelle, SpalteX):
        #Tabelle = [np.array(['spalte1','spalte2','spalte3']),np.array([1,2,3,4]),np.array([None,None,100,200]),np.array(['m','w','l','s'])]

            Spaltennummer     = np.where(Tabelle[0] == SpalteX)[0][0] + 1
            gebrauchte_Index  = np.where(Tabelle[Spaltennummer] != None)[0]
            Neue_Tabelle      = [Tabelle[0]] + [Tabelle[i][gebrauchte_Index].tolist() for i in range(1, len(Tabelle))]

            return Neue_Tabelle
    #print(Tabelle_auf_SpalteX_kürzen_mit_PK([np.array(['spalte1','spalte2','spalte3']),np.array([1,2,3,4]),np.array([None,None,100,200]),np.array(['m','w','l','s'])], 'spalte2'))



