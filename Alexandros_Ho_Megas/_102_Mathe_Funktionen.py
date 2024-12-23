import numpy as np





#Funktionen____________________________________________________________________________________________________________


#Spezifische_Funktionen________________________________________________________________________________________________
def Permutationen_finden_3Zahlen_2Listlänge(Zahlenliste):
    liste =  []
    for i in Zahlenliste:
        for t in Zahlenliste:
            x = np.array([i,t])
            liste.append(x)
    return liste
#print(Permutationen_finden_3Zahlen_2Listlänge([0,1,2]))


