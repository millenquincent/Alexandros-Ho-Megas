





#def _1001(x):
#    return x+10
#def _1002(x):
#    return x+10

#______________________________________________________________________________________________________________________

def Str_to_Function(Str):
    #Es gibt eine funktion die _1000 heißt und man hat das Str: '_1000' und formt das str so um dass man die Funktion heraus bekommt
    return globals()[Str]
#print(Str_to_Function('_1001')(10))

def List_Str_to_List_Funktion(List_Str):
    List_Funktion = []
    for i in range(0,len(List_Str)):
        if List_Str[i] is not None:
            Function = globals()[List_Str[i]]
            List_Funktion.append(Function)
        else: pass

    return List_Funktion
#print(List_Str_to_List_Funktion(['_1001', '_1002']))