import pandas as pd





data_1 = [1,3,4,5,7,12,20,5,4,3,3,3,3,3,1]
data_2 = [2,8,8,7,6,5,12,11,12,9,5,1,1,1,1]

#__________________________________________________________________________________________________
def Delete_List_Wert(list,list_to_delete_index):
    for i in range(0,len(list_to_delete_index)):
        del list[list_to_delete_index[i]]
        for t in range(0,len(list_to_delete_index)):
            if list_to_delete_index[i] < list_to_delete_index[t]:
                Eins_kleiner = list_to_delete_index[t]-1
                list_to_delete_index[t] = Eins_kleiner
    return list
#print(Delete_List_Wert(data_1,[2,3,4]))