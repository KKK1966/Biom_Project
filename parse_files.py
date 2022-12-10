import csv
from biom import load_table
import threading
import time
import os
import re

# BIOM_FILE_PATH = '/Users/konstantinkravchenko/Desktop/Project_biom/biom/'
TARGET = "s__bacterium_Ellin7504"
Q_THREADS = 4


# start_time = time.time()

Sum_by_taxon = 0
Quantity_by_taxon = 0

def parse_biom(arr):
    for i in arr:
        table = load_table( i )
        f_filter = lambda values, id_, md: TARGET in md['taxonomy']
        env = table.filter(f_filter, axis='observation', inplace=False)
        with lock:
            global Sum_by_taxon , Quantity_by_taxon
            Sum_by_taxon += env.sum(axis='whole')
            Quantity_by_taxon  += len(env.ids(axis='observation'))


    return
 

if __name__ == "__main__":

    #получаем все файлы в директории

    dirname = os.path.dirname(__file__)

    tree = os.walk(dirname + "/biom/")


    for i in tree:
        biom_arr = [*i]

    Number_of_Files = len(biom_arr[2])

    for i in range(Number_of_Files):
        biom_arr[2][i] = dirname + "/biom/" + biom_arr[2][i]


    L_Q_Threads = Number_of_Files//Q_THREADS

        #разделим исходный массив на 4 подмассива, чтобы запустить 4 потока для каждого. 4 потому что TOTAL_BIOM делится на 4
    biom_arr_by_threads = [[]*Q_THREADS for i in range(Q_THREADS)]

    for i in range(Q_THREADS-1):
        biom_arr_by_threads[i] = biom_arr[2][L_Q_Threads*i:L_Q_Threads*(i+1)]

    biom_arr_by_threads[Q_THREADS - 1] = biom_arr[2][L_Q_Threads*(Q_THREADS-1) : Number_of_Files]
        
    lock = threading.Lock()

    threads = [threading.Thread(target = parse_biom, args = (biom_arr_by_threads[i],)) for i in range(Q_THREADS)]

    for thread in threads:
        thread.start()
    
    for thread in threads:
        thread.join()

    print("\n\nDone!\n")



    print('Taxon', TARGET ,'Sum_by_taxon = ', Sum_by_taxon, 'Quantity_by_taxon = ', Quantity_by_taxon)

    print("\n\n")