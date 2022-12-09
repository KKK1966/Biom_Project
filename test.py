
from biom import load_table
import threading
import os


BIOM_FILE_PATH = '/Users/konstantinkravchenko/Desktop/Project_biom/biom/'
TARGET = "s__"
Q_THREADS = 4

# def parse_biom(arr):

tree = os.walk(BIOM_FILE_PATH)
    
for i in tree:
    biom_arr = [*i]

Number_of_Files = len(biom_arr[2])

L_Q_Threads = Number_of_Files//Q_THREADS

    #разделим исходный массив на 4 подмассива, чтобы запустить 4 потока для каждого. 4 потому что TOTAL_BIOM делится на 4
biom_arr_4 = [[]*Q_THREADS for i in range(Q_THREADS)]

for i in range(Q_THREADS-1):
    biom_arr_4[i] = biom_arr[2][L_Q_Threads*i:L_Q_Threads*(i+1)]

biom_arr_4[Q_THREADS - 1] = biom_arr[2][L_Q_Threads*(Q_THREADS-1) : Number_of_Files]



for i in biom_arr_4:
    print(i)

    
    