import csv
from biom import load_table
import threading
import time
import os
import re

# Определим таксон по умолчанию
TARGET = "s__bacterium_Ellin7504"

# Количество потоков по умолчанию
Q_THREADS = 4

# Инициализируем количество упоминаний заданного таксона и суммарную плотность

Quantity_by_taxon = 0
Sum_by_taxon = 0

# Эта функция выгружает данные из файла biom в обьект класса biom. 
# Аргументы:
# 1. Массив полных имен файлов 
# 2. Имя таксона 
# Затем методами класса biom проводит фильтрацию по заданному таксону и 
# определение Sum_by_taxon и Quantity_by_taxon
# Функция запускается в отдельных потоках
 
def parse_biom(arr, taxon):
    for i in arr:
        table = load_table( i )
        f_filter = lambda values, id_, md: taxon in md['taxonomy']
        env = table.filter(f_filter, axis='observation', inplace=False)
  
# При обращении функции к общим переменным происходит блокировка, 
# чтобы не возникло конфликта между потоками

        with lock:
            global Sum_by_taxon , Quantity_by_taxon
            Sum_by_taxon += env.sum(axis='whole')
            Quantity_by_taxon  += len(env.ids(axis='observation'))


    return
 

if __name__ == "__main__":

# Ввод имени таксона. Если имя не вводится то используется таксон по умолчанию

    taxon = input("Введите имя таксона или нажмите Enter (Имя по умолчанию %s )\n\n" % TARGET)

    
    if taxon == "" : taxon = TARGET

#получаем имена всех файлов в папке ./biom
    dirname = os.path.dirname(__file__)
    tree = os.walk(dirname + "/biom/")

# Формируем массив имен файлов для передачи в функцию обработки

    for i in tree:
        biom_arr = [*i]

    Number_of_Files = len(biom_arr[2])

    for i in range(Number_of_Files):
        biom_arr[2][i] = dirname + "/biom/" + biom_arr[2][i]


#Разделим исходный массив на части для каждого потока

    L_Q_Threads = Number_of_Files//Q_THREADS
    
    biom_arr_by_threads = [[]*Q_THREADS for i in range(Q_THREADS)]

    for i in range(Q_THREADS-1):
        biom_arr_by_threads[i] = biom_arr[2][L_Q_Threads*i:L_Q_Threads*(i+1)]

    biom_arr_by_threads[Q_THREADS - 1] = biom_arr[2][L_Q_Threads*(Q_THREADS-1) : Number_of_Files]
        
# Запускаем метод блокировки для избежания конфликта между потоками при обращении к одним переменным
    lock = threading.Lock()

# Формируем массив потоков
    threads = [threading.Thread(target = parse_biom, args = (biom_arr_by_threads[i],taxon,)) for i in range(Q_THREADS)]

# Старт потоков
    for thread in threads:
        thread.start()
    
    for thread in threads:
        thread.join()


# Вывод результата обработки

    print("\n\nDone!\n")

    print('Taxon ', taxon ,'\nSum_by_taxon = ', Sum_by_taxon, '\nQuantity_by_taxon = ', Quantity_by_taxon)

    print("\n\n")