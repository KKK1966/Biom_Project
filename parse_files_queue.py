import csv
from biom import load_table
from collections import Counter
import threading
import queue
from time  import sleep
import os
import re
import multiprocessing

# Определим таксон по умолчанию
TARGET = "s__"

# Количество потоков по умолчанию
Q_THREADS = 4

# Инициализируем количество упоминаний заданного таксона и суммарную SSU rRNA и 
# составим словарь-счетчик всех упоминаний различных  таксонов во всех файлах и их SSU rRNA

SSU_rRNA_by_taxon = Counter()
Number_by_taxon = Counter()

# Обьявляем очередь для передачи данных из потоков в Общие счетчики-словари
queue_collect_SSU = queue.Queue()


# Эта функция выгружает данные из файла biom в обьект класса biom. 
# Аргументы:
# 1. Массив полных имен файлов 
# 2. Имя таксона 
# Затем методами класса biom проводит фильтрацию по всем таксонам 
# создает словарь и добавляет словарь этого файла к глобальному словарю-счетчику
# Функция запускается в отдельных потоках
 
def parse_biom(arr, taxon):

    Dict_SSU_temp = {}
    Name_of_taxon_temp = ''
    SSU_rRNA_by_taxon_temp = 0

    for i in arr:
        try:
            table = load_table( i )
        except TypeError:
            continue
        f_filter = lambda values, id_, md: taxon in md['taxonomy']
        table.filter(f_filter, axis='observation', inplace=True)

        for i in table.ids(axis='observation'):
            Name_of_taxon_temp = table.metadata(id = i, axis = 'observation').get('taxonomy').split(';')[-1]
            SSU_rRNA_by_taxon_temp = table.data(id = i, axis='observation')[0]
            Dict_SSU_temp[Name_of_taxon_temp] = SSU_rRNA_by_taxon_temp

  
# Выгрузка происходит в  очередь

            # SSU_rRNA_by_taxon.update(Dict_SSU_temp)
        queue_collect_SSU.put(Dict_SSU_temp)
            # Number_by_taxon.update(Number_taxon_temp)
            # queue_collect_Number.put(Number_taxon_temp)

    return
 
def parse_tsv(arr,taxon):

    Dict_SSU_temp = {}
    Name_of_taxon_temp = ''
    SSU_rRNA_by_taxon_temp = 0

    for i in arr:

        with open(i, 'r') as f:
            try:
                tsv_reader = csv.reader(f, delimiter='\t')
            except TypeError:
                continue
            headers = next(tsv_reader)
            if len(headers) < 4:
                 headers = next(tsv_reader)

            index_taxonomy = headers.index('taxonomy')
            index_SSU_rRNA = headers.index('SSU_rRNA')

            for row in tsv_reader:
                if len(row) == 4:
                    if taxon in row[index_taxonomy]:
                        Name_of_taxon_temp = row[index_taxonomy].split(';')[-1]
                        SSU_rRNA_by_taxon_temp = float(row[index_SSU_rRNA])
                        Dict_SSU_temp[Name_of_taxon_temp] = SSU_rRNA_by_taxon_temp


            
                # SSU_rRNA_by_taxon.update(Dict_SSU_temp)
            queue_collect_SSU.put(Dict_SSU_temp)
                # Number_by_taxon.update(Number_taxon_temp)
                # queue_collect_Number.put(Number_taxon_temp)
    return

# Collector  забирает информацию из очереди и добавляет ее в соответствующие счетчики

def collector():

    while True:

        dict_temp = queue_collect_SSU.get()
        SSU_rRNA_by_taxon.update(dict_temp)

        for i in dict_temp : dict_temp[i] = 1
        Number_by_taxon.update(dict_temp)
        


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


# Формируем массив потоков
    threads = [threading.Thread(target = parse_biom, args = (biom_arr_by_threads[i],taxon,), daemon=True) for i in range(Q_THREADS)]

# Формируем поток для collector

    thread_collector = threading.Thread(target = collector, daemon=True)

# Старт потоков
    thread_collector.start()

    for thread in threads:
        thread.start()
    
    for thread in threads:
        thread.join()


    #получаем имена всех файлов в папке ./tsv
    dirname = os.path.dirname(__file__)
    tree = os.walk(dirname + "/tsv/")

# Формируем массив имен файлов для передачи в функцию обработки

    for i in tree:
        tsv_arr = [*i]

    Number_of_Files = len(tsv_arr[2])

    for i in range(Number_of_Files):
        tsv_arr[2][i] = dirname + "/tsv/" + tsv_arr[2][i]


#Разделим исходный массив на части для каждого потока


    L_Q_Threads = Number_of_Files//Q_THREADS
    
    tsv_arr_by_threads = [[]*Q_THREADS for i in range(Q_THREADS)]

    for i in range(Q_THREADS-1):
        tsv_arr_by_threads[i] = tsv_arr[2][L_Q_Threads*i:L_Q_Threads*(i+1)]

    tsv_arr_by_threads[Q_THREADS - 1] = tsv_arr[2][L_Q_Threads*(Q_THREADS-1) : Number_of_Files]


# Формируем массив потоков
    threads = [threading.Thread(target = parse_tsv, args = (tsv_arr_by_threads[i],taxon,), daemon=True) for i in range(Q_THREADS)]

# Старт потоков
    for thread in threads:
        thread.start()
    
    for thread in threads:
        thread.join()

# Вывод результата обработки в файл

    with open(dirname +'/taxon.csv', 'w', newline='') as csvfile:

        writer = csv.writer(csvfile, quoting=csv.QUOTE_NONNUMERIC)

        arr_temp = [[],[],[]]

        for i in SSU_rRNA_by_taxon:
            arr_temp[0] = i
            arr_temp[1] = SSU_rRNA_by_taxon[i]
            arr_temp[2] = Number_by_taxon[i]
            writer.writerow(arr_temp)