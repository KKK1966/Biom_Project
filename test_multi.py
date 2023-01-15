import threading
from multiprocessing import Process
import queue
from time import sleep

q = queue.Queue()

count = 0


def getter():
    for item in range(0,200,2):
        sleep(0.013)
        q.put(item)

def getter2():
    for item in range(0,200,5):
        sleep(0.017)
        q.put(item)




def worker():

    while True:
        item = q.get()
        
        # print(f'Working on {item}')
        # print(f'Finished {item}')
        global count
        count +=1

        # q.task_done()


if __name__ == "__main__":

    # Turn-on the worker thread.

    p1 = Process(target=getter, daemon=True)
    p2 = Process(target=getter2, daemon=True)




    p1.start()
    p2.start()

    ppp = Process(target=worker, daemon=True)

    ppp.start()

    # Send thirty task requests to the worker.


    # Block until all tasks are done.
    # q.join()
    p1.join()
    p2.join()
    ppp.join()


    print('All work completed', count)