import multiprocessing
import os

def printsquare(q):
    while not q.empty():
        print(pow(2, q.get()))
    print('Queue is now empty')

def push_queue(mylist, q):
    for num in mylist:
        q.put(num)

if __name__ == '__main__':

    mainlist = [0,1,2,3,4,5,6,7,8,9,10]

    q = multiprocessing.Queue()
    p1 = multiprocessing.Process(target=push_queue, args=(mainlist, q,))
    p2 = multiprocessing.Process(target=printsquare, args=(q,))

    p1.start()
    p1.join()

    p2.start()
    p2.join()
