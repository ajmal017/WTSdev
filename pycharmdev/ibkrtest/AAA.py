proc2.start()
time.sleep(0.25)

proc3.start()
time.sleep(0.25)

proc4.start()
time.sleep(0.25)

proc5.start()
time.sleep(0.25)

proc6.start()
time.sleep(0.25)

proc1.join()
proc2.join()
proc3.join()
proc4.join()
proc5.join()
proc6.join()

time.sleep(0.25)