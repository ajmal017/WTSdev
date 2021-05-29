# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import time
from threading import Thread

from ibapi.client import EClient
from ibapi.wrapper import EWrapper


class SampleClient(EWrapper, EClient):
    def __init__(self, addr, port, client_id):
        EWrapper.__init__(self)
        EClient.__init__(self, self)

        # Connect to TWS
        self.connect(addr, port, client_id)

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()

    def iswrapper(fn):
        return fn

    @iswrapper
    def CurrentTime(self, cur_time):
        t = datetime.fromtimestamp(cur_time)
        print('Current time: {}'.format(t))

    @iswrapper
    def error(self, req_id, code, msg):
        print('Error {}: {}'.format(code, msg))


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # print_hi('PyCharm')

    # Create the client and connect to TWS

    # client = SampleClient
    client = SampleClient('127.0.0.1', 7497, 200)

    client.reqCurrentTime()

    # Sleep while the request is processed
    time.sleep(0.5)

    # Disconnect from TWS
    client.disconnect()
    print('Done')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
