from ibapi.client import EClient
from ibapi.wrapper import EWrapper

class SampleClient (EWrapper, EClient):
    def __init__(self, addr, port,client_id):
        EWrapper.__init__(self)
        EClient.__init__(self,self)

        # Connect to TWS
        self.connect(addr, port, client_id)
        
        #Launch the client thread
        thread = Thread(target=self.run)
        thread.start()
        
    #@iswrapper
    def CurrentTime(self, cur_time):
        t = datetime.fromtimestamp(cur_time)
        print ('Current time: {}'.format(t))
        
    #@iswrapper
    def error(self,req_id,code,msg):
        print ('Error {}: {}'.format(code, msg))
        
def main():
    # Create the client and connect to TWS
    client = SimpleClient('157.46.98.104', 7497,0)
    client.reqCurrentTime()
    
    #Sleep while the request is processed
    time.sleep(1)
    
    #Disconnect from TWS
    client.disconnect()
