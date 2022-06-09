# Script per l'header del sistema
from funcx import FuncXClient
import time
from functions import *

if __name__ == '__main__':
    # Istanziare la classe
    fxc = FuncXClient()

    # Endpoint: Linux home
    LinuxEndpoint = 'firstEndPoint'

    # Endpoint: PurpleJeans
    PurpleEndpoint = 'secondEndPoint'

    # Endpoint: Purplejeans 2
    PurpleEndpoint2 = 'thirdEndPoint'

    # Function UUID
    UUIDParserTemperature = fxc.register_function(parserTemperature)
    UUIDParserLW = fxc.register_function(parserLW)
    UUIDParserPrecip = fxc.register_function(parserPrecip)

    # Execute remote function
    resTemperature = fxc.run(function_id=UUIDParserTemperature, endpoint_id=PurpleEndpoint)
    resLW = fxc.run(function_id=UUIDParserLW, endpoint_id=LinuxEndpoint)
    resPrecip = fxc.run(function_id=UUIDParserPrecip, endpoint_id=PurpleEndpoint2)

    time.sleep(3)
    
    # Recupera lo stato di esecuzione del task
    while fxc.get_task(resTemperature)['pending'] == True:
        time.sleep(3)
    
    # Recupera lo stato di esecuzione del task
    while fxc.get_task(resLW)['pending'] == True:
        time.sleep(3)    
    
    # Recupera lo stato di esecuzione del task
    while fxc.get_task(resPrecip)['pending'] == True:
        time.sleep(3)
    
    # Recupera i risultati del task singolo
    print(fxc.get_result(resTemperature))
    print(fxc.get_result(resLW))
    print(fxc.get_result(resPrecip))