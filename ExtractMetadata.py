'''
# This script simulates the PC header that takes care of calling the parsers
# to extract the metadata. Moreover, having to use funcX is an excellent
# example of how this framework works.
'''
from funcx import FuncXClient
import time
from functions import *

if __name__ == '__main__':
    # Instantiate the class
    fxc = FuncXClient()

    # Endpoint: Linux home
    LinuxEndpoint = '$ENDPOINT_CODE1'

    # Endpoint: PurpleJeans
    PurpleEndpoint = '$ENDPOINT_CODE2'

    # Endpoint: Purplejeans 2
    PurpleEndpoint2 = '$ENDPOINT_CODE3'

    # Function UUID
    UUIDParserTemperature = fxc.register_function(parserTemperature)
    UUIDParserLW = fxc.register_function(parserLW)
    UUIDParserPrecip = fxc.register_function(parserPrecip)

    # Execute remote function
    resTemperature = fxc.run(function_id=UUIDParserTemperature, endpoint_id=PurpleEndpoint)
    resLW = fxc.run(function_id=UUIDParserLW, endpoint_id=LinuxEndpoint)
    resPrecip = fxc.run(function_id=UUIDParserPrecip, endpoint_id=PurpleEndpoint2)

    time.sleep(3)
    
    # get the task execution status
    while fxc.get_task(resTemperature)['pending'] == True:
        time.sleep(3)
    
    # get the task execution status
    while fxc.get_task(resLW)['pending'] == True:
        time.sleep(3)    
    
    # get the task execution status
    while fxc.get_task(resPrecip)['pending'] == True:
        time.sleep(3)
    
    # Get the results of the single task
    print(fxc.get_result(resTemperature))
    print(fxc.get_result(resLW))
    print(fxc.get_result(resPrecip))