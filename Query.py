'''
# This script simulates the PC header that queries via the web interface.
# Again, funcX is used.
'''
from funcx import FuncXClient
import time
from functions import *
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--lat_min', type=float, nargs='?', help='Latitudine Minima')
    parser.add_argument('--lat_max', type=float, nargs='?', help='Latitudine Massima')
    parser.add_argument('--long_min', type=float, nargs='?', help='Longitudine Minima')
    parser.add_argument('--long_max', type=float, nargs='?', help='Latitudine Massima')
    parser.add_argument('--data_min', type=str, nargs='?', help='Data Minima')
    parser.add_argument('--data_max', type=str, nargs='?', help='Data Massima')
    parser.add_argument('--misura', type=str, nargs='?', help='File Misura da utilizzare', choices=["Air Surface Temperature Anomaly", "Precipitation", "Outgoing Longwave Radiation"])
    
    args = parser.parse_args()

    # Instantiate the class
    fxc = FuncXClient()

    # Endpoint: Purplejeans 2
    PurpleEndpoint2 = '$ENDPOINT_CODE2'

    # Function UUID
    if args.misura == "Air Surface Temperature Anomaly":
        UUID_Query = fxc.register_function(queryTemperature)
    elif args.misura == "Precipitation":
        UUID_Query = fxc.register_function(queryPrecipitation)
    elif args.misura == "Outgoing Longwave Radiation":
        UUID_Query = fxc.register_function(queryLW)
    
    # Execute remote function
    resQuery = fxc.run(args.lat_min, args.lat_max, args.long_min, args.long_max, args.data_min, args.data_max, args.misura, function_id=UUID_Query, endpoint_id=PurpleEndpoint2)
    time.sleep(3)

    # get the task execution status
    while fxc.get_task(resQuery)['pending'] == True:
        time.sleep(3)

    # Get the results of the single task
    print(fxc.get_result(resQuery))