# COMPLETO
def parserPrecip():
    import netCDF4 as nc
    import requests
    import os.path
    import datetime

    for anno in range(1979, 2023):
        # Download and get the file
        URL = 'https://downloads.psl.noaa.gov/Datasets/cpc_global_precip/precip.' + str(anno) + '.nc'
        filename = URL.rsplit("/", -1)[-1]

        if not os.path.exists(filename):
            response = requests.get(URL)
            open(filename, "wb").write(response.content)

        # Create the dataset struct
        ds = nc.Dataset(filename)

        tupla = {}

        # Estrai latitudine
        latitudine = ds['lat'].__dict__
        lat_max = latitudine['actual_range'][0]
        lat_min = latitudine['actual_range'][1]

        tupla['lat_min'] = lat_min
        tupla['lat_max'] = lat_max

        # Estrai longitudine
        longitude = ds['lon'].__dict__
        long_min = longitude['actual_range'][0]
        long_max = longitude['actual_range'][1]

        tupla['long_min'] = long_min
        tupla['long_max'] = long_max
        
        # Make Grid
        lats = ds['lat'][:].compressed()
        longs = ds['lon'][:].compressed()
        gridY = abs(lats[1] - lats[0])
        gridX = abs(longs[1] - longs[0])

        # Make Time
        time = ds['time'].__dict__
        time_min = time['actual_range'][0]
        time_max = time['actual_range'][1]
        t_unit = time['units']
        t_cal = u"gregorian" # or standard

        datevar = []
        datevar.append(nc.num2date(time_min, units = t_unit,calendar = t_cal))
        datevar.append(nc.num2date(time_max, units = t_unit,calendar = t_cal))

        time_modified_min = datetime.datetime.strptime(str(datevar[0]), "%Y-%m-%d %H:%M:%S")
        time_modified_max = datetime.datetime.strptime(str(datevar[1]), "%Y-%m-%d %H:%M:%S")

        date_min = str(time_modified_min.year) + "-" + str(time_modified_min.month) + "-" + str(time_modified_min.day)
        date_max = str(time_modified_max.year) + "-" + str(time_modified_max.month) + "-" + str(time_modified_max.day)

        tupla['time_min'] = date_min
        tupla['time_max'] = date_max

        field = ds['precip'].__dict__
        description = field['var_desc']

        import psycopg2

        sql = """INSERT INTO files(lat_min, lat_max, long_min, long_max, grid_Y, grid_X, time_min, time_max, link, misura) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

        conn = None
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect("connector to database")

            # create a new cursor
            cur = conn.cursor()
    
            # execute the INSERT statement
            cur.execute(sql, (str(lat_min), str(lat_max), str(long_min), str(long_max), str(gridY), str(gridX), date_min, date_max, URL, str(description),))

            # commit the changes to the database
            conn.commit()
            
            # close communication with the database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    return tupla


def parserLW():
    
    import datetime
    import netCDF4 as nc
    import requests
    import os.path

    # Download and get the file
    URL = 'https://downloads.psl.noaa.gov/Datasets/olrcdr/olr.day.mean.nc'
    
    filename = URL.rsplit("/", -1)[-1]

    if not os.path.exists(filename):
        response = requests.get(URL)
        open(filename, "wb").write(response.content)

    # Create the dataset struct
    ds = nc.Dataset(filename)

    tupla = {}

    # Estrai latitudine
    latitudine = ds['lat'].__dict__
    lat_max = latitudine['actual_range'][0]
    lat_min = latitudine['actual_range'][1]

    tupla['lat_min'] = lat_min
    tupla['lat_max'] = lat_max

    # Estrai longitudine
    latitudine = ds['lon'].__dict__
    long_min = latitudine['actual_range'][0]
    long_max = latitudine['actual_range'][1]

    tupla['long_min'] = long_min
    tupla['long_max'] = long_max

    # Make Grid
    lats = ds['lat'][:].compressed()
    longs = ds['lon'][:].compressed()
    gridY = abs(lats[1] - lats[0])
    gridX = abs(longs[1] - longs[0])

    # Make Time
    time = ds['time'].__dict__
    time_min = time['actual_range'][0]
    time_max = time['actual_range'][1]
    t_unit = time['units']
    t_cal = u"gregorian" # or standard

    datevar = []
    datevar.append(nc.num2date(time_min, units = t_unit,calendar = t_cal))
    datevar.append(nc.num2date(time_max, units = t_unit,calendar = t_cal))

    time_modified_min = datetime.datetime.strptime(str(datevar[0]), "%Y-%m-%d %H:%M:%S")
    time_modified_max = datetime.datetime.strptime(str(datevar[1]), "%Y-%m-%d %H:%M:%S")

    date_min = str(time_modified_min.year) + "-" + str(time_modified_min.month) + "-" + str(time_modified_min.day)
    date_max = str(time_modified_max.year) + "-" + str(time_modified_max.month) + "-" + str(time_modified_max.day)

    tupla['time_min'] = date_min
    tupla['time_max'] = date_max

    field = ds['olr'].__dict__
    description = field['var_desc']

    import psycopg2

    sql = """INSERT INTO files(lat_min, lat_max, long_min, long_max, grid_Y, grid_X, time_min, time_max, link, misura) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect("connector to database")

        # create a new cursor
        cur = conn.cursor()
  
        # execute the INSERT statement
        cur.execute(sql, (str(lat_min), str(lat_max), str(long_min), str(long_max), str(gridY), str(gridX), date_min, date_max, URL, str(description),))

        # commit the changes to the database
        conn.commit()
        
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return tupla


def parserTemperature():
    import netCDF4 as nc
    import requests
    import os.path
    import datetime
    
    for anno in range(1880, 2030, 10):
        # Download and get the file
        URL = 'http://berkeleyearth.lbl.gov/auto/Global/Gridded/Complete_TAVG_Daily_LatLong1_' + str(anno) + '.nc'
        
        filename = URL.rsplit("/", -1)[-1]

        if not os.path.exists(filename):
            response = requests.get(URL)
            open(filename, "wb").write(response.content)

        # Create the dataset struct
        ds = nc.Dataset(filename)

        tupla = {}

        # COMPLETO (Estrazione tramite dati e non metadati)
        '''
        Il Berkley non ha i range definiti
        nei metadati quindi bisogna estrarre dai dati
        '''
        import datetime

        # Estrai latitudine
        latitudine = ds['latitude'][:].compressed()
        lat_min = latitudine[0]
        lat_max = latitudine[-1]

        tupla['lat_min'] = lat_min
        tupla['lat_max'] = lat_max

        # Estrai longitudine
        longitude = ds['longitude'][:].compressed()
        long_min = longitude[0]
        long_max = longitude[-1]

        tupla['long_min'] = long_min
        tupla['long_max'] = long_max

        # Make Grid
        lats = ds['latitude'][:].compressed()
        longs = ds['longitude'][:].compressed()
        gridY = abs(lats[1] - lats[0])
        gridX = abs(longs[1] - longs[0])

        # Crea tempo
        time = ds['year'][:].compressed()
        time_min = datetime.datetime(int(time[0]), 1, 1)
        time_max = datetime.datetime(int(time[-1]), 12, 31)

        date_min = time_min.strftime("%Y-%m-%d")
        date_max = time_max.strftime("%Y-%m-%d")

        tupla['time_min'] = date_min
        tupla['time_max'] = date_max

        field = ds['temperature'].__dict__
        description = field['long_name']

        import psycopg2

        sql = """INSERT INTO files(lat_min, lat_max, long_min, long_max, grid_Y, grid_X, time_min, time_max, link, misura) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

        conn = None
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect("connector to database")

            # create a new cursor
            cur = conn.cursor()
    
            # execute the INSERT statement
            cur.execute(sql, (str(lat_min), str(lat_max), str(long_min), str(long_max), str(gridY), str(gridX), date_min, date_max, URL, str(description),))

            # commit the changes to the database
            conn.commit()
            
            # close communication with the database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    return tupla

# Query function
def queryTemperature(lat_min, lat_max, long_min, long_max, date_min, date_max, misura):
    
    import datetime
    import psycopg2
    import re
    import numpy as np
    import numpy.ma as ma
    import netCDF4 as nc
    import os
    import requests

    # QUERY SIDE
    sql_query = """ SELECT link \
                    FROM files \
                    WHERE \
                        lat_min <= %s AND lat_max >= %s AND \
                        lat_min <= %s AND lat_max >= %s AND \
                        long_min <= %s AND long_max >= %s AND \
                        long_min <= %s AND long_max >= %s AND \
                        time_min >= %s AND time_max <= %s AND \
                        misura = %s \
                """

    queryResults = None
    conn = None
    
    loc_date_min = datetime.datetime.strptime(date_min, "%Y-%m-%d")
    loc_date_max = datetime.datetime.strptime(date_max, "%Y-%m-%d")

    app_dmin = re.sub(r".$", "0", str(loc_date_min.year))
    app_dmax = re.sub(r".$", "9", str(loc_date_max.year))

    query_data_min = app_dmin + '-01-01'
    query_data_max = app_dmax + '-12-31'

    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect("connector to database")

        # create a new cursor
        cur = conn.cursor()

        # execute the QUERY statement
        cur.execute(sql_query, (str(lat_min), str(lat_min), str(lat_max), str(lat_max), str(long_min), str(long_min), str(long_max), str(long_max), query_data_min, query_data_max, str(misura),))

        # Get Query results
        queryResults = cur.fetchall()
        
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    spero_query = np.asarray(queryResults)

    list_spero_query = spero_query.tolist()

    if os.path.exists('TupleTemperatures.txt'):
        os.remove('TupleTemperatures.txt')

    # PARSER SIDE
    Links = []

    for i in list_spero_query:
        Links.append(i[0])

    datetime_beg = datetime.datetime.strptime(date_min, '%Y-%m-%d')
    datetime_end = datetime.datetime.strptime(date_max, '%Y-%m-%d')

    days_off_beg = datetime_beg.timetuple().tm_yday
    days_off_end = datetime_end.timetuple().tm_yday
    year_off_beg = int(repr(datetime_beg.year)[-1])
    year_off_end = int(repr(datetime_end.year)[-1])

    index_beg = 365 * year_off_beg + days_off_beg
    index_end = 365 * year_off_end + days_off_end

    num_NETCDF = len(Links)
    contatore = 0
    locale_sinistro = None
    locale_destro = None
    
    # Dictionary Variables
    infoData = {}
    numElems = 0

    while contatore < num_NETCDF:
        # Apri file NETCDF4
        nomeFile = Links[contatore].rsplit("/", -1)[-1]

        if not os.path.exists(nomeFile):
            response = requests.get(Links[contatore])
            open(nomeFile, "wb").write(response.content)
        ds = nc.Dataset(nomeFile)

        day = ds['day'][:]
        month = ds['month'][:]
        year = ds['year'][:]

        temperature = ds['temperature'][:]
        latitudini = ds['latitude'][:].compressed()
        longitudini = ds['longitude'][:].compressed()

        rangeLat = np.where((latitudini >= float(lat_min)) & (latitudini <= float(lat_max)))
        rangeLong = np.where((longitudini >= float(long_min)) & (longitudini <= float(long_max)))

        spero1 = np.asarray(rangeLat)
        spero2 = np.asarray(rangeLong)

        listrangeLat = spero1.tolist()[0]
        listrangeLong = spero2.tolist()[0]

        # 1 file
        if num_NETCDF == 1:
            locale_sinistro = index_beg
            locale_destro = index_end
        # Inizio
        elif contatore == 0:
            locale_sinistro = index_beg
            locale_destro = 3652
        # Centrale
        elif contatore > 0 and contatore < num_NETCDF-1:
            locale_sinistro = 0
            locale_destro = 3652
        else:
        # Fine
            locale_sinistro = 0
            locale_destro = index_end

        # Scrivi direttamente su file
        with open(r'TupleTemperatures.txt', 'a') as fp:
            for z in range(locale_sinistro, locale_destro):
                for i in listrangeLat:
                    for j in listrangeLong:
                        if temperature[z][i][j] is not ma.masked:
                            numElems += 1
                            giornata = str(int(year[z])) + '-' + str(int(month[z])) + '-' + str(int(day[z]))
                            elem = {'lat': latitudini[i], 'long': longitudini[j], 'day': giornata, 'temp': temperature[z][i][j]}
                            fp.write("%s\n" % elem)
        
        contatore += 1
        fp.close()
    
    infoData = {'numNetCDF': num_NETCDF, 'numElements': numElems}
    return infoData

def queryPrecipitation(lat_min, lat_max, long_min, long_max, data_min, data_max, misura):
    
    # Codice della query per estrarre i link
    import datetime
    import psycopg2
    import netCDF4 as nc
    import numpy as np
    import datetime
    import numpy.ma as ma
    import os
    import requests

    sql_query = """ SELECT  link \
                    FROM 	files \
                    WHERE   lat_min <= %s AND lat_max >= %s AND \
                            long_min <= %s AND long_max >= %s AND \
                            time_min >= %s AND time_max <= %s AND \
                            misura = %s; \
                """

    queryResults = None
    conn = None

    loc_date_min = datetime.datetime.strptime(data_min, "%Y-%m-%d")
    loc_date_max = datetime.datetime.strptime(data_max, "%Y-%m-%d")

    query_data_min = str(loc_date_min.year) + '-01-01'
    query_data_max = str(loc_date_max.year) + '-12-31'

    # Esegui Query
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect("connector to database")

        # create a new cursor
        cur = conn.cursor()

        # execute the QUERY statement
        cur.execute(sql_query, (str(lat_min), str(lat_max), str(long_min), str(long_max), query_data_min, query_data_max, str(misura),))

        # Get Query results
        queryResults = cur.fetchall()
        
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    spero_query = np.asarray(queryResults)
    list_spero_query = spero_query.tolist()

    if os.path.exists('TuplePrecipitation.txt'):
        os.remove('TuplePrecipitation.txt')

    Links = []

    for i in list_spero_query:
        Links.append(i[0])

    ''' Filtraggio dati '''

    # Calcola offset inizio e fine
    datetime_beg = datetime.datetime.strptime(data_min, '%Y-%m-%d')
    datetime_end = datetime.datetime.strptime(data_max, '%Y-%m-%d')

    index_beg = datetime_beg.timetuple().tm_yday
    index_end = datetime_end.timetuple().tm_yday

    num_NETCDF = len(Links)
    contatore = 0
    locale_sinistro = None
    locale_destro = None

    # Dictionary Variables
    infoData = {}
    numElems = 0

    while contatore < num_NETCDF:
        # Apri file NETCDF4
        nomeFile = Links[contatore].rsplit("/", -1)[-1]

        if not os.path.exists(nomeFile):
            response = requests.get(Links[contatore])
            open(nomeFile, "wb").write(response.content)
        ds = nc.Dataset(nomeFile)

        time = ds['time'].__dict__
        current_time = ds['time'][:]
        t_unit = time['units']
        t_cal = u"gregorian" # or standard

        precipitazioni = ds['precip'][:]
        latitudini = ds['lat'][:].compressed()
        longitudini = ds['lon'][:].compressed()

        rangeLat = np.where((latitudini >= lat_min) & (latitudini <= lat_max))
        rangeLong = np.where((longitudini >= long_min) & (longitudini <= long_max))

        spero1 = np.asarray(rangeLat)
        spero2 = np.asarray(rangeLong)

        listrangeLat = spero1.tolist()[0]
        listrangeLong = spero2.tolist()[0]

        if num_NETCDF == 1:
            locale_sinistro = index_beg
            locale_destro = index_end
        # Inizio
        elif contatore == 0:
            locale_sinistro = index_beg
            locale_destro = 364
        # Centrale
        elif contatore > 0 and contatore < num_NETCDF-1:
            locale_sinistro = 0
            locale_destro = 364
        else:
        # Fine
            locale_sinistro = 0
            locale_destro = index_end

        with open(r'TuplePrecipitation.txt', 'a') as fp:
            for z in range(locale_sinistro, locale_destro):
                for i in listrangeLat:
                    for j in listrangeLong:
                        if precipitazioni[z][i][j] is not ma.masked:
                            numElems += 1
                            current_date = nc.num2date(current_time[z], units = t_unit,calendar = t_cal)
                            elem = {'lat': latitudini[i], 'long': longitudini[j], 'day': str(current_date), 'prec': precipitazioni[z][i][j]}
                            fp.write("%s\n" % elem)

        contatore += 1
        fp.close()

    infoData = {'numNetCDF': num_NETCDF, 'numElements': numElems}
    return infoData

def queryLW(lat_min, lat_max, long_min, long_max, data_min, data_max, misura):
    import datetime
    import psycopg2
    import netCDF4 as nc
    import numpy as np
    import datetime
    import numpy.ma as ma
    import os
    import requests

    # Basta cercare la misura "Outgoing Longwave Radiation"
    sql_query = """ SELECT  link \
                    FROM 	files \
                    WHERE   misura = %s;
                """

    queryResults = None
    conn = None

    # Esegui Query
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect("connector to database")

        # create a new cursor
        cur = conn.cursor()

        # execute the QUERY statement
        cur.execute(sql_query, (str(misura),))

        # Get Query results
        queryResults = cur.fetchall()
        
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    spero_query = np.asarray(queryResults)
    list_spero_query = spero_query.tolist()

    if os.path.exists('TupleLW.txt'):
        os.remove('TupleLW.txt')

    Links = list_spero_query[0]

    ''' Filtraggio dati '''

    # Dictionary Variables
    infoData = {}
    numElems = 0

    # Apri file NETCDF4
    nomeFile = Links[0].rsplit("/", -1)[-1]

    if not os.path.exists(nomeFile):
        response = requests.get(Links[0])
        open(nomeFile, "wb").write(response.content)
    ds = nc.Dataset(nomeFile)

    time = ds['time'].__dict__
    current_time = ds['time'][:]
    t_unit = time['units']
    t_cal = u"gregorian" # or standard

    datetime_min = datetime.datetime.strptime(data_min, "%Y-%m-%d")
    datetime_max = datetime.datetime.strptime(data_max, "%Y-%m-%d")

    date_min_number = nc.date2num(datetime_min, units = t_unit, calendar = t_cal)
    date_max_number = nc.date2num(datetime_max, units = t_unit, calendar = t_cal)

    index_begNP = np.where(current_time == date_min_number)
    index_endNP = np.where(current_time == date_max_number)

    speroindex_beg = np.asarray(index_begNP)
    speroindex_end = np.asarray(index_endNP)

    index_beg = speroindex_beg.tolist()[0]
    index_end = speroindex_end.tolist()[0]

    time = ds['time'].__dict__
    current_time = ds['time'][:]
    t_unit = time['units']
    t_cal = u"gregorian" # or standard

    latitudini = ds['lat'][:].compressed()
    longitudini = ds['lon'][:].compressed()

    rangeLat = np.where((latitudini >= lat_min) & (latitudini <= lat_max))
    rangeLong = np.where((longitudini >= long_min) & (longitudini <= long_max))

    spero1 = np.asarray(rangeLat)
    spero2 = np.asarray(rangeLong)

    listrangeLat = spero1.tolist()[0]
    listrangeLong = spero2.tolist()[0]

    with open(r'TupleLW.txt', 'w') as fp:
        for z in range(index_beg[0], index_end[0]):
            for i in listrangeLat:
                for j in listrangeLong:
                    try:
                        if ds['olr'][z][i][j] is not ma.masked:
                            current_date = nc.num2date(current_time[z], units = t_unit, calendar = t_cal)
                            elem = {'lat': latitudini[i], 'long': longitudini[j], 'day': str(current_date), 'olr': ds['olr'][z][i][j]}
                            fp.write("%s\n" % elem)
                            numElems += 1
                    except(Exception) as error:
                        current_date = nc.num2date(current_time[z], units = t_unit, calendar = t_cal)
                        elem = {'lat': latitudini[i], 'long': longitudini[j], 'day': str(current_date), 'olr': error}
                        fp.write("%s\n" % elem)

    fp.close()
    infoData = {'numNetCDF': 1, 'numElements': numElems}
    return infoData