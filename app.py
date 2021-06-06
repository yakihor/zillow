import numpy as np
import pandas as pd

import threading
from threading import Lock

import geopy
from geopy.geocoders import Nominatim
geopy.geocoders.options.default_user_agent = "yakymenkoihor0@gmail.com"
# geo = geocode (data ['addr'], provider = 'nominatim')

# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 30, fill = '█', printEnd = "\r"):    
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} || {percent}% {suffix} => {iteration} rows', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print(f'{prefix} 100% {suffix}')

def get_address(df, idx, geolocator, lat_field, lon_field, total):
    location = geolocator.reverse((df[lat_field], df[lon_field]))
    with Lock():
        row_count[idx] += 1
        percent = ("{0:." + str(1) + "f}").format(100 * (row_count[idx] / float(total)))
        print (f'{percent}% completed! => {row_count[idx]} / {total} rows')
        # printProgressBar(row_count[idx], total)

    # postcode = 0
    # address = location.raw['display_name']
    # if 'postcode' in location.raw['address']:
    #     postcode = location.raw['address']['postcode']
    
    # print (location.raw['display_name'].split())

    return location.raw['display_name']


geolocator = Nominatim(user_agent='zillow')

def thread1(i):
    print ('Thread ' + str(i) + ' started! =>')
    df = pd.read_csv('coord\\cood'+str(i)+'.csv')
    df = df.reindex(columns=list(df.columns) + ["Street Address","Year Built","Square Footage","Lot Size","Previous Sold","Tax"])

    row_count[i] = 0    
    addresses = df.apply(get_address, axis=1, idx=i, geolocator=geolocator, lat_field='Latitude', lon_field='Longitude', total=3600)
    df["Street Address"] = addresses

    print ('Getting addresses done! ' + str(row_count[i]) + ' rows')
    df.to_csv('info\\zillow'+ str(i) +'.csv')

row_count = [0] * 546
for i in range(100, 546):
    # threading.Thread(target=thread1, args=[i]).start();
    print ('Thread ' + str(i) + ' started! =>')
    df = pd.read_csv('./coord/cood'+str(i)+'.csv')
    print (df.columns)
    df = df.reindex(columns=list(df.columns) + ["Street Address","Year Built","Square Footage","Lot Size","Previous Sold","Tax"])
    print (df.columns)

    row_count[i] = 0    
    addresses = df.apply(get_address, axis=1, idx=i, geolocator=geolocator, lat_field='Latitude', lon_field='Longitude', total=3600)
    df["Street Address"] = addresses

    print ('Getting addresses done! ' + str(row_count[i]) + ' rows')
    df.to_csv('./info/zillow'+ str(i) +'.csv')