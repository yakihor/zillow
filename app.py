import numpy as np
import pandas as pd

import threading
from threading import Lock

import boto3
s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-2',
    aws_access_key_id='AKIAZINQ5KGBA2TFDRUC',
    aws_secret_access_key='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'
)

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
        print (f'Process {idx}: {percent}% completed! => {row_count[idx]} / {total} rows')

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

# Print out bucket names
for bucket in s3.buckets.all():
    print(bucket.name)

row_count = [0] * 546
for i in range(500, 546):
    # threading.Thread(target=thread1, args=[i]).start();
    print (f'Thread {i} started! =>')
    df = pd.read_csv(f'./coord/cood{i}.csv')
    print (df.columns)
    df = df.reindex(columns=list(df.columns) + ["Street Address","Year Built","Square Footage","Lot Size","Previous Sold","Tax"])
    print (df.columns)

    row_count[i] = 0    
    addresses = df.apply(get_address, axis=1, idx=i, geolocator=geolocator, lat_field='Latitude', lon_field='Longitude', total=3600)
    df["Street Address"] = addresses

    print ('Getting addresses done! ' + str(row_count[i]) + ' rows')
    df.to_csv(f'zillow{i}.csv')
    s3.Bucket('zillowinfo').upload_file(Filename=f'zillow{i}.csv', Key=f'zillow{i}.csv')

    for obj in s3.Bucket('zillowinfo').objects.all():
        print(obj)