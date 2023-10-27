import pandas as pd
import os
from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import requests
import json
import ast
import csv
import sys
import time
from datetime import datetime
from datetime import datetime
import re
import numpy as np
import time
import json
from math import radians,sin,cos,tan,asin,sqrt

class usa_hyundai_137574():
    def __init__(self, brand, cat1, cat2, country, batch, moduleName, root, results, coordinates):
        self.brand = brand
        self.cat1 = cat1
        self.cat2 =cat2
        self.country = country
        self.api = 'https://www.hyundaiusa.com/var/hyundai/services/dealer/dealersByZip.json?brand=hyundai&model=all&lang=en_US&zip=35801&maxdealers=1000'
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"}
        self.batch = batch
        self.root = root
        self.results = results
        self.moduleName = moduleName
        self.coordinates = coordinates
    
    def start(self,):
        os.chdir(r"{}/{}".format(self.results, self.moduleName))
        if "batch_{}".format(self.batch) not in os.listdir():
            os.mkdir("batch_{}".format(self.batch))
        print()
        print('Checking connection to website API...............')
        try:
            self.source =requests.get(self.api, headers = self.headers)
        except:
            print("Website down.")
        if self.source.ok!= True:
            print("Status code:",self.source.status_code)
            print("Response:", self.source.reason)
            print('Something went wrong. Please check headers or Website API url.')
            sys.exit(1)
        else:
            print('Connection Successful!')
            print("Status code:",self.source.status_code)
            print("Response:",self.source.reason)

        os.chdir(r"{}/{}/batch_{}".format(self.results, self.moduleName, self.batch))
        fpout = open('{}_extraction_batch_{}.csv'.format(self.brand,self.batch).lower(), 'a+', encoding = 'UTF-8')
        header= 'lat|lon|name|address|cat1|cat2|brand|phone|email|url|openinghours\n'
        fpout.write(header)
        print()
        print('Data Extraction in progress for BATCH {}............................./'.format(self.batch))
        c = 0
        for code in self.coordinates:
            print(c)
            c+=1
            url = f'https://www.hyundaiusa.com/var/hyundai/services/dealer/dealersByZip.json?brand=hyundai&model=all&lang=en_US&zip={code}&maxdealers=1000'
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"}
            source = requests.get(url,headers = self.headers)
            stores = json.loads(source.text)
            try:
                a = stores['dealers']
            except:
                print('No stores')
                continue
            for i in stores['dealers']:
                lat =i['latitude']
                lon = i['longitude']
                name = i['dealerNm']
                address = '{}, {}, {}, {}'.format(i['address1'], i['city'], i['state'], i['zipCd'])
                cat1 = 'Stores'
                cat2 = 'Auto Dealership'
                brand = 'Hyundai'
                phone = i['phone']
                email = i['dealerEmail']
                url = 'https://www.hyundaiusa.com/us/en/dealer-locator'
                openinghours = "7:00AM - 5:00PM"
                op = '{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}\n'.format(lat,lon,name,address,cat1,cat2,brand,phone,email,url,openinghours)
                fpout.write(op)
        fpout.close()
        b = pd.read_csv("{}_extraction_batch_{}.csv".format(self.brand, self.batch).lower(), delimiter = '|').drop_duplicates().reset_index(drop = True)
        b['index'] = b.index + 1
        b.to_csv("{}_extraction_batch_{}.csv".format(self.brand, self.batch).lower())