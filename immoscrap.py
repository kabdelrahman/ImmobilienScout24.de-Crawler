# -*- coding: utf-8 -*-

####PRELIMINARIES####

#module import#
import requests
from bs4 import BeautifulSoup
from pandas import DataFrame
import re
import datetime
import sys
import os

#get current date#
current_datetime_master = datetime.datetime.now()
current_datetime = current_datetime_master.strftime("%Y%m%d_%H%M%S")

#get user input#
userinput_type = [x for x in sys.argv if x.startswith("--type")]
if len(userinput_type) > 0:
    userinput_type = userinput_type[0]
    userinput_type = re.search("(?<=\=).*", userinput_type).group()

userinput_payment = [x for x in sys.argv if x.startswith("--payment")]
if len(userinput_payment) > 0:
    userinput_payment = userinput_payment[0]
    userinput_payment = re.search("(?<=\=).*", userinput_payment).group()

#define sub-sites# 
site_list = ["/Wohnung-Miete", 
             "/Haus-Miete",
             "/Wohnung-Kauf", 
             "/Haus-Kauf"]

#filter accordingly to user input#
if "h" not in userinput_type:
    site_list = [x for x in site_list if not x.startswith("/Haus")]
    
if "f" not in userinput_type:
    site_list = [x for x in site_list if not x.startswith("/Wohnung")]
    
if "r" not in userinput_payment:
    site_list = [x for x in site_list if not x.endswith("-Miete")]
    
if "b" not in userinput_payment:
    site_list = [x for x in site_list if not x.endswith("-Kauf")]

#define top level url#
domain="https://www.immobilienscout24.de/Suche/S-T"

#initialize dataframe#
immoscout_data = DataFrame()


####EXTRACTING LINKS FOR CRAWLER####

#get the last link for every asset type#
def get_max(url):
    try:
        url = requests.get(url)
    except Exception:
        print("Fehler beim Oeffnen der Website")
    try:
        site_extract = BeautifulSoup(url.text, "lxml")
    except Exception:
        print("Fehler beim Einlesen in BeautifulSoup")
    try:
        max_link = max([int(n["value"]) for n in site_extract.find_all("option")])#get the maximum value for links in a specific sub-site
    except Exception:
        print("Fehler beim Loop")
    else:
        return max_link
    


####EXTRACT DATA FROM EVERY SINGLE LINK####
    
def get_data(url):
    try:
        url_raw = url#save url as string for real estate type
        url = requests.get(url)
    except Exception:
        return None
    except Exception:
        return None
    try:
        site_extract = BeautifulSoup(url.text, "lxml")
        rawdata_extract = site_extract.find_all("div", {"class":"result-list-entry__data"})#extract every result box
    except AttributeError as e:
        return None
    global immoscout_data#use global dataframe
    price = []
    size = []
    location = []
    ownership = []
    immo_type = []
    for i in range(0,len(rawdata_extract)):
        try:
            price.append(rawdata_extract[i].find_all("dd")[0].get_text().strip())#extract price
        except Exception:
            price.append(None)
        try:
            size.append(rawdata_extract[i].find_all("dd")[1].get_text().strip())#extract size
        except Exception:
            size.append(None)
        try:
            location.append(rawdata_extract[i].find_all("div", {"class":"result-list-entry__address"})[0].get_text().strip())#extract location
        except Exception:
            location.append(None)
            
        if "/Wohnung" in url_raw:
            immo_type.append("Wohnung")
        elif "/Haus" in url_raw:
            immo_type.append("Haus")
        if "-Miete" in url_raw:
            ownership.append("Miete")
        elif "-Kauf" in url_raw:
            ownership.append("Kauf")
    immoscout_data = immoscout_data.append(DataFrame({"price":price, 
                                                  "size":size,
                                                  "location":location, 
                                                  "real_estate":immo_type, 
                                                  "ownership":ownership}), 
    ignore_index=True)
    



####START CRAWLER####

def immo_crawl(site_list):
    max_dict = {}#initialize dictionary for maximum values of links
    for site in site_list:
        max_dict[site] = get_max(domain+site)#associate maximal link value with specific sub-site
    link_list_full = []#initialize list for full links to crawl#
    for site in max_dict:
        for i in range(1,max_dict[site]):
            link_list_full.append(domain+"/P-"+str(i)+site)#populate link_list_full
    link_count = 1#start for progress indicator
    len_link_list_full = len(link_list_full)#end for progress indicator
    for link in link_list_full:
        print("Crawling: "+link+" (link #"+str(link_count)+" of "+str(len_link_list_full)+")")#print progress
        link_count += 1#add to progress indicator
        get_data(link)
    
    
immo_crawl(site_list)#start crawler


####PROCESS DATA####

#export raw data#
raw_path = os.path.join(os.getcwd(),"Results", "Raw")
if not os.path.isdir(raw_path):
    os.makedirs(raw_path)

raw_path_write = os.path.join(raw_path, 
                              "immoscout_data_raw_"+current_datetime+".csv")

immoscout_data.to_csv(raw_path_write, sep=";", index=False)#export unprocessed data


#clean data#
def clean_pricesize(data):
    data = data.replace("€", "")
    data = data.replace(".", "")
    data = data.replace("m²", "")
    data = re.sub(re.compile(" \D.*"), "", data)
    data = data.strip()
    return data


def get_firstlayer(data):
    fist_layer = data.split(",")[0]
    return fist_layer.strip()

def get_lastlayer(data):
    last_layer = data.split(",")[-1]
    return last_layer.strip()
    
immoscout_data_clean = immoscout_data.dropna(axis=0)
immoscout_data_clean["price"] = immoscout_data_clean["price"].apply(clean_pricesize)
immoscout_data_clean["size"] = immoscout_data_clean["size"].apply(clean_pricesize)
immoscout_data_clean["location_first"] = immoscout_data_clean["location"].apply(get_firstlayer)
immoscout_data_clean["location_last"] = immoscout_data_clean["location"].apply(get_lastlayer)

immoscout_data_clean["crawled"] = current_datetime_master.strftime("%Y-%m-%d %H:%M:%S")

#export cleaned data#
clean_path = os.path.join(os.getcwd(),"Results", "Clean")
if not os.path.isdir(clean_path):
    os.makedirs(clean_path)
    
clean_path_write = os.path.join(clean_path, 
                              "immoscout_data_clean_"+current_datetime+".csv")

immoscout_data_clean.to_csv(clean_path_write, sep=";", index=False)
