from concurrent.futures.thread import BrokenThreadPool
from os import write
from pymongo import collection
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from requests_html import HTMLSession
import concurrent.futures
from requests.models import Response
import json
import pprint
import time
import pymongo

def dbConnect():
    db_connect = False
    try:
        cluster = pymongo.MongoClient("mongodb+srv://minibems:<minibems>@url.i2ey5.mongodb.net/URL?retryWrites=true&w=majority")
        db = cluster["url"]
        collection = db["url"]
        db_connect = True
        print("[INFO] Successfully connected to database ")
    except Exception as e:
        print('[INFO] Failed to connect to database')
    return db_connect

def is_valid(url):
    '''
    checks if url is valid by verifying if protocol and domain name exists in the url
    '''
    parsed_url = urlparse(url)
    return bool(parsed_url.netloc) and bool(parsed_url.scheme)

global list1
list1 = []
def extract_url(url, debug=False, test=False, soup_test=None):
    
    # raise exception if we get a non-string input
    if type(url) != str:
        raise Exception(f"url is of type {type(url)}, please input a valid url as a string")

    # connect to MongoDB database which stores data (this will be most commonly used urls but I have yet to implement this)
    db_connect = dbConnect()
    if db_connect:
        try:
            # the structure of entried of the database is {_id : url, urls: set of urls accessible from parent url}
            result = collection.find({"_id" : url})
            if result:
                # append to list which keeps track of all urls
                list1.append({url: result["urls"]})
                return
        except:
            if debug:
                print(f"[INFO] {url} not present in database")
            else:
                pass
    
    internal_urls = set()
    if debug:
        external_urls = set()
        broken_urls = set()

    # domain name without protocol
    domain_name = urlparse(url).netloc
    response = requests.get(url)

    # render javascript with requests_html module
    try:
        response.html.render()
    except:
        pass

    if test:
        soup = soup_test
    else: soup = BeautifulSoup(requests.get(url).content, "html.parser")
    
    # Look for all urls in HTML a tags (note all urls are in HTML a tags)
    for a_tag in soup.findAll("a"):

        href = a_tag.attrs.get("href")

        if not href or href == "":
            continue

        # not all links are absolute, therefore we need to join relative urls with their domain name
        href = urljoin(url, href)
        parsed_href = urlparse(href)
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path + "/"
    
        if not is_valid(url):
            if debug:
                print(f"[INFO] Broken link : {href} ... ")
                broken_urls.add(url)
            continue

        # Prevents double counts
        if href in internal_urls:
            continue

        # Typically a landing page will have a link to itself. This code snippet allows us to ignore this
        if href == url:
            continue

        if domain_name not in href:
            if debug and href not in external_urls:
                print(f"[INFO] External link : {href} ... ")
                external_urls.add(href)
            continue
        
        if domain_name in href:
            if debug: print(f"[INFO] Internal link : {href} ... ")
            internal_urls.add(href)
            if db_connect:
                collection.insert_one({"_id": href})

    if debug:
        print('[INFO] External url list: ')
        for external_url in external_urls:
            print(external_url)
        print('[INFO] Broken url list: ')
        for broken_url in broken_urls:
            print(broken_url)

    list1.append({url:internal_urls})
    return

if __name__ == "__main__":

    import argparse
    ap = argparse.ArgumentParser(description='parent url')
    ap.add_argument("-u", "--url", help='parent url', default="https://www.minibems.com/")
    ap.add_argument("-d", "--debug", help='debug status', default=True)
    ap.add_argument("-f", "--file", help="print to file", default=True)
    args = ap.parse_args()
    parent_url, debug, write_file = args.url, args.debug, args.file

    dict1 = extract_url(parent_url)
    
    # keep track of time to perform function
    t0 = time.time()

    # multithreading since the function is I/O bound
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(extract_url, dict1[parent_url])
    

    domain_name = urlparse(parent_url).netloc
    if write_file:
        with open(f"{domain_name}_links.txt", "w") as f:
            for elem in list1:
                print(elem, file=f)

    if debug:
        for elem in list1:
            pprint.pprint(elem)

    # release list1
    del list1

    t1 = time.time()
    if debug:
        print(f"[INFO] Ran in {t1-t0} seconds...")


