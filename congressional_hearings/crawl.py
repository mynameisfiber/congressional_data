#!/bin/env python2.7

from lxml import html
from urllib2 import urlopen
from urlparse import urljoin
import ujson as json

def find_urls(cur_url, already_visited=None):
    print "Getting: ", cur_url
    already_visited = already_visited or set()
    fd = urlopen(cur_url)
    data = fd.read()
    fd.close()
    dom = html.fromstring(data)
    expands = dom.xpath(".//div[contains(@class,'browse-')]/a")

    urls = []
    texts = dom.xpath(".//td[@class = 'browse-download-links']/a[contains(text(),'Text')]")
    print "Found texts: ", len(texts)
    for text in texts:
        uri = text.attrib['href']
        url = urljoin(cur_url, uri)
        urls.append(url)


    new_directories = []
    print "Found expands: ", len(expands)
    for expand in expands:
        uri = expand.attrib['onclick'].split("'")[1]
        url = urljoin(cur_url, uri)
        if url not in already_visited:
            print "Recursing on: ", url
            new_directories.append(url)
            already_visited.add(url)
            urls.extend(find_urls(url, already_visited))
        else:
            print "Already crawled: ", url

    return urls


if __name__ == "__main__":
    start_url = "http://www.gpo.gov/fdsys/browse/collection.action?collectionCode=CHRG"
    hearings_text_urls = find_urls(start_url)
    json.dump(hearings_text_urls, open("hearings_text_urls.json", "w+"))
