#!/bin/env python2.7

from lxml import html
from collections import deque
from urllib2 import urlopen, HTTPError
from urlparse import urljoin, urlparse
import ujson as json

def hash_url(url):
    parse = urlparse(url)
    return parse.query

def find_urls(to_visit, already_visited):
    cur_url = to_visit.popleft()
    print "Getting: {}".format(cur_url)
    if hash_url(cur_url) in already_visited:
        print "Skipping..."
        return [], to_visit, already_visited
    already_visited.add(hash_url(cur_url))

    try:
        fd = urlopen(cur_url)
    except HTTPError, e:
        print "Could not retrieve {}: {}".format(cur_url, e)
        return [], to_visit, already_visited

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
        desc = text.getparent().getparent().getprevious().xpath("td/span/text()")[0]
        urls.append({"url" : url, "desc" : desc})


    print "Found expands: ", len(expands)
    for expand in expands:
        try:
            uri = expand.attrib['onclick'].split("'")[1]
        except KeyError:
            print "Could not find 'conclick': ", expand.attrib
            continue
        url = urljoin(cur_url, uri)
        if hash_url(url) not in already_visited:
            to_visit.append(url)

    return urls, to_visit, already_visited


if __name__ == "__main__":
    to_visit = deque(["http://www.gpo.gov/fdsys/browse/collection.action?collectionCode=CREC"])
    urls = []
    already_visited = set()

    counter = 0
    while to_visit:
        cur_urls, to_visit, already_visited = find_urls(to_visit, already_visited)
        urls.extend(cur_urls)
        print "Number of URLS: ", len(urls)
        print "Queue Size: ", len(to_visit)
        print "Seen Size: ", len(already_visited)
        if len(urls) > 1000:
            print "FLUSHING"
            json.dump(urls, open("hearings_text_urls_{:08d}.json".format(counter), "w+"))
            urls = []
            counter += 1
    json.dump(urls, open("hearings_text_urls_{:08d}.json".format(counter), "w+"))
