from tornado import ioloop
from tornado.httpclient import AsyncHTTPClient
from tornado import gen
import ujson as json
import progressbar as PB

from itertools import izip
from functools import partial
import random
import os
import time


AsyncHTTPClient.configure(
    "tornado.curl_httpclient.CurlAsyncHTTPClient", max_clients=100)

def chunk_seq(seq, chunk_size):
    for i in xrange(0, len(seq), chunk_size):
        yield seq[i:i+chunk_size]

def url_to_filename(url):
    date = url['url'][37:].split("/")[0].replace("-", "/")
    return "./data/" + date + "/{5:s}_{7:s}.json".format(*url['url'].split("/"))

def data_not_exists(url):
    return not os.path.exists(url_to_filename(url))


@gen.coroutine
def run_experiment():
    http_client = AsyncHTTPClient()
    num_files = len(os.listdir("./urls"))
    for i, url_file in enumerate(os.listdir("./urls")):
        if not url_file.endswith(".json"):
            print "Skilling: ", url_file
            continue
        urls = json.load(open("./urls/" + url_file))
        filtered_urls = filter(data_not_exists, urls)
        random.shuffle(filtered_urls)
        p = PB.ProgressBar(maxval=len(filtered_urls)//10 + 1, widgets=("{} / {}".format(i, num_files), PB.Bar(), PB.ETA())).start()
        for urls_chunk in p(chunk_seq(filtered_urls, 10)):
            try:
                responses = yield [http_client.fetch(url['url']) for url in urls_chunk]
            except:
                print "Failed for some result in: ", urls_chunk
                continue
            for raw, response in izip(urls_chunk, responses):
                url = raw['url']
                data = {"url" : url, "body" : response.body, "desc" : raw['desc']}
                fname = url_to_filename(raw)
                try:
                    os.makedirs(os.path.dirname(fname))
                except OSError:
                    pass
                json.dump(data, open(fname, "w+"))
            time.sleep(.5)

if __name__ == "__main__":
    _ioloop = ioloop.IOLoop.instance()

    start = time.time()
    result = _ioloop.run_sync(run_experiment)
    end = time.time()
    print result, (end - start)
