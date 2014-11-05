from tornado import ioloop
from tornado.httpclient import AsyncHTTPClient
from tornado import gen
import ujson as json
import progressbar as PB

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
    return "./data/{5:s}_{7:s}.json".format(*url.split("/"))

def data_not_exists(url):
    return not os.path.exists(url_to_filename(url))


@gen.coroutine
def run_experiment(urls):
    http_client = AsyncHTTPClient()
    filtered_urls = filter(data_not_exists, urls)
    random.shuffle(filtered_urls)
    p = PB.ProgressBar(maxval=len(filtered_urls)//10 + 1, widgets=(PB.Percentage(), PB.Bar(), PB.ETA())).start()
    for urls_chunk in p(chunk_seq(filtered_urls, 10)):
        try:
            responses = yield [http_client.fetch(url) for url in urls_chunk]
        except:
            print "Failed for some result in: ", urls_chunk
            continue
        print "Got a bunch of results!"
        for response in responses:
            url = response.request.url
            data = {"url" : url, "body" : response.body}
            fname = url_to_filename(url)
            print fname
            json.dump(data, open(fname, "w+"))
        time.sleep(.5)

if __name__ == "__main__":
    urls = json.load(open("hearings_text_urls.json"))
    _ioloop = ioloop.IOLoop.instance()
    run_func = partial(
        run_experiment,
        urls,
    )

    import time
    start = time.time()
    result = _ioloop.run_sync(run_func)
    end = time.time()
    print result, (end - start)
