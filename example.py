
import requests
import pipelined


def extract(iter):
    for line in iter:
        print("EXT", line)
        yield line


def output(chunks):
    for c in chunks:
        print("CHUNK:", c)
        yield sum(c)


def get_pages(urls):
    import time

    urls = list(urls)
    for url in urls:
        print("PROCESSING URL", url)
        r = requests.get(url)
        time.sleep(1)

        #yield " ".join('href="%s"' % s for s in urls)
        yield url, r.text


def find_links(pages):
    import re
    #mem = set()

    link_re = re.compile(r'href=[\'"]?([^\'" >]+)')
    for base_url, page in pages:
        for link in link_re.findall(page):
            #if link not in mem:
            yield base_url, link
            #    mem.add(link)


def save_links(chunks):

    i = 0
    for links in chunks:
        print("GOT LINKS", links)
        i += len(links)
        #yield "ok"
    return i


def log_links(links):

    print("DDDDUPA")
    for link in links:
        print("DEBUG:", link)
        yield link


def unique_links(stream):
    mem = set()
    for link in stream:
        if link not in mem:
            yield link
            mem.add(link)


def find_images(pages):
    import re
    for base_url, page in pages:
        img_re = re.compile(r'src=[\'"]?([^\'" >]+)')
        for img in img_re.findall(page):
            yield base_url, img


def fake_get_pages(url_stream):

    with open('test.html') as f:
        yield 'http://youtube.com', f.read()


def process_page(pages):

    for page in pages:
        pipelined.run(link_process, [page])
        pipelined.run(image_process, [page])


def fix_urls(stream):
    print(stream.context)

    import collections
    c = collections.Counter()

    for base_url, url in stream:
        if url.startswith('//'):
            url = 'http:' + url
        elif url.startswith('/'):
            url = base_url + url
        elif url.startswith('\\'):
            continue

        c[1] += 1
        if c[1] == 5:
            raise "DUPA"
        yield url

fix_and_unique_urls = [
    fix_urls,
    unique_links,
]

link_process = [
    find_links,
    fix_and_unique_urls,
]

image_process = [
    find_images,
    fix_and_unique_urls,
]

web_process = [
    #pipelined.from_input('page_fetcher'),
    fake_get_pages,
    pipelined.tee(link_process, image_process),
    pipelined.chunked(1),
    save_links,
]

source = [
    'http://onet.pl',
    'http://google.pl',
]
source = range(12)

#p = pipelined.Pipeline(web_process)
#p.feed(source)
#for r in p.run():
#    print(r)

#r = pipelined.run(web_process, source)  #, page_fetcher=fake_get_pages)
#print("FINISHED", r)

def process_numbers(stream):
    for num in stream:
        if num == 2:
            1/0
        yield num

exc_test = [
    process_numbers,
    sum,
]

r = pipelined.run(exc_test, range(10))
print(r)
