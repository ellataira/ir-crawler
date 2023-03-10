from url_normalize import url_normalize
from urllib.parse import urljoin, urlparse


def canonicalize(url, base_url=None):
    # TODO : dependent on how relative urls are written
    if base_url:
        url = urljoin(base_url, url)

    # convert to lowercase
    # remove ports
    # remove duplicate slashes
    ret_url = url_normalize(url)

    # replace https with http (port 443)
    ret_url.replace("https", "http")

    # remove fragments beginning with #
    find_hash = ret_url.find("#")
    if find_hash != -1:
        ret_url = ret_url[:find_hash]

    # remove ending / if exists
    if ret_url[len(ret_url) - 1] == "/":
        ret_url = ret_url[:len(ret_url) - 1]

    return ret_url



assert canonicalize("dogs.html", "http://www.google.com") == "http://www.google.com/dogs.html"
assert canonicalize("HTTP://www.Example.com/SomeFile.html") == "http://www.example.com/SomeFile.html"
assert canonicalize("http://www.example.com:80") == "http://www.example.com"
assert canonicalize("http://www.example.com:80/") == "http://www.example.com"
assert canonicalize("http://www.example.com/a.html#anything") == "http://www.example.com/a.html"
assert canonicalize("http://www.example.com//a.html") == "http://www.example.com/a.html"
