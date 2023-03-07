import time
from queue import PriorityQueue
from urllib.parse import urlparse
import requests
import url_normalizer
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup


class Crawler:
    def __init__(self, visited_links_dict, frontier):
        self.visited_links_dict = visited_links_dict
        self.frontier = frontier
        self.robot_dict = {}

    # read robots.txt to see if allowed to crawl
    def allow_crawl(self, link, rp):
        parsed = urlparse(link)
        url = parsed.scheme + "://" + parsed.netloc + "/robots.txt"
        try:
            if url not in self.robot_dict:
                rp.set_url(url)
                rp.read()
                self.robot_dict[url] = rp
            cc = self.robot_dict[url].can_fetch("*")
        except:
            cc = True
        return cc

    def get_delay(self, url):
        rp = self.robot_dict[url]
        d= rp.crawl_delay("*")
        if d == None:
            return 1
        else:
            return d

    def init_frontier(self):
        seeds = ["https://en.wikipedia.org/wiki/Social_justice", "https://en.wikipedia.org/wiki/Women%27s_rights",
                 "https://www.unwomen.org/en/what-we-do/ending-violence-against-women",
                 "https://www.amnesty.org/en/latest/campaigns/2018/03/8-women-who-are-changing-the-world/"]

        pq = PriorityQueue()
        for i, s in enumerate(seeds):
            pq.put((-i, s)) # set all scores to negative bc priorityqueue pops lowest score first
        self.frontier = pq
        print(self.frontier.get())


    def crawl(self):
        rp = RobotFileParser()
        page_count = 0
        last_delay = 0
        last_domain = None

        while page_count < 40000:
            next_link = self.frontier.get()

            # if the link has not yet been visited , check if allowed to crawl
            if next_link not in self.visited_links_dict:
                allow_crawl = self.allow_crawl(next_link, rp)

                # if allowed to crawl,
                if allow_crawl:
                    # get delay and wait if necesary
                    last_delay = self.get_delay(next_link)

                    # only need to wait if sending request to same domain
                    if last_domain == next_link.domain:
                        time.sleep(last_delay)

                    with requests.get(next_link.link) as opened:
                        type = opened.headers.get('Content-Type', 0)
                        language = opened.headers.get('Content-Language',0)
                        soup = BeautifulSoup(opened.text, 'html.parser')

                        if "text/html" in type and language == 'en':
                            # save doc
                            # update in in and out links



c = Crawler({}, {})
c.init_frontier()