import time
from queue import PriorityQueue
from urllib.parse import urlparse
import requests
import url_normalizer
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup

from ir_hw3.FrontierObject import FrontierObject


class Crawler:
    def __init__(self, visited_links, frontier):
        self.visited_links = visited_links
        self.frontier = frontier
        self.robot_dict = {} # maps url/robots.txt : RobotFileParser
        self.inlinks = {} # maps url: inlinks
        self.outlinks = {} # maps url : outlinks
        self.badlinks = {} #TODO update bad links
        self.parsed_docs = {} # map url: soup-parsed doc

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
            pq.put((100000, s)) # set all scores to negative bc priorityqueue pops lowest score first
        self.frontier = pq
        print(self.frontier.get())

    def parse_doc(self, soup, next_link):
        doc = {}
        doc['docno'] = next_link.link
        doc['text'] = soup.get_text()
        try:
            title = soup.title.string
            doc['head'] = title
        except:
            doc['head'] = ""
        doc['outlinks'] = []
        for link in soup.find_all('a'):
            can_url = url_normalizer.canonicalize(link.get('href'))
            if can_url not in doc['outlinks']: # remove duplicates
                doc['outlinks'].append(can_url)

        return doc

    def update_link_graph(self, parsed_doc):
        url = parsed_doc['docno']
        outlinks = parsed_doc['outlinks']
        unseen_links = []

        for l in outlinks:
            if l not in self.badlinks:
                # add outlink to url's outlink graph
                try:
                    self.outlinks[url].append(l)
                except:
                    self.outlinks[url] = [l]

                # add outlink in inlink graph
                try:
                    self.inlinks[l].append(url)
                except:
                    self.outlinks[l] = [url]

                if l not in self.visited_links:
                    unseen_links.append(l)

        # update parsed doc to cleaned set of outlinks
        parsed_doc['outlinks'] = self.outlinks[url]

        return unseen_links

    def add_to_frontier(self, unseen_links, wave):
        for link in unseen_links:
            frontier_obj = FrontierObject(link, wave)
            frontier_obj.update_score()
            self.frontier.put(frontier_obj.score, frontier_obj)

    def crawl(self):
        rp = RobotFileParser()
        page_count = 0
        last_delay = 0
        last_domain = None
        wave = 0

        while page_count < 40000:
            next_link = self.frontier.get()

            # if the link has not yet been visited , check if allowed to crawl
            if next_link not in self.visited_links:
                allow_crawl = self.allow_crawl(next_link, rp)

                # if allowed to crawl,
                if allow_crawl:
                    # get delay and wait if necesary
                    last_delay = self.get_delay(next_link)

                    # only need to wait if sending request to same domain
                    if last_domain == next_link.domain:
                        time.sleep(last_delay)

                    with requests.get(next_link.link) as opened:
                        cont_type = opened.headers.get('Content-Type', 0)
                        language = opened.headers.get('Content-Language',0)
                        soup = BeautifulSoup(opened.text, 'html.parser')

                        if "text/html" in cont_type and language == 'en':
                            # parse webpage
                            parsed_doc = self.parse_doc(soup, next_link)

                            # update inlink and outlink graphs, and return unseen links to add to frontier
                            # when saving, will be deriving in and out links from self.inlinks / self.outlinks ,
                            # NOT from parsed_doc field
                            unseen_links = self.update_link_graph(parsed_doc)

                            # save html info to later save doc
                            self.parsed_docs[parsed_doc['docno']] = parsed_doc

                            # add unseen links to frontier
                            self.add_to_frontier(unseen_links, wave)

                            # update seen links and counters
                            self.visited_links.append(next_link.link)
                            page_count +=1
                            last_domain = next_link.domain




c = Crawler({}, {})
c.init_frontier()