import re
import string
import time
from socket import socket
from urllib.parse import urlparse
import requests
import url_normalizer
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
from Utils import Utils
from PriorityQueue import PriorityQueue
from FrontierObject import FrontierObject
from ir_hw3.Document import Document


class Crawler:
    def __init__(self, visited_links=[], frontier=None):
        self.visited_links = visited_links
        self.frontier = frontier # custom PriorityQueue data structure based on queue.PriorityQueue: pops lowest score first
        self.robot_dict = {} # maps url/robots.txt : RobotFileParser
        self.inlinks = {} # maps url: inlinks
        self.outlinks = {} # maps url : outlinks
        self.badlinks = ['collectionscanada.gc.ca', 'portal.unesco.org', 'www.passports.gov.au', 'www.youtube.com', 'www.instagram.com', 'www.twitter.com',
                         'www.google.com', 'www.facebook.com', 'www.tiktok.com', 'www.pinterest.com'] #TODO update bad links
        self.parsed_docs = {} # map url: soup-parsed doc
        self.PAGECOUNT = 1000

    # read robots.txt to see if allowed to crawl
    def read_robots_txt(self, link):
        print('reading robots.txt')
        parsed = urlparse(link)
        robot_url = parsed.scheme + "://" + parsed.netloc + "/robots.txt"
        rp = RobotFileParser()

        try:
            rp.set_url(robot_url)
            rp.read()
            cc = rp.can_fetch('*', link)
            print("fetched can-crawl")
        except:
            cc = False

        d = rp.crawl_delay('*')
        print("fetched crawl-delay")
        if d == None:
            d = 1

        return (cc, d)


    def init_frontier(self):
        seeds = ["https://en.wikipedia.org/wiki/Social_justice", "https://en.wikipedia.org/wiki/Women%27s_rights",
                 "https://www.unwomen.org/en/what-we-do/ending-violence-against-women",
                 "https://www.aclu.org/issues/womens-rights"]

        pq = PriorityQueue()
        for i, s_url in enumerate(seeds):
            s_url = url_normalizer.canonicalize(s_url)
            new_obj = FrontierObject(s_url)
            pq.enqueue(new_obj.score, new_obj)  # set all scores to negative bc priorityqueue pops lowest score first
            self.inlinks[s_url] = {}
            self.outlinks[s_url] = {}
        self.frontier = pq

    def parse_doc(self, soup, next_link, req_opened, page_count):
        doc = Document(soup, next_link, req_opened, page_count)

        unfiltered_outlinks = []
        for link in soup.find_all('a'):
            try:
                can_url = url_normalizer.canonicalize(link.get('href'), next_link)
                if can_url not in unfiltered_outlinks: # remove duplicates
                    unfiltered_outlinks.append(can_url)
            except:
                pass

        return doc, unfiltered_outlinks


    def update_link_graph(self, parsed_doc, unfiltered_outlinks):
        print('updating link graph')
        url = parsed_doc.docno
        unseen_links = []
        print('url: ' + url)

        for l in unfiltered_outlinks:
            parsed = urlparse(l)
            if parsed.netloc not in self.badlinks and l != 'https://':
                print('link: ' + l)
                # add outlink to url's outlink graph
                try:
                    self.outlinks[url].append(l)
                except:
                    self.outlinks[url] = [l]

                # add outlink in inlink graph
                try:
                    self.inlinks[l].append(url)
                except:
                    self.inlinks[l] = [url]

                if l not in self.visited_links:
                    unseen_links.append(l)

        return unseen_links

    def add_to_frontier(self, unseen_links, wave):
        for link in unseen_links:
            frontier_obj = FrontierObject(link, wave, self.inlinks[link])
            frontier_obj.update_score()
            self.frontier.enqueue(frontier_obj.score, frontier_obj)


    def refresh_frontier(self):
        new_frontier = PriorityQueue()
        num_to_update = 1000
        s = 0
        for score, url in self.frontier.get_p_queue(): #tuples (score, frontierObject)
            fo = self.frontier.get_frontier_object(url)
            if s < num_to_update: # only update the top 1000 documents (which prioritizes the most recent wave / most relevant docs
                fo.update_inlinks(self.inlinks[fo.link])
                # no need to update outlinks bc these docs havent been parsed yet => no outlinks found
                fo.update_score()
                s += 1
            new_frontier.enqueue(fo.score, fo)
        self.frontier = new_frontier


    def save_docs(self):
        for url, doc in self.parsed_docs.items():
            with open("/Users/ellataira/Desktop/is4200/crawling/docs/no_" + str(doc.doc_idx) + ".txt", "w") as file:
                file.write('<DOC>\n')
                file.write("<DOCNO>{}</DOCNO>\n".format(doc.docno))
                if doc.head != "":
                    file.write("<HEAD>{}</HEAD>\n".format(doc.head))
                file.write("<HEADERS>{}</HEADERS>\n".format(doc.headers))
                text = doc.text
                re.sub('\n', " ", text)
                re.sub('\t', " ", text) # TODO stripping excess \n ???
                file.write("<TEXT>{}</TEXT>\n".format(text))
                file.write("<RAW_HTML>{}</RAW_HTML>\n".format(doc.raw_html))
                inlinks_to_str = ", ".join(self.inlinks[doc.docno])
                file.write("<INLINKS>{}</INLINKS>\n".format(inlinks_to_str))
                try: # some links may not yet be a key in self.outlinks if the crawling terminates before that doc is popped from frontier
                    outlinks_to_str = ", ".join(self.outlinks[doc.docno])
                except:
                    outlinks_to_str = ""
                file.write("<OUTLINKS>{}</OUTLINKS>\n".format(outlinks_to_str))
                file.write('</DOC>\n')
                file.close()

    def save_dicts(self, page_count):
        utils = Utils()
        base_filepath = "/Users/ellataira/Desktop/is4200/crawling/dict_backup/"
        utils.save_dict(base_filepath + "visited_links_at_" + str(page_count) + "_pages.pkl", self.visited_links)
        utils.save_dict(base_filepath + "frontier_at_"+ str(page_count)+ "_pages.pkl" , self.frontier.frontier_obj_dict)
        utils.save_dict(base_filepath + "inlinks_at_" + str(page_count) + "_pages.pkl", self.inlinks)
        utils.save_dict(base_filepath + "outlinks_at_" + str(page_count) + "_pages.pkl", self.outlinks)
        # utils.save_dict(base_filepath + "parsed_docs_at_" + str(page_count) + "_pages.pkl", self.parsed_docs) TODO cant save bc max recusrion depth


    def crawl(self):
        rp = RobotFileParser()
        page_count = 0
        last_domain = None

        while page_count < self.PAGECOUNT and not self.frontier.is_empty():
            score, next_frontier_obj = self.frontier.get()
            next_link = next_frontier_obj.link

            print(next_link)
            print("wave: " + str(next_frontier_obj.wave_no) )
            print("page count: " + str(page_count))

            try:
                # if the link has not yet been visited , check if allowed to crawl
                if next_link not in self.visited_links:
                    print("checking if crawl allowed / delays")
                    allow_crawl, delay = self.read_robots_txt(next_link)
                    print("can crawl = "  + str(allow_crawl))

                    # if allowed to crawl,
                    if allow_crawl:
                        # only need to wait if sending request to same domain
                        if last_domain == next_frontier_obj.domain:
                            time.sleep(delay)

                        try:
                            with requests.get(next_link, timeout=100) as opened:
                                soup = BeautifulSoup(opened.text, 'html.parser')
                                cont_type = opened.headers.get('Content-Type')
                                language = opened.headers.get('Content-Language')

                                try:
                                    lang_soup = True if 'en' in soup.html.get('lang', '') else False
                                except:
                                    lang_soup = False

                                try:
                                    is_eng = True if 'en' in language else False
                                except:
                                    # language == None
                                    is_eng = False

                                is_eng = is_eng or lang_soup

                                print("language = eng == " + str(is_eng))
                                print("content type = " + str(cont_type))

                                if "text/html" in cont_type and is_eng:
                                    print("OK to parse!")
                                    # parse webpage
                                    parsed_doc, unfiltered_outlinks = self.parse_doc(soup, next_link, opened, page_count)
                                    print("parsed doc: " + str(next_link))

                                    # update inlink and outlink graphs, and return unseen links to add to frontier
                                    # when saving, will be deriving in and out links from self.inlinks / self.outlinks ,
                                    # NOT from parsed_doc field
                                    unseen_links = self.update_link_graph(parsed_doc, unfiltered_outlinks)

                                    # save html info to later save doc
                                    self.parsed_docs[parsed_doc.docno] = parsed_doc

                                    # add unseen links to frontier
                                    wave = next_frontier_obj.wave_no + 1
                                    self.add_to_frontier(unseen_links, wave)
                                    print("added new links to frontier")

                                    # update seen links and counters
                                    self.visited_links.append(next_link)
                                    page_count +=1
                                    last_domain = next_frontier_obj.domain
                        except requests.exceptions.Timeout:
                            continue


                if page_count in [100, 500, 2500, 12500, 30000]:
                    self.refresh_frontier()
                    self.save_docs()
                    self.save_dicts(page_count)
                    print("refreshed and saved at " + str(page_count))

            except: # in case of crash
                print('crashed : (')
                self.save_docs()
                self.save_dicts(page_count)

        self.save_docs()
        self.save_dicts(page_count)
        print("exited while loop and saved")
        print("terminating crawl")


if __name__ == "__main__":
    crawler = Crawler()
    crawler.init_frontier()
    print("init frontier")
    crawler.crawl()

"""
- is the sleep() being applied correctly ? i think so ... 
- how to filter out bad links 
    - possible to timeout somewhere? 
- bad links (insecure connections, 404, etc) 
    => requests.exceptions.ConnectionError
    => infinite attempt on url (gets stuck) but never times out? weird ... 
    
- not parsing all docs it should ... detecting english / html correctly ? 
- how to strip excess whitespace when saving to doc .txt 
"""