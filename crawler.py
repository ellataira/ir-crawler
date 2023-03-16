import queue
import time
from urllib.parse import urlparse
import requests
import url_normalizer
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
from Utils import Utils
from Frontier import PriorityQueue, Frontier
from FrontierObject import FrontierObject
from Document import Document


class Crawler:
    def __init__(self, visited_links=[], frontier=None):
        self.visited_links = visited_links
        self.frontier = frontier # custom PriorityQueue data structure based on queue.PriorityQueue: pops lowest score first
        self.inlinks = {} # maps url: inlinks
        self.outlinks = {} # maps url : outlinks
        self.badlinks = ['collectionscanada.gc.ca', 'portal.unesco.org', 'www.passports.gov.au', 'www.youtube.com', 'www.instagram.com', 'www.twitter.com',
                         'www.google.com', 'www.facebook.com', 'www.tiktok.com', 'www.pinterest.com', 'www.anh-usa.org', 'www.cynthiawerner.com'] #TODO update bad links
        self.PAGECOUNT = 40000

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

        f = Frontier()
        f.add_wave(0)
        wave0 = f.get_wave(0)

        for i, s_url in enumerate(seeds):
            s_url = url_normalizer.canonicalize(s_url)
            new_obj = FrontierObject(s_url)
            wave0.enqueue(new_obj.score, new_obj)  # set all scores to negative bc priorityqueue pops lowest score first
            self.inlinks[s_url] = {}
            self.outlinks[s_url] = {}

        self.frontier = f

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
        # print('updating link graph')
        url = parsed_doc.docno
        unseen_links = []
        # print('url: ' + url)

        for l in unfiltered_outlinks:
            parsed = urlparse(l)
            if parsed.netloc not in self.badlinks and l != 'https://':
                # print('link: ' + l)
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
            wave_frontier = self.frontier.get_wave(wave)
            wave_frontier.enqueue(frontier_obj.score, frontier_obj)


    def refresh_frontier(self, wave_no):
        num_to_update = 250
        s = 0

        wave_frontier = self.frontier.get_wave(wave_no)
        wave_q = wave_frontier.get_queue()

        for score, url in wave_q: #tuples (score, frontierObject)
            if s < num_to_update:
                fo = wave_frontier.get()[1] # pops highest priority item from original queue (deleting it)
                fo.update_inlinks(self.inlinks[fo.link])
                # no need to update outlinks bc these docs havent been parsed yet => no outlinks found
                fo.update_score()
                wave_frontier.enqueue(fo.score, fo)
                s += 1
            else:
                break

    def save_doc(self, doc):
        with open("/Users/ellataira/Desktop/is4200/crawling/docs_test/no_" + str(doc.doc_idx) + ".txt", "w") as file:
            file.write('<DOC>\n')
            file.write("<DOCNO>{}</DOCNO>\n".format(doc.docno))
            if doc.head != "":
                file.write("<HEAD>{}</HEAD>\n".format(doc.head))
            file.write("<HEADERS>{}</HEADERS>\n".format(doc.headers))
            text = doc.text
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
        for waveno, wave in self.frontier.waves.items():
            utils.save_dict(base_filepath + "frontier_at_"+ str(page_count)+ "_wave_" + str(waveno) + "_pages.pkl" , wave.frontier_obj_dict)
        utils.save_dict(base_filepath + "inlinks_at_" + str(page_count) + "_pages.pkl", self.inlinks)
        utils.save_dict(base_filepath + "outlinks_at_" + str(page_count) + "_pages.pkl", self.outlinks)
        # utils.save_dict(base_filepath + "parsed_docs_at_" + str(page_count) + "_pages.pkl", self.parsed_docs) TODO cant save bc max recusrion depth


    def crawl(self, page_count=0, wave=0):
        last_domain = None
        already_saved = False

        while page_count < self.PAGECOUNT and not self.frontier.is_empty():
            # explore frontier by wave
            wave_frontier = self.frontier.get_wave(wave)

            # if current wave is empty, increment to next wave
            if not wave_frontier.is_empty():
                score, next_frontier_obj = wave_frontier.get()
                next_link = next_frontier_obj.link

                print(next_link)
                print("wave: " + str(next_frontier_obj.wave_no) )
                print("page count: " + str(page_count))

                # if the link has not yet been visited , check if allowed to crawl
                if next_link not in self.visited_links:
                    # print("checking if crawl allowed / delays")
                    allow_crawl, delay = self.read_robots_txt(next_link)
                    # print("can crawl = "  + str(allow_crawl))

                    # if allowed to crawl,
                    if allow_crawl:
                        # only need to wait if sending request to same domain
                        if last_domain == next_frontier_obj.domain:
                            time.sleep(delay)

                        try:
                            with requests.get(next_link, timeout=10) as opened:
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

                                # print("language = eng == " + str(is_eng))
                                # print("content type = " + str(cont_type))

                                if "text/html" in cont_type and is_eng:
                                    print("OK to parse!")
                                    # parse webpage
                                    parsed_doc, unfiltered_outlinks = self.parse_doc(soup, next_link, opened, page_count)
                                    print("parsed doc: " + str(next_link))

                                    # update inlink and outlink graphs, and return unseen links to add to frontier
                                    # when saving, will be deriving in and out links from self.inlinks / self.outlinks ,
                                    # NOT from parsed_doc field
                                    unseen_links = self.update_link_graph(parsed_doc, unfiltered_outlinks)

                                    # save doc TODO why not just save the doc right away? that's what lecture did
                                    self.save_doc(parsed_doc)

                                    # add unseen links to frontier
                                    next_wave = next_frontier_obj.wave_no + 1
                                    self.add_to_frontier(unseen_links, next_wave)
                                    # print("added new links to frontier")

                                    # update seen links and counters
                                    self.visited_links.append(next_link)
                                    page_count += 1
                                    last_domain = next_frontier_obj.domain
                        except :
                            continue


                if page_count % 1000 == 0 and page_count > 0 and not already_saved:
                    already_saved = True # so it doesn't keep saving while looking for 1001 document , etc
                    self.refresh_frontier(wave)
                    self.save_dicts(page_count)
                    print("refreshed and saved at " + str(page_count))

                if page_count % 1000 == 1:
                    already_saved = False # reset saved flag

            else:
                self.frontier.remove_empty_wave(wave)
                wave += 1

        self.save_dicts(page_count)
        print("exited while loop and saved")
        print("terminating crawl")

    def restore(self, visited_links, frontiers, inlinks, outlinks):
        """
        self.visited_links = visited_links
        self.frontier = frontier # custom PriorityQueue data structure based on queue.PriorityQueue: pops lowest score first
        self.inlinks = {} # maps url: inlinks
        self.outlinks = {} # maps url : outlinks
        """
        utils = Utils()
        # new_frontier = Frontier()
        # for f in frontiers:
        #     wave_frontier = utils.read_pickle(f)
        #     terms = f.split("_")
        #     wave_no = terms[4]
        #     new_frontier.restore_frontier(wave_frontier, wave_no)
        # print('restored frontier')
        new_visited_links = utils.read_pickle(visited_links)
        print('restored visited links')
        new_inlinks = utils.read_pickle(inlinks)
        print('restored inlinks')
        new_outlinks = utils.read_pickle(outlinks)
        print('restored outlinks')

        # self.frontier = new_frontier
        self.visited_links = new_visited_links
        self.inlinks = new_inlinks
        self.outlinks = new_outlinks

def restored_crawl(visited_links, frontier, inlinks, outlinks, page_count, wave_no):
    crawler = Crawler()
    crawler.restore(visited_links, frontier, inlinks, outlinks)

    utils = Utils()
    fs = ["/Users/ellataira/Desktop/is4200/crawling/dict_backup/frontier_at_19000_wave_2_pages.pkl",
          "/Users/ellataira/Desktop/is4200/crawling/dict_backup/frontier_at_19000_wave_3_pages.pkl"]
    nf = Frontier()
    i = 2
    for f in fs:
        w2 = utils.read_pickle(f)
        nf.restore_frontier(w2, i)
        w = nf.waves
        pq = nf.get_wave(i)
        q = pq.get_queue()
        # print(q)
        i += 1

    crawler.frontier = nf

    print('ready to crawl!')
    crawler.crawl(page_count, wave_no)

def regular_crawl():
    crawler = Crawler()
    crawler.init_frontier()
    print("init frontier")
    crawler.crawl()

if __name__ == "__main__":
    # regular_crawl()
    frontier = ["/Users/ellataira/Desktop/is4200/crawling/dict_backup/frontier_at_12000_wave_2_pages.pkl",
                "/Users/ellataira/Desktop/is4200/crawling/dict_backup/frontier_at_12000_wave_3_pages.pkl"]
    seen_links = "/Users/ellataira/Desktop/is4200/crawling/dict_backup/visited_links_at_19000_pages.pkl"
    inlinks = "/Users/ellataira/Desktop/is4200/crawling/dict_backup/inlinks_at_12000_pages.pkl"
    outlinks = "/Users/ellataira/Desktop/is4200/crawling/dict_backup/outlinks_at_12000_pages.pkl"
    restored_crawl(seen_links, frontier, inlinks, outlinks, 1200, 2)

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