import re
from urllib.parse import urlparse

# represents a single item in the frontier
class FrontierObject:
    def __init__(self, link, wave_no=0, inlinks={}, outlinks={}, score=0):
        self.link = link
        self.inlinks = inlinks
        self.outlinks = outlinks
        self.wave_no = wave_no
        self.score = score
        self.domain = urlparse(link).netloc

    # updates score of a webpage based on inlinks, keywords, and wave number
    def update_score(self): # priority queue will select smallest score first
        key_words = ['justice', 'women', 'right', 'femini', 'gender', 'discriminat', 'girl',
                     'sexual', 'violenc', 'harrassment', 'suffrage', 'equity', 'sex', 'abortion', 'pregnancy',
                     'autonom', 'female']
        score = len(self.inlinks) * -10 # prefer pages with more inlinks

        found_keywords = []
        for k in key_words: # prefer urls with keywords
            found = re.findall(k, self.link)
            found_keywords.append(found)

        score += len(found_keywords) * -10
        self.score = score

    # updates stored inlinks
    def update_inlinks(self, inlinks):
        self.inlinks = inlinks

    # updates stored outlinks
    def update_outlinks(self, outlinks):
        self.outlinks = outlinks