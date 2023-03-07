from urllib.parse import urlparse
class Webpage:
    def __init__(self, link, wave_no, score):
        self.link = link
        self.inlinks = []
        self.outlinks = []
        self.wave_no = wave_no
        self.score = score
        self.domain = urlparse(link).netloc

    # TODO calc_score
    def calc_score(self):
        self.score = 0