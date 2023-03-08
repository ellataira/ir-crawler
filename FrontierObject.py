from urllib.parse import urlparse
class FrontierObject:
    def __init__(self, link, wave_no, score=0):
        self.link = link
        self.inlinks = []
        self.outlinks = []
        self.wave_no = wave_no
        self.score = score
        self.domain = urlparse(link).netloc

    # TODO update score
    def update_score(self):
        self.score = 0