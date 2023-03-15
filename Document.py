from ir_hw3 import url_normalizer


class Document:

    def __init__(self, soup, next_link, req_opened, doc_idx):
        self.doc_idx = doc_idx
        self.docno = next_link
        self.headers = req_opened.headers
        self.text = ""
        ps = soup.find_all('p')
        for p in ps:
            self.text += p.get_text()
        self.raw_html = soup
        try:
            self.head = soup.title.string
        except:
            self.head = ""
