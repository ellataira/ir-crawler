import os
import re

"""
    regex syntax: 
    '.' operator == matches any character 
    '*' operator == repeat preceding operator (i.e. any char) 0+ times
    '?' operator == repeat preceding char 0 or 1 times 
"""
DOC_REGEX = re.compile("<DOC>.*?</DOC>", re.DOTALL) ## DOTALL allows '.' to equal any character, including \n
DOCNO_REGEX = re.compile("<DOCNO>.*?</DOCNO>")
TEXT_REGEX = re.compile("<TEXT>.*?</TEXT>", re.DOTALL)
INLINKS_REGEX = re.compile("<INLINKS>.*?</INLINKS>")
OUTLINKS_REGEX = re.compile("<OUTLINKS>.*?</OUTLINKS>")

class Indexer:

    def __init__(self, index):
        self.index = index


    def create_index(self):
        index_json = {
            'settings':{
                'number_of_shards':1,
                'number_of_replicas': 1,
                'max_result_window': 1000,
                'analysis':{
                    'analyzer': {
                        'stopped':{
                            'type': 'custom',
                            'tokenizer':'standard'
                        }
                    }
                }
            },
            'mappings': {
                'properties':{
                    'content': {
                        'type': 'text',
                        'fielddata': True,
                        'analyzer': 'stopped',
                        'index_options': 'positions'
                    },
                    'inlinks': {
                        'type': 'text'
                    },
                    'outlinks':{
                        'type': 'text'
                    },
                    'author': {
                        'type': 'text'
                    }
                }
            }
        }

        esclient.indices.create(index = self.index, body=request_body)

    def merge_and_index(self, inlinks, url, outlinks, body, author):

        ret = {
            'id': url,
            'content': body,
            'inlinks': inlinks,
            'outlinks':outlinks,
            'author': author
        }

        if es.exists(index=self.index, id=url):
            # TODO update existing index item to add new in/outlinks and author
            self.update(ret)

        else:
            insert_body = {
                'content': body,
                'inlinks': inlinks,
                'outlinks': outlinks,
                'author': author
            }

        es.index(index= self.index, body=insert_body, id=url)


    """parses an individual file from the collection for documents / info """
    def parse(self, filepath, stemmer, stops):

        with open(filepath, encoding="ISO-8859-1") as opened:

            read_opened = opened.read()
            found_docs = re.findall(DOC_REGEX, read_opened)

            for doc in found_docs:

                found_doc = re.search(DOCNO_REGEX, doc)
                docno = re.sub("(<DOCNO> )|( </DOCNO>)", "", found_doc[0])

                found_text = re.search(TEXT_REGEX, doc)
                text = re.sub("(<TEXT>\n)|(\n</TEXT>)", "", found_text[0])
                text = re.sub("\n", " ", text)

                found_inlinks =  re.search(INLINKS_REGEX, doc)
                inlinks = re.sub("(<INLINKS>)|(</INLINKS>)", "", found_inlinks[0])

                found_outlinks = re.search(OUTLINKS_REGEX, doc)
                outlinks = re.sub("(<OUTLINKS>)|(</OUTLINKS>)", "", found_outlinks[0])

                tokens = word_tokenize(text)
                res = []
                for t in tokens:
                    if t not in stops:
                        res.append(stemmer.stem(t))
                    if t not in VOCAB:
                        VOCAB.append(t)

                text = " ".join(res)

            print("doc index: " + str(id))
            return docno, text, inlinks, outlinks, "Ella"


    """opens file collection and delegates to parse individual files """
    def open_dir(self, es, stemmer, stops, document_folder) :

        entries = os.listdir(document_folder)
        id = 0

        print(id)
        # for every 'ap....' file in the opened directory, parse it for documents
        for entry in entries:
            # parse txt file for all info
            filepath = document_folder + "/" + entry
            docid, body, inlinks, outlinks, author = self.parse(filepath, stemmer, stops)
            # merge and index with elasticsearch
            self.merge_and_index(inlinks, docid, outlinks, body, author)

if __name__ == '__main__':
    indexer = Indexer()
    indexer.open_dir(es, stemmer, stops, document_folder)
