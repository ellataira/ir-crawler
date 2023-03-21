import os
import re
from elasticsearch7 import Elasticsearch

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

""""
user = elastic
pw = 74QCRsRmX0WpC67mIj0PZfDw
"""

INDEX = 'homework3'
CLOUD_ID = 'homework3:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ3NjJhZDM3NTU4MTY0OWM1ODM3ZTRiYjg5NjI5ZmFiNyQyMWU0ZDM1MDQzNmY0NDA3OGIzZTY0NTMyN2Q0NTUzNg=='
DOCUMENT_FOLDER = '/Users/ellataira/Desktop/is4200/crawling/docs'
class Indexer:

    def __init__(self, index):
        self.index = index


    def create_index(self, es):
        # es.indices.delete(index=INDEX, ignore=[404, 400])

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

        es.indices.create(index=self.index, body=index_json)

    def merge_and_index(self, inlinks, url, outlinks, body, author, es):

        resp = ""
        # TODO is it _id or id
        if es.exists(index=self.index, id=url):
            # TODO update existing index item to add new in/outlinks and author
            # get all existing data under url

            to_update = es.get(index=self.index, id=url)

            old_inlinks = to_update['_source']['inlinks']
            old_outlinks= to_update['_source']['outlinks']
            old_author = to_update['_source']['author']

            if old_author == "Ella":
                pass
            else:
                # update item in index with new data
                # add non-duplicate links
                new_inlinks = self.update(old_inlinks, inlinks)
                new_outlinks = self.update(old_outlinks, outlinks)
                new_author = self.update(old_author, author)

                # TODO what's the difference btw insert_body and ret ... supposed to be updating ret instead?!
                insert_body = {
                    'doc':{
                        'inlinks': new_inlinks,
                        'outlinks': new_outlinks,
                        'author': new_author
                    }
                }
                try:
                    resp = es.update(index=self.index, id=url, body=insert_body)
                    print("merged\n")
                except:
                    pass


        else:
            insert_body = {
                'content': body,
                'inlinks': inlinks,     # joined inlinks with ", "
                'outlinks': outlinks,   # joined outlinks with ", "
                'author': author        # joined authors with ", "
            }
            try:
                resp = es.index(index= self.index, body=insert_body, id=url)
                print("indexed: " + url)
            except:
                pass


        return resp

    # returns ', ' string appended combined links with no duplicates
    def update(self, existing, new):
        old_arr = existing.split(', ')
        new_arr = new.split(', ')
        combined_arr = old_arr
        # remove duplicates
        for n in new_arr:
            if n not in combined_arr:
                combined_arr.append(n)
        arr_to_str = ', '.join(combined_arr)
        return arr_to_str


    """parses an individual file from the collection for documents / info """
    def parse(self, filepath):

        with open(filepath, encoding="ISO-8859-1") as opened:

            read_opened = opened.read()
            found_docs = re.findall(DOC_REGEX, read_opened)

            for doc in found_docs:

                found_doc = re.search(DOCNO_REGEX, doc)
                docno = re.sub("(<DOCNO>)|(</DOCNO>)", "", found_doc[0])

                found_text = re.search(TEXT_REGEX, doc)
                text = re.sub("(<TEXT>\n)|(\n</TEXT>)|(<TEXT>)|(</TEXT>)", "", found_text[0])
                text = re.sub("\n", " ", text)

                found_inlinks =  re.search(INLINKS_REGEX, doc)
                inlinks = re.sub("(<INLINKS>)|(</INLINKS>)", "", found_inlinks[0])

                found_outlinks = re.search(OUTLINKS_REGEX, doc)
                outlinks = re.sub("(<OUTLINKS>)|(</OUTLINKS>)", "", found_outlinks[0])

            return docno, text, inlinks, outlinks, "Ella"


    """opens file collection and delegates to parse individual files """
    def open_dir_and_merge_index(self, es, document_folder) :

        entries = os.listdir(document_folder)

        # for every 'ap....' file in the opened directory, parse it for documents
        for entry in entries:
            # parse txt file for all info
            filepath = document_folder + "/" + entry
            docid, body, inlinks, outlinks, author = self.parse(filepath)
            # merge and index with elasticsearch
            self.merge_and_index(inlinks, docid, outlinks, body, author, es)


if __name__ == '__main__':
    es = Elasticsearch(request_timeout = 1000, cloud_id = CLOUD_ID, http_auth= ('elastic', '74QCRsRmX0WpC67mIj0PZfDw'))
    indexer = Indexer(INDEX)
    # init index
    # indexer.create_index(es)
    indexer.open_dir_and_merge_index(es, DOCUMENT_FOLDER)
