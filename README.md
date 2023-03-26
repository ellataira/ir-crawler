# Topical Crawler

For this project, I implemented a web crawler to construct a document collection focused on a particular topic. The 40,000 crawled documents and their relevant data, including inlinks and outlinks, were saved locally as individual text files and then merged into a group index using Elasticsearch. 

## Crawling
`crawler.py`, `Frontier.py`, `FrontierObject.py`, `Document.py`

Starting from a set of seed URLs, the crawler will parse each documents with a modified breadth-first search. Only English, HTML documents will be saved. 

The `Frontier` of pages to be crawled is a dictionary of `PriorityQueue`s, where each page's priority score is calculated using inlinks and presence of key terms relevant to the topic. The `Frontier` contains a `PriorityQueue` for each wave of the crawl, where the seed URLs make up wave0, the outlinks found in wave0 make up wave1, and so forth. The crawler will visit links wave-by-wave, by each page's calculated priority. Every 500 documents, the `Frontier` is refreshed, where the next 250 most relevant links are re-scored using update inlink counts. 

When the crawler pops each webpage off the `Frontier`, it reads the domain's `robots.txt` and follows the set politeness policy. 

As the crawler parses the 40,000 documents, it will back up the `Frontier`, inlink and outlink graphs, and other relevant data in case of a crash. Then, the crawler can simply resume its crawl at the last known position. 

Each visited and parsed webpage is saved as a text file containing the text content, headers, inlinks, outlinks, and raw HTML. 

## Indexing 
`indexer.py`

After crawling all 40,000 documents, the documents were uploaded to a group Elasticsearch cluster. Each team member indexed their files and merged any duplicate documents, where an existing URL will have its inlinks, outlinks, and authors updated to reflect the additional data. 
