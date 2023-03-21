import queue

# represents the frontier that the crawler will parse through using ~BFS
class Frontier:
    # the frontier consists of multiple waves, each of which will have its own priority queue of pages to crawl
    # because the crawler will crawl one wave at a time (BFS)
    def __init__(self):
        self.waves = {}

    # restores frontier from saved dict after crash
    def restore_frontier(self, url_fo_dict, wave_no):
        pq = PriorityQueue()
        pq.frontier_obj_dict = url_fo_dict  # set dict
        for url, fo in url_fo_dict.items(): # set queue
            pq.manual_enqueue(url, fo)
        self.waves[wave_no] = pq

    # adds a new wave to frontier
    def add_wave(self, wave_no):
        self.waves[wave_no] = PriorityQueue()

    # returns a PriorityQueue holding all objects in a particular wave
    def get_wave(self, wave_no):
        try:
            return self.waves[wave_no]
        except:
            self.add_wave(wave_no)
            return self.get_wave(wave_no)

    # refreshes a given wave with new priority queue
    def refresh_wave(self, wave_no, new_pq):
        self.waves[wave_no] = new_pq

    # returns True if the frontier is empty
    def is_empty(self):
        if len(self.waves) == 0:
            return True
        return False

    # removes a wave when empty
    def remove_empty_wave(self, wave_no):
        del self.waves[wave_no]

# custom priority queue class to contain FrontierObject values
class PriorityQueue:
    def __init__(self):
        self.queue = queue.PriorityQueue() # the queue only holds (score, url)
        self.frontier_obj_dict = {} # retains all frontier_object info (not just url)

    # adds a (score, FrontierObject) pair to priority queue
    def enqueue(self, score, frontier_obj):
        self.frontier_obj_dict[frontier_obj.link] = frontier_obj
        t = (score, frontier_obj.link)
        self.queue.put(t)

    # manually enqueues (used for resumed crawls)
    def manual_enqueue(self, url, fo):
        score = fo.score
        self.queue.put((score, url))

    # gets the next item in the priority queue (lowest score)
    def get(self):
        score, next_url = self.queue.get()
        ret_obj = self.frontier_obj_dict[next_url]
        # del self.frontier_obj_dict[next_url] # delete from dict once popped
        return score, ret_obj

    # gets the FrontierObject based on its url
    def get_frontier_object(self, url):
        return self.frontier_obj_dict[url]

    # returns priorityqueue.queue
    def get_queue(self):
        return self.queue.queue

    # returns True if the priority queue is empty (all items have been popped)
    def is_empty(self):
        if len(self.queue.queue) == 0:
            return True
        return False
