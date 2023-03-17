import queue

class Frontier:
    def __init__(self):
        self.waves = {}

    def restore_frontier(self, url_fo_dict, wave_no):
        pq = PriorityQueue()
        pq.frontier_obj_dict = url_fo_dict  # set dict
        for url, fo in url_fo_dict.items(): # set queue
            pq.manual_enqueue(url, fo)
        self.waves[wave_no] = pq

    def add_wave(self, wave_no):
        self.waves[wave_no] = PriorityQueue()

    # returns a PriorityQueue holding all objects in a particular wave
    def get_wave(self, wave_no):
        try:
            return self.waves[wave_no]
        except:
            self.add_wave(wave_no)
            return self.get_wave(wave_no)

    def refresh_wave(self, wave_no, new_pq):
        self.waves[wave_no] = new_pq

    def is_empty(self):
        if len(self.waves) == 0:
            return True
        return False

    def remove_empty_wave(self, wave_no):
        del self.waves[wave_no]

class PriorityQueue:
    def __init__(self):
        self.queue = queue.PriorityQueue() # the queue only holds (score, url)
        self.frontier_obj_dict = {} # retains all frontier_object info (not just url)

    def enqueue(self, score, frontier_obj):
        self.frontier_obj_dict[frontier_obj.link] = frontier_obj
        t = (score, frontier_obj.link)
        self.queue.put(t)

    def manual_enqueue(self, url, fo):
        score = fo.score
        self.queue.put((score, url))

    def get(self):
        score, next_url = self.queue.get()
        ret_obj = self.frontier_obj_dict[next_url]
        # del self.frontier_obj_dict[next_url] # delete from dict once popped
        return score, ret_obj

    def get_frontier_object(self, url):
        return self.frontier_obj_dict[url]

    def get_queue(self):
        return self.queue.queue

    def is_empty(self):
        if len(self.queue.queue) == 0:
            return True

        return False
#
# utils = Utils()
# fs = ["/Users/ellataira/Desktop/is4200/crawling/dict_backup/frontier_at_19000_wave_2_pages.pkl",
#                 "/Users/ellataira/Desktop/is4200/crawling/dict_backup/frontier_at_19000_wave_3_pages.pkl"]
# nf = Frontier()
# i = 2
# for f in fs:
#     w2 = utils.read_pickle(f)
#     nf.restore_frontier(w2, i)
#     w = nf.waves
#     pq = nf.get_wave(i)
#     q = pq.get_queue()
#     print(q)
#     i+=1


