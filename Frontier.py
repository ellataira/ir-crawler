import queue

class Frontier:
    def __init__(self):
        self.waves = {}

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

    def get(self):
        score, next_url = self.queue.get()
        ret_obj = self.frontier_obj_dict[next_url]
        del self.frontier_obj_dict[next_url] # delete from dict once popped
        return score, ret_obj

    def get_frontier_object(self, url):
        return self.frontier_obj_dict[url]

    def get_queue(self):
        return self.queue.queue

    def is_empty(self):
        if len(self.queue.queue) == 0:
            return True

        return False

