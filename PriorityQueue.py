import queue


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
        return score, self.frontier_obj_dict[next_url]

    def get_frontier_object(self, url):
        return self.frontier_obj_dict[url]

    def get_p_queue(self):
        return self.queue.queue

    def is_empty(self):
        if self.queue.qsize() == 0:
            return True

        return False
