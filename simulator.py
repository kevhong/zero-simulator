import cachetools
import math
import assoc_rules

from collections import defaultdict

class ZeroThread(object):
    def __init__(self, name, database):
        self.name = name
        self.pinned_pages = set()
        self.DB = database
        self.place = 0
        self.curr_xct = []
        self.predicted = set()
        self.done = False

    def add_xct(self, line):
        self.curr_xct = line.split(" ")[1:]
        self.place = 0
        self.pinned_pages = set()
        self.predicted = set()

    def preform_tick(self):
        wanted_page = self.curr_xct[self.place]
        if wanted_page in self.pinned_pages:
            self.place += 1
            return True

        if self.DB.request_page(self.curr_xct[self.place], self.name):
            self.place += 1
            return True

    def is_done(self):
        return self.done or self.place == len(self.curr_xct)

    def get_stats(self):
        pass



class ZeroSimulator(object):
     def __init__(self, buffer_pool_size, predicted_pages_size,
                  threads_c, predictor):
         self.buffer_pool_limit = buffer_pool_size
         self.buffer_pool = cachetools.LRUCache(maxsize = buffer_pool_size)
         self.buffer_pool_limit = predicted_pages_size
         self.predicted_pages_pool = cachetools.LRUCache(maxsize = predicted_pages_size)
         self.threads = []

         for x in xrange(threads_c):
            self.threads.append(ZeroThread(str(x), self))

         self.predictor = predictor
         self.requested_pages = []
         self.lock_manger = defaultdict(list)
         self.done = False
         self.granted = {}

     def request_page(self, page, name):
        if page in self.requested_pages:
            return False
        elif page in self.buffer_pool:
            temp = self.buffer_pool['page'] # to simulate access
            return True
        elif page in self.predicted_pages_pool:
            temp = self.buffer_pool['page'] # to simulate access
            return True

     def simulate(self, xct_file, dirty_page_file, latency_penalty,
                  recovery_penalty, predict_point):
        dirty_pages = set()
        with open(dirty_page_file, 'r') as handle:
            for row in handle:
                d_page = row[:row.find(",")]
                dirty_pages.add(d_page)

        sim_done = False
        ticks = 0
        completed = 0
        requested = defaultdict(list)

        tps_stats = []

        with open(xct_file, 'r') as handle:

            while not sim_done:

                self.wanted = []

                to_del = []
                for page, time in self.requested_pages.iteritems():
                    if time <= ticks:
                        self.buffer_pool[page] = 1
                        if page in dirty_pages:
                            dirty_pages.remove(page)
                        for thread in requested[page]:
                            thread.pinned.add(page)
                        to_del.add(page)

                for page in to_del:
                    self.requested_pages.pop(page)
                    requested[page] = []


                for t in self.threads:
                    if t.is_done():
                        if not t.done:
                            completed += 1
                        next_xct = next(handle)
                        if next_xct:
                            t.add_xct(next_xct)
                        else:
                            t.done = True
                    else:
                        if t.preform_tick() and t.place == predict_point:
                            if self.predictor != None:
                                predicted = self.predictor.predict(t.curr_xct[:t.place])
                                for p in predicted:
                                    if p not in self.buffer_pool or p not in self.pooled_pages: # to do with locks
                                        self.wanted.add((p, t.name))
                                    else:
                                        t.pinned.add(p)
                                    t.predicted.add(p)

                for page, thread in self.wanted:
                    requested[page].append(thread)
                    penalty = latency_penalty* math.ceil(len(requested.iterkeys()) + len(self.wanted))
                    if page not in self.requested_pages: # not yet requested
                        if page in dirty_pages:
                            penalty += recovery_penalty
                        self.requested_pages[page] = penalty

                ticks += 1

                if ticks % 200 == 0:
                   print "%d completed in last 200, curr: %d" % (completed, ticks)
                   tps_stats.append(ticks, completed)
                   completed = 0

                sim_done = all(map(lambda sm_thread: sm_thread.is_done()))

        print tps_stats
        return ticks


if __name__ == '__main__':
    predictor_t = assoc_rules.AssocRulesPredictor('test', 'spmf_run3_train_100_groupedBy_1', 5, 20)
    print "here"
    print predictor_t.predict([3, 4, 101536], {'min_conf':.90})
    t = ZeroSimulator(2, 3, 8, predictor_t)