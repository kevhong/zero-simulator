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
        self.predicted = set()
        self.unique_pages = set()
        self.done = False

    def add_xct(self, line):
        self.curr_xct = map(lambda x: int(x), line.split(",")[1:])
        self.place = 0
        self.pinned_pages = set()
        self.predicted = set()
        self.unique_pages = set()

    def preform_tick(self):
        wanted_page = self.curr_xct[self.place]
        if wanted_page in self.pinned_pages:
            self.place += 1
            self.unique_pages.add(wanted_page)
            return True

        if self.DB.request_page(self.curr_xct[self.place], self):
            self.place += 1
            self.unique_pages.add(wanted_page)
            return True

    def is_done(self):
        return self.done or self.place == len(self.curr_xct)

    def get_stats(self):
        pass


class ZeroSimulator(object):
    def __init__(self, buffer_pool_size, predicted_pages_size,
                 threads_c, predictor):
        self.buffer_pool_limit = buffer_pool_size
        self.buffer_pool = cachetools.LRUCache(maxsize=buffer_pool_size)
        self.predicted_pages_limit = predicted_pages_size
        self.predicted_pages_pool = cachetools.LRUCache(maxsize=predicted_pages_size)
        self.threads = []

        for x in xrange(threads_c):
            self.threads.append(ZeroThread(str(x), self))

        self.predictor = predictor
        self.requested_pages = {}
        self.lock_manger = defaultdict(list)
        self.done = False
        self.granted = {}
        self.wanted = []

    def clean(self):
        self.buffer_pool = cachetools.LRUCache(maxsize=self.buffer_pool_limit)
        self.predicted_pages_pool = cachetools.LRUCache(maxsize=self.predicted_pages_limit)
        threads_c = len(self.threads)
        self.threads = []

        for x in xrange(threads_c):
            self.threads.append(ZeroThread(str(x), self))

        self.requested_pages = {}
        self.lock_manger = defaultdict(list)
        self.done = False
        self.granted = {}
        self.wanted = []

    def request_page(self, page, thread):
        # if page in thread.predicted:
        #     return True
        if page in self.requested_pages:
            return False
        elif page in self.buffer_pool:
            temp = self.buffer_pool[page]  # to simulate access
            return True
        elif page in self.predicted_pages_pool:
            temp = self.buffer_pool[page]  # to simulate access
            return True
        else:
            self.wanted.append((page, thread))
            return False

    def simulate(self, xct_file, dirty_page_file, latency_penalty,
                 recovery_penalty, predict_point, eval_params):

        self.clean()

        dirty_pages = set()
        with open(dirty_page_file, 'r') as handle:
            for row in handle:
                d_page = row.split(",")[2]
                dirty_pages.add(int(d_page))

        sim_done = False
        ticks = 0
        completed = 0
        recovered = 0
        requested = defaultdict(list)

        tps_stats = []

        with open(xct_file, 'r') as handle:

            next(handle) # first is 2 2 2 2 (system xct)

            while not sim_done:

                self.wanted = []

                to_del = []
                for page, time in self.requested_pages.iteritems():
                    if time <= ticks:
                        self.buffer_pool[page] = 1
                        if page in dirty_pages:
                            dirty_pages.remove(page)
                            recovered += 1
                        for thread in requested[page]:
                            if page in thread.predicted:
                                thread.pinned_pages.add(page)
                        to_del.append(page)

                for page in to_del:
                    self.requested_pages.pop(page)
                    requested[page] = []

                for curr_thread in self.threads:
                    if curr_thread.is_done():
                        if not curr_thread.done:
                            completed += 1
                        try:
                            next_xct = next(handle)
                            curr_thread.add_xct(next_xct)
                        except StopIteration:
                            curr_thread.done = True
                    else:
                        if curr_thread.preform_tick() and \
                                ((predictor_t.unique == False and curr_thread.place == predict_point) or
                                    ((predictor_t.unique and len(curr_thread.unique_pages) == predict_point))):
                            if self.predictor is not None:
                                predicted = self.predictor.predict(curr_thread.curr_xct[:curr_thread.place], eval_params)
                                for p in predicted:
                                    if p not in self.buffer_pool or p not in self.predicted_pages_pool:  # to do with locks
                                        self.wanted.append((p, curr_thread))
                                    else:
                                        curr_thread.pinned.add(p)
                                    curr_thread.predicted.add(p)

                for page, thread in self.wanted:
                    requested[page].append(thread)
                    penalty = latency_penalty * math.ceil(len(requested.keys()) + len(self.wanted))
                    if page not in self.requested_pages:  # not yet requested
                        if page in dirty_pages:
                            penalty += recovery_penalty
                        self.requested_pages[page] = penalty

                ticks += 1

                if ticks % 10000 == 0:
                    print "%d completed in last 10000, curr: %d recovered %d" % (completed, ticks, recovered)
                    tps_stats.append((ticks, completed))
                    completed = 0
                    recovered = 0

                sim_done = all(map(lambda sm_thread: sm_thread.done, self.threads))

        print tps_stats
        print ticks
        return ticks


if __name__ == '__main__':
    predictor_t = assoc_rules.AssocRulesPredictor('test', 'spmf_run3_train_100_groupedBy_1', 5, 20)
    zero = ZeroSimulator(5000, 0, 8, predictor_t)
    zero.simulate('run3_xct.txt', 'run3_dirty.txt', 1, 40, 5, {'min_conf': 0.2})
    # zero2 = ZeroSimulator(1000, 0, 8, None)
    # zero2.simulate('run3_xct.txt', 'run3_dirty.txt', 2, 20, 5, {'min_conf': 0.2})
