import cachetools
import math
import assoc_rules
import sys

import oracle

from collections import defaultdict


class ZeroThread(object):
    def __init__(self, name, database):
        self.name = name
        self.pinned_pages = set()
        self.DB = database
        self.place = 0
        self.curr_xct = []
        self.predicted = set()
        self.unique_pages = set()
        self.done = False
        self.waiting = False

    def add_xct(self, line):
        self.curr_xct = map(lambda x: int(x), line.split(",")[1:])
        self.place = 0
        self.pinned_pages = set()
        self.predicted = set()
        self.unique_pages = set()
        self.waiting = False

    def wanted_page(self):
        if self.is_done():
            return -1
        else:
            return self.curr_xct[self.place]

    def preform_tick(self):
        wanted_page = self.wanted_page()

        if self.DB.request_page(wanted_page, self):
            self.place += 1
            self.unique_pages.add(wanted_page)
            return self.wanted_page(), True

        return self.wanted_page(), False

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
        self.predicted_pages = 0

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
        self.predicted_pages = 0

    def request_page(self, page, thread):
        # if len(thread.predicted) != 0:
        #     print "~~~"*10
        #     print thread.predicted
        #     print page
        #     print page in thread.predicted
        #     print "~~"*10
        if page in thread.predicted:
            self.predicted_pages += 1

        if page in self.requested_pages:
            thread.waiting = True
            return False
        elif page in self.buffer_pool:
            if not thread.waiting:
                temp = self.buffer_pool[page]  # to simulate access
            thread.waiting = False
            return True
        elif page in self.predicted_pages_pool:
            temp = self.predicted_pages_pool[page]  # to simulate access
            return True
        else:
            self.wanted.append((page, thread))
            return False

    def simulate(self, xct_file, dirty_page_file, latency_penalty,
                 recovery_penalty, predict_point, eval_params, oracle = None):

        self.clean()

        dirty_pages = set()
        with open(dirty_page_file, 'r') as handle:
            for row in handle:
                d_page = row.split(",")[0]
                dirty_pages.add(int(d_page))

        sim_done = False
        ticks = 0
        completed = 0
        recovered = 0
        requested = defaultdict(list)

        tps_stats = []
        unique  = set()

        with open(xct_file, 'r') as handle:

            next(handle) # first is 2 2 2 2 (system xct)

            while not sim_done:

                self.wanted = []

                to_del = []
                for page, time in self.requested_pages.iteritems():
                    unique.add(page)
                    if time >= ticks:
                        self.buffer_pool[page] = 1
                        if page in dirty_pages:
                            dirty_pages.remove(page)
                            recovered += 1
                        for thread in requested[page]:
                            if page in thread.predicted:
                                thread.pinned_pages.add(page)
                                #self.predicted_pages_pool[page] = 1
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

                    next_page, moved = curr_thread.preform_tick()
                    if moved and self.predictor and \
                            ((self.predictor.unique == False and curr_thread.place == predict_point) or
                                ((self.predictor.unique and len(curr_thread.unique_pages) == predict_point))):

                        if self.predictor is not None and len(curr_thread.predicted) == 0:
                            if not oracle:
                                predicted = self.predictor.predict(curr_thread.curr_xct[:curr_thread.place], eval_params, all = True)
                            else:
                                predicted = oracle.predict(curr_thread.curr_xct[curr_thread.place:])
                            for p in predicted:
                                if p not in self.buffer_pool or p not in self.predicted_pages_pool:  # to do with locks
                                    self.wanted.append((p, curr_thread))
                                else:
                                    curr_thread.pinned_pages.add(p)
                                curr_thread.predicted.add(p)
                            # print set(curr_thread.curr_xct)
                            # print curr_thread.curr_xct[:curr_thread.place]
                            # print curr_thread.predicted
                            # print curr_thread.curr_xct[curr_thread.place:]
                            # print curr_thread.predicted.intersection(set(curr_thread.curr_xct[curr_thread.place:]))
                            # print "--"*50
                            # count += 1

                for page, thread in self.wanted:
                    requested[page].append(thread)
                    penalty = ticks + 1
                    #penalty += latency_penalty * math.ceil((len(requested.keys()) + len(self.wanted))/5)
                    #penalty =latency_penalty
                    if page not in self.requested_pages:  # not yet requested
                        if page in dirty_pages:
                            penalty += recovery_penalty
                        self.requested_pages[page] = penalty

                ticks += 1

                # if completed == 10:
                #     print "---"
                #     print self.predicted_pages
                #     return

                if ticks % 5000 == 0:
                    #print "%d completed in last 5000, curr: %d recovered %d predicted %d" % \
                          #(completed, ticks, recovered, self.predicted_pages)
                    tps_stats.append((ticks, completed, recovered))
                    #print "%d %d %d" % (ticks, completed, recovered)
                    print "%d" % completed
                    completed = 0
                    recovered = 0
                    self.predicted_pages = 0
                    #print len(unique)

                sim_done = all(map(lambda sm_thread: sm_thread.done, self.threads))

        print ticks
        print self.predicted_pages
        print len(unique)
        return ticks


if __name__ == '__main__':
    predictor_t = assoc_rules.AssocRulesPredictor('test', 'spmf_run3_train_50_groupedBy_1', 5, 20)
    if sys.argv[1] == '1':
        print "Predictor"
        zero = ZeroSimulator(500, 0, 64, predictor_t)
        zero.simulate('run3_xct.txt', 'run3_page_info.txt', 25, 500, 5, {'min_conf': 0.2})
    elif sys.argv[1] == '2':
       print "Baseline"
       zero2 = ZeroSimulator(500, 0, 64, None)
       zero2.simulate('run3_xct.txt', 'run3_page_info.txt', 25, 500, 5, {'min_conf': 0.3})
    else:
        print "Oracle Mode"
        oracle_t = oracle.PageOracle(80, 20)
        zero2 = ZeroSimulator(1000, 0, 64, predictor_t)
        zero2.simulate('run3_xct.txt', 'run3_page_info.txt', 1, 50, 5, {'min_conf': 0.3}, oracle=oracle_t)



    #todo: latching + predicted pages pool
