



class PageOracle(object):

    def __init__(self, recall, precision):
        self.recall = recall
        self.precision = precision
        self.bad = "a"


    def predict(self, access):
        total = len(access)
        get = int(total * self.recall / 100) - 1
        num_bad = int(get / self.precision)
        to_add = []
        for a in xrange(num_bad):
            to_add.append(self.bad)
            self.bad += str(a)
        return access[0:get] + to_add

