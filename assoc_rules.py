import os
import pickle
import csv
import utils

from collections import defaultdict

from page_predictor import page_predictor



def train_model(src, minsup, minconf):
    out_fn = os.path.join('exp_files',
                          os.path.basename(src).split(".")[0] + "_assocRules_%d_%d.txt" % (minsup, minconf))
    if not os.path.isfile(out_fn):
        to_run = 'java -Xmx8192m  -jar spmf.jar run MNR %s %s %d%% %d%%' % \
                 (src, out_fn, minsup, minconf)
        print to_run
        os.system(to_run)
    return out_fn


# rules always stored in
def create_dict(rules_file):
    out_fn = os.path.join('exp_files',
                          os.path.basename(rules_file).split(".")[0] + "_pickle.txt")

    rules_dic = defaultdict(list)

    with open(rules_file, 'rb') as rules:
        for row in rules:
            parsed = row.split(" ==>")
            # print row
            key = reduce(lambda x, y: "%s %d" % (x, y),
                         sorted(map(lambda x: int(x),
                                    parsed[0].strip().split(" "))),
                         "").strip()
            # print key
            val = reduce(lambda x, y: "%s %d" % (x, y),
                         sorted(map(lambda x: int(x),
                                    parsed[1][0:parsed[1].find("#SUP:")].strip().split(" "))),
                         "").strip()
            # print val
            conf = float(parsed[1].split(" ")[-1])
            # print conf
            rules_dic[key].append((val, conf))

    return rules_dic


class AssocRulesPredictor(page_predictor):
    def __init__(self, name, src, min_sup, min_conf):
        self.name = name
        rules_file = train_model(src, min_sup, min_conf)
        self.rules_file = rules_file
        self.rules_dic = create_dict(rules_file)
        self.unique = True

    def predict(self, seen, eval_params, all=False):
        '''
        :param seen: list of integers of pids
        :param eval_params:
        :return: set of predicted pages
        '''

        min_conf = eval_params.get('min_conf', 0.2)


        def get_all_subseq(input_string):
            length = len(input_string)
            return [input_string[i:j + 1] for i in xrange(length) for j in xrange(i, length)]

        # http://stackoverflow.com/questions/952914/making-a-flat-list-out-of-list-of-lists-in-python
        flatten = lambda l: [item for sublist in l for item in sublist]

        guesses = set()

        seen_pages = list(set(seen))

        # keys always in increasing order, so we generate all possible
        to_guess = map(lambda g:
                       reduce(lambda x, y: "%s %d" % (x, y), g, "").strip(),
                       get_all_subseq(seen_pages))

        for g in to_guess:
            to_add = flatten(
                map(lambda g: map(lambda x: int(x), g),
                    map(lambda x: x[0].split(),
                        filter(lambda x: x[1] >= min_conf, self.rules_dic[g]))))
            for t_a in set(to_add):
                guesses.add(t_a)

        if all:
            for x in seen:  # don't guess part of what we have already seen
                guesses.discard(x)

        #guesses = filter(lambda x: x < 100, guesses)

        return guesses

    def eval_param_string(self, eval_params):
        return "min_conf: {:.2f}" % eval_params.get('min_conf')

    def dict_stats(self):
        unique = set()
        print len(self.rules_dic)
        print self.rules_dic
        for x,y in self.rules_dic.iteritems():
            for k in x.split(" "):
                #print k
                unique.add(k)
            for t in y:
                print y
                for k in t[0].split(" "):
                    #print k
                    unique.add(k)

        return len(unique)


if __name__ == '__main__':
    predictor_t = AssocRulesPredictor('test', 'spmf_run3_train_100_groupedBy_1', 5, 20)
    t1 = "3 4 14 5 88053 88051 12 76204 79258 13 133345 97791 96083 11 123978 184838 192367 27524 36443 60296 57997 27527 6048 33940 30875 19083 43617 42315 16070 40424 39673 94752 133340 111653 109662 127332 137246 151218 147793 150221 158521 159760 6341 31011 126220 140374 137813 131812 144043 142903 13966 37793 9 64770 191487 10 122561 79816 8 186709 190826"
    print predictor_t.eval_line_once(t1, {'min_conf': .20}, 5)
    print predictor_t.dict_stats()
    # print predictor_t.eval_line_once(t2, {'min_conf': .20}, 5)
    # print predictor_t.eval_line_once(t3, {'min_conf': .20}, 5)

