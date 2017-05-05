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

    def predict(self, seen, eval_params):
        """
        :param seen: list of integers of pids
        :param eval_params:
        :return: set of predicted pages
        """
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

        for x in seen:  # don't guess part of what we have already seen
            guesses.discard(x)

        #guesses = filter(lambda x: x < 100, guesses)

        return guesses

    def eval_param_string(self, eval_params):
        return "min_conf: {:.2f}" % eval_params.get('min_conf')


if __name__ == '__main__':
    # predictor_t = AssocRulesPredictor('test', 'spmf_run3_train_100_groupedBy_1', 5, 20)
    # t1 = "3 4 14 5 81633 86256 12 76204 81226 13 133340 77826 77547 11 123978 186896 192363 27527 7402 8847 7328 127332 126518 121619 120949 51623 64068 62036 60741 101536 116751 111956 111221 116721 111109 71592 68856 68258 49949 48946 47711 130815 126195 125238 1536 2698 1574 120746 116863 115130 108751 104992 103390 27524 31453 32021 30374 9 131420 191324 10 80820 95458 8 128082 191126"
    # t2 = "3 4 14 5 40762 45266 12 127332 126518 13 133340 91258 90971 11 123978 57347 192237 51623 64068 33962 32908 101536 116751 82300 81665 116721 81538 71592 40162 39774 192417 49949 21428 20303 130815 95827 95117 27527 1536 147268 146518 120746 86812 85444 108751 75438 74195 27524 31453 3173 2899 48040 19233 18478 19965 166080 164549 9 50394 191891 10 43572 49027 8 128082 191113"
    # t3 = "6 132331 88718 5 77024 78301 10 122561 81430 9 64770 87424 184152 186291 11 123978 184838 186397"
    # print predictor_t.eval_line_once(t1, {'min_conf': .20}, 5)
    # print predictor_t.eval_line_once(t2, {'min_conf': .20}, 5)
    # print predictor_t.eval_line_once(t3, {'min_conf': .20}, 5)


    m_sup_s = 20
    m_conf_s = 30
    for m_sup_s in [20, 10, 5, 3]:
        for test_part in [0, 0.2, 0.4, 0.5, 0.7, 0.8]:
            test, train = utils.split_file("spmf_run5.txt", test_part)
            if test_part == 0:
                test = train

            train_grouped = utils.create_grouping(train, 1)

            model = train_model(train_grouped, m_sup_s, m_conf_s)
            rules_dict = create_dict(model)
