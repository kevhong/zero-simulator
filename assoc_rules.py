import os
import pickle
import csv

from collections import defaultdict

from page_predictor import page_predictor


def convert(xct_page_access_file, output_file):
    with open(xct_page_access_file, 'rb') as csvfile, open(output_file, 'w') as outfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in reader:
            to_write = []
            written = set()
            for item in row[1:]:
                if item not in written:
                    to_write.append(item)
                    written.add(item)

            outfile.write(reduce(lambda x, y: str(x) + " " + str(y),
                                 to_write) + '\n')


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

    def predict(self, seen, eval_params):
        """
        :param seen: list of integers of pids
        :param eval_params:
        :return: set of predicted pages
        """
        min_conf = eval_params.get('min_conf', 0.2)
        print min_conf

        def get_all_subseq(input_string):
            length = len(input_string)
            return [input_string[i:j + 1] for i in xrange(length) for j in xrange(i, length)]

        # http://stackoverflow.com/questions/952914/making-a-flat-list-out-of-list-of-lists-in-python
        flatten = lambda l: [item for sublist in l for item in sublist]

        guesses = set()

        # keys always in increasing order, so we generate all possible
        to_guess = map(lambda g:
                       reduce(lambda x, y: "%s %d" % (x, y), g, "").strip(),
                       get_all_subseq(seen))

        for g in to_guess:
            to_add = flatten(
                map(lambda g: map(lambda x: int(x), g),
                    map(lambda x: x[0].split(),
                        filter(lambda x: x[1] >= min_conf, self.rules_dic[g]))))
            for t_a in set(to_add):
                guesses.add(t_a)

        for x in seen:  # don't guess part of what we have already seen
            guesses.discard(x)

        return guesses

    def eval_param_string(self, eval_params):
        return "min_conf: {:.2f}" % eval_params.get('min_conf')
