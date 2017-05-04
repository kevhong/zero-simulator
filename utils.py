import os
import csv

def create_grouping(src, groupBy=1):
    out_fn = os.path.join('exp_files',
                          os.path.basename(src).split(".")[0] + "_groupedBy_%d.txt" % (groupBy))

    if os.path.isfile(out_fn):
        return out_fn

    with open(src, 'rb') as src_file, open(out_fn, 'w') as out_file:
        reader = csv.reader(src_file, delimiter=' ', quotechar='|')
        count = 0
        to_write = []
        written = set()
        for row in reader:
            for item in row:
                if item not in written:
                    to_write.append(item)
                    written.add(item)
            count += 1
            if count % groupBy == 0:
                out_file.write(reduce(lambda x, y: str(x) + " " + str(y),
                                      to_write) + '\n')
                to_write = []
                written = set()

    return out_fn

def split_file(src, cutoff):
    test_fn = os.path.join('exp_files', os.path.basename(src).split(".")[0] + "_test_%d.txt" % (cutoff*100))
    train_fn = os.path.join('exp_files',
        os.path.basename(src).split(".")[0] + "_train_%d.txt" % (int((1-cutoff)*100)))

    if not os.path.isfile(test_fn) or not os.path.isfile(train_fn):
        with open(src, 'rb') as src_file, open(test_fn, 'w') as test_file, \
                open(train_fn, 'w') as train_file:

            for line in src_file:
                r = np.random.uniform()
                if r >= cutoff:
                    train_file.writelines(line)
                else:
                    test_file.writelines(line)

    return test_fn, train_fn

def convert_to_spaces(xct_page_access_file, output_file):
    with open(xct_page_access_file, 'rb') as csvfile, open(output_file, 'w') as outfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in reader:
            to_write = []
            for item in row[1:]:
                to_write.append(item)

            outfile.write(reduce(lambda x, y: str(x) + " " + str(y),
                                 to_write) + '\n')
