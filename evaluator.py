import numpy as np
import csv
import utils
import os


# file pages must be spaced

def evaluate(predictor, eval_params, test_file, train_amt,
             data_name,
             group_by=1,
             eval_point=1,
             verbose=False):

    dest_file = "%s_full_%d.txt" % (data_name, predictor)
    dest_log = "%s_full_%d.txt" % (data_name, predictor.name)

    stat_file = "%s_%s_%s_results_%d_%d.txt" % (data_name,
                                                predictor.name,
                                                predictor.eval_param_string(eval_params),
                                                group_by, eval_point)

    if os.path.isfile(os.path.join('logged', stat_file)):
        return

    temp_file = utils.create_grouping(test_file, group_by)

    unique_pages = set()

    stats = []

    with open(temp_file, 'rb') as test:
        next(test)
        for row in test:
            # print row
            vals = map(lambda x: int(x), row.split(" "))
            for v in vals:
                unique_pages.add(v)

            if len(vals) > eval_point:
                know = vals[0:eval_point]
                # print know
                unknown = vals[eval_point:]
                # print unknown

                guesses = predictor.predict(know, eval_params)

                found = 0
                for p in guesses:
                    if p in unknown:
                        found += 1

                precision = float(found) / len(guesses) if len(guesses) != 0 else 0
                recall = float(found) / len(unknown) if len(unknown) != 0 else 0

                # if precision == 0 or recall == 0:
                #     print vals
                #     print know
                #     print guesses
                #     print unknown
                #     print found
                #     print '--'*50

                stats.append((found,
                              len(guesses),
                              len(unknown),
                              precision,
                              recall))

    with open(stat_file, 'wb') as csv_file:
        s_writer = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for s in stats:
            s_writer.writerow(list(s))

    def aggregate_stats(place):
        count = 0
        for x in stats:
            count += x[place]
        return count

    total_found = aggregate_stats(0)
    total_guesses = aggregate_stats(1)
    total_unknown = aggregate_stats(2)

    def find_avg_and_var(place):
        data = map(lambda x: x[place], stats)
        return float(np.mean(data)), float(np.var(data))

    avg_found, var_found = find_avg_and_var(0)
    avg_guesses, var_guesses = find_avg_and_var(1)
    avg_unknown, var_unknown = find_avg_and_var(2)

    overall_precision = float(total_found) / float(total_guesses) if total_guesses != 0 else 0
    overall_recall = float(total_found) / float(total_unknown) if total_unknown != 0 else 0

    avg_precision, var_precision = find_avg_and_var(3)
    avg_recall, var_recall = find_avg_and_var(4)

    os.system('rm %s' % temp_file)

    to_write_file = open(dest_file, 'a')
    to_write_file.write("--" * 50 + "\n")
    to_write_file.write('Run Config\n'
                        ' Predictor: {}\n'
                        ' Test File: {}\n'
                        ' Eval_Point: {:d}\n'
                        ' Xct in a Group: {:d}\n'
                        .format(predictor.name + " " + predictor.eval_param_string(eval_params),
                                test_file,
                                eval_point,
                                group_by))

    to_write_file.write("Unique pages: %d\n" % (len(unique_pages)))
    to_write_file.write('Run Result\n'
                        ' Xct Analyzed: {:d}\n'
                        ' Avg Precision: {:.4f}\n'
                        ' Var Precision: {:.4f}\n'
                        ' Avg Recall: {:.4f}\n'
                        ' Var Recall: {:.4f}\n'
                        ' Avg Found: {:.4f}\n'
                        ' Var Found: {:.4f}\n'
                        ' Avg Guesses: {:.4f}\n'
                        ' Var Guesses: {:.4f}\n'
                        ' Avg Unknown: {:.4f}\n'
                        ' Var Unknown: {:.4f}\n'
                        ' Overall Precision: {:.4f}\n'
                        ' Overall Recall: {:.4f}\n'
                        ' Total Found: {:d}\n'
                        ' Total Guesses: {:d}\n'
                        ' Total Unknown: {:d}\n'
                        .format(len(stats),
                                avg_precision,
                                var_precision,
                                avg_recall,
                                var_recall,
                                avg_found,
                                var_found,
                                avg_guesses,
                                var_guesses,
                                avg_unknown,
                                var_unknown,
                                overall_precision,
                                overall_recall,
                                total_found,
                                total_guesses,
                                total_unknown))
    if verbose:
        to_write_file.write(str(stats) + "\n")
    to_write_file.write("--" * 50 + "\n\n")
    to_write_file.close()

    to_write_file = open(dest_log, 'a')
    to_write_file.write('{:d},{:s},{:d},{:d},{:d},'.format(train_amt,
                                                           predictor.eval_param_string(eval_params),
                                                           group_by,
                                                           eval_point,
                                                           len(unique_pages)))
    to_write_file.write('{:d},'
                        '{:.4f},{:.4f},'
                        '{:.4f},{:.4f},'
                        '{:.4f},{:.4f},'
                        '{:.4f},{:.4f},'
                        '{:.4f},{:.4f},'
                        '{:.4f},{:.4f},'
                        '{:d},{:d},{:d}\n'
                        .format(len(stats),
                                avg_precision,
                                var_precision,
                                avg_recall,
                                var_recall,
                                avg_found,
                                var_found,
                                avg_guesses,
                                var_guesses,
                                avg_unknown,
                                var_unknown,
                                overall_precision,
                                overall_recall,
                                total_found,
                                total_guesses,
                                total_unknown))
    to_write_file.close()
