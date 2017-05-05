


# template class for predictor

class page_predictor(object):

    def __init(self):
        self.unique = False # if operates on a unqiue list of pages

    def predict(self, list_line, eval_params):
        '''
        :param list_line: a line from zero in int list form
        :param eval_params:
        :return: the predicted pages
        '''
        pass

    def eval_line_once(self, string_line, eval_params, eval_point):
        '''
        :param string_line: a line from zero in string form
        :param eval_params: eval_parms
        :return: precision and recall
        Used for testing cases
        '''
        PageIDs = map(lambda x: int(x), string_line.split(" "))

        if self.unique:
            to_add = []
            seen = set()
            for x in PageIDs:
                if x not in seen:
                    to_add.append(x)
                    seen.add(x)
            PageIDs = to_add

        if len(PageIDs) > eval_point:
            know = PageIDs[0:eval_point]
            # print know
            unknown = PageIDs[eval_point:]
            # print unknown

            guesses = self.predict(know, eval_params)

            found = 0
            for p in guesses:
                if p in unknown:
                    found += 1

            precision = float(found) / len(guesses) if len(guesses) != 0 else 0
            recall = float(found) / len(unknown) if len(unknown) != 0 else 0

            print guesses
            print unknown

            return "{:.4f}, {:.4f}, {:d}, {:d}, {:d}".\
                format(precision, recall, found, len(guesses), len(unknown))

        return "Eval Point: %d, Xct Length: %d, No Prediction Made".format(eval_point, len(PageIDs))


    def eval_param_string(self, eval_params):
        pass
