import numpy as np
import datetime


class IndicatorScorer:
    def __init__(self, values, scores, input_type):
        if input_type == 'values':
            if len(values) != len(scores):
                raise ValueError("if input_type == 'values', len(scores) must be the equal to len(values)")
        elif input_type == 'edges':
            if len(values) + 1 != len(scores):
                raise ValueError("if input_type == 'edges', len(scores) must be the equal to len(values) + 1")
        else:
            raise ValueError(f"unrecognized argument {input_type}")
        self._values = values
        self._scores = scores
        self._input_type = input_type

    @property
    def input_type(self):
        return self._input_type

    @property
    def scores(self):
        return self._scores

    @property
    def values(self):
        return self._values

    def __get_index(self, a):
        """
        Assign an index to the value a given the edges (thresholds) provided
        :param a:
        :return: float, numpy.array
        """
        if self.input_type == 'values':
            return self.values.index(a)
        else:
            return np.digitize(a, self.values)

    def get_score(self, a, normalize=False):
        """
        Return score associated to value a
        :param a: data
        :type a: float, list
        :param normalize: boolean flag for score normalisation
        :type normalize: bool
        :return: score
        :rtype: float, list
        """
        if self.scores is None:
            raise ValueError("No score associated to values or thresholds")
        try:
            return [self.get_score(it) for it in a]
        except TypeError as te:
            index = self.__get_index(a)
            if normalize:
                score = (self.scores[index] - min(self.scores)) / (max(self.scores) - min(self.scores))
            else:
                score = self.scores[index]
            return score

    def get_normalized_score(self, a):
        """
        Return normalized score [0,1]
        :param a: data
        :type a: float
        :return: score
        :rtype: float
        """
        return self.get_score(a, normalize=True)

    def display_digitization(self, x):
        """
        Display the values and the intervals in which they lie
        :param x: array of values
        :type x: list
        :return:
        """
        index = self.__get_index(x)
        try:
            for n in range(len(x)):
                print(self.values[index[n] - 1], "<=", x[n], "<", self.values[index[n]])
        except TypeError as te:
            print(self.values[index - 1], "<=", x, "<", self.values[index])


if __name__ == '__main__':
    scorer_edges = IndicatorScorer(values=[2, 4, 6, 8], scores=[0, 1, 2, 3, 4], input_type='edges')
    for i in np.linspace(0, 10, 21):
        print(i, scorer_edges.get_score(i))

    scorer_values = IndicatorScorer(values=[2, 4, 6, 8], scores=[4, 6, 8, 10], input_type='values')
    for i in [2, 2, 4, 8, 6]:
        print(i, scorer_values.get_score(i))
