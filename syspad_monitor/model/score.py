import os.path
import pprint
import numpy as np
from collections import deque

# this can be set into a config file
items_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "items.py"))

def slurp(path) -> str:
    """
    Helper file for slurpGraph function
    Read ascii file and return a string
    Parameters
    ----------
    path: str
        filename

    Returns
    -------
    data: str
        content of the ascii
    """
    with open(path, 'r') as file:
        data = file.read()
    return data


def slurpGraph(path) -> dict:
    """
    Read ascii file and return a string
    The content of the ascii file can have any python object syntax
    as long as the ``eval`` function can be applied

    Parameters
    ----------
    path: str
        filename

    Returns
    -------
    data: dict
        content of the ascii file as dictionary
    """
    return eval(slurp(path).replace(": nan", ": np.nan"))


class Score:
    """
    Create a score object

    Example 1:
    --------
    >>> from syspad_monitor.model.score import Score
    >>> score = Score()
    >>> data_raw = score.generate_data()
    >>> score.update(data_raw)
    >>> print(score.score4today)

    Example 2:
    --------
    >>> from syspad_monitor.model.score import Score
    >>> score = Score()
    >>> for i in range(8):
    >>>    score.update(score.generate_data())
    >>> print(score.data)
    """

    def __init__(self, history_length=6, item_list=None):
        """
        :param history_length: length of the data history (default 6 last days)
        :param item_list: dictionary containing items, possible score, etc.
        """
        self.history_length = history_length
        self.__data = self.get_empty_data()
        if item_list is None:
            item_list = slurpGraph(items_path)
        self.item_list = item_list

    def get_empty_data(self, ) -> dict:
        """
        Return an empty data structure
        :return: empty data structure
        :rtype: dict
        """
        return {'data_current_day': {'values': [],
                                     'filled': [],
                                     'filling_rate': None,
                                     'score4today': None,
                                     'score4tomorrow': None,
                                     'trust_index': None},
                'data_prev_days': deque(maxlen=self.history_length),
                'real_score_prev_days': deque(maxlen=self.history_length),
                'pred_score_prev_days': deque(maxlen=self.history_length),
                'trust_index_prev_days': deque(maxlen=self.history_length),
                'filling_rate_prev_days': deque(maxlen=self.history_length)}

    def get_number_of_items(self, ) -> int:
        """
        Return number of items
        :return: number of items
        :rtype: int
        """
        return len(self.item_list)

    def get_normalized_score(self, item_id: int, input_score: float) -> float:
        """
        Return normalized score such that score <- (score - min(score_list))/(max(score_list)- min(score_list))
        :param item_id: id of the item
        :param input_score: input value
        :return: normalized score [0,1]
        """
        item = [it for it in self.item_list if it['id'] == item_id][0]
        item_score_values = set(list(zip(*item['score']))[1])
        if input_score in item_score_values:
            return (input_score - min(item_score_values)) / (max(item_score_values) - min(item_score_values))
        else:
            raise ValueError(
                f"Input value for item {item_id} ({item['name']}) is {input_score} might be one of these: {item_score_values}")

    def get_weight(self, item_id: int) -> float:
        """
        Return the unnormalized weights
        :param item_id: id of the item
        :type item_id: int
        :return: unnormalized weights
        :rtype: float
        """
        return [it['weight'] for it in self.item_list if it['id'] == item_id][0]

    def get_weight_coeff(self, ) -> float:
        """
        Return the sum of weights
        :return: sum of unnormalized weights
        :rtype: weights
        """
        return sum([it['weight'] for it in self.item_list])

    def get_normalized_weight(self, item_id: int) -> list:
        """
        Return the normalized weights
        :param item_id: id of the item
        :type item_id: int
        :return:  normalized weights
        :rtype: list[float]
        """
        return [it['weight'] for it in self.item_list if it['id'] == item_id][0] / self.get_weight_coeff()

    def generate_data_idx(self, idx) -> tuple:
        """
        Return a generated value based on a given item index
        :param idx: id of the item
        :type idx: int
        :return: couple of values: (id of the item, score as if was returns from a questionnaire)
        :rtype: (int, float)
        """
        item = [item for item in self.item_list if item['id'] == idx][0]
        return item['id'], np.random.choice(list(zip(*item['score']))[1], 1, p=item['prob'])[0]

    def get_item_ids(self, ) -> list:
        """
        Return list of item ids
        :return: item ids
        :rtype: list[int]
        """
        return [item['id'] for item in self.item_list]

    def generate_data(self, missing=True):
        """
        Return a list of couples (idx, value) corresponding to the data of one day
        :parameter missing: if True then randomly set 2 missing values. Either None, or -1 or completely missing
        :type missing: bool
        :return: list of tuples [(idx0, value0), (idx1, value1), ...]
        """
        if missing:
            list_of_item_ids = self.get_item_ids()
            ids = np.random.choice(list_of_item_ids, 2, replace=False)
        L = []
        for item in self.item_list:
            if missing and (item['id'] in ids):
                choice = np.random.choice(3, 1)
                if choice == 0:
                    L.append((item['id'], None))
                elif choice == 1:
                    L.append((item['id'], -1))
                elif choice == 2:
                    pass  # Explicit code block to show that for choice == 2 we do not generate any data
            else:
                L.append((item['id'], np.random.choice(list(zip(*item['score']))[1], 1, p=item['prob'])[0]))
        return L

    def generate_empty_data(self) -> []:
        L = []
        for item in self.item_list:
            L.append( (item['id'], None) )
        return L

    def generate_data_basedOnPrior(self) -> []:
        """
        Return a list of couples (idx, value) corresponding to the data of one day
        based on prior probabilities
        :return: list of tuples [(idx0, value0), (idx1, value1), ...]
        """
        L = []
        for item in self.item_list:
            L.append( (item['id'],self.impute_missing(item['id'], method='prob')) )
        return L

    def impute_missing(self, idx, method='mean_nearest'):
        """
        Return a value imputed based on historical data

        :param idx: index of the item
        :type idx: int
        :param method: imputation method {'mean_nearest'|'mean'|'mode'|'prob'}
        :type method: str
        :return: (idx, value)
        :rtype: tuple[int, float]
        """
        if method.lower() == 'prob':
            # the value is drawn from a (discrete) probability distribution
            return self.generate_data_idx(idx)[1]
        elif method.lower() == 'mean':  # should not be used directly since it returns non integer values
            values_idx = self.get_value_history_idx(idx)
            if len(values_idx) == 0:
                print(f'Empty history for idx {idx}, generating data based on prior')
                return self.impute_missing(idx, method='prob')
            else:
                return np.mean(values_idx)
        elif method.lower() == 'mean_nearest':
            # compute the mean value and pick up the nearest possible value
            value_mean = self.impute_missing(idx, method='mean')
            possible_values = np.array(list(self.get_set_of_possible_values_idx(idx)))
            idx = (np.abs(possible_values - value_mean)).argmin()
            return possible_values[idx]
        elif method.lower() == 'mode':
            # compute the statistical mode of the historical data
            values_idx = self.get_value_history_idx(idx)
            if len(values_idx) == 0:
                print(f'Empty history for idx {idx}, generating data based on prior')
                return self.impute_missing(idx, method='prob')
            else:
                vals, counts = np.unique(values_idx, return_counts=True)
                return vals[np.argmax(counts)]
        else:
            raise ValueError(f"Unknown argument {method} imputation method")

    def get_value_history_idx(self, idx: int, imputed=True) -> list:
        """
        Return values from the previous day
        :param idx: index of the item
        :param imputed: if True, imputed data are returned. If false returns only raw data
        :type idx: int
        :type imputed: bool
        :return: list of past values
        :rtype: list[float]
        """
        if imputed:
            # Don't filter out imputed data
            data_prev_days = self.__data["data_prev_days"]
            values = [d['values'] for d in data_prev_days]
            values_idx = [val[1] for value in values for val in value if val[0] == idx]
        else:
            print(f"Option imputed=False not yet implemented, falling back to imputed=True")
            values_idx = self.get_value_history_idx(idx, imputed=True)
        return values_idx

    def get_set_of_possible_values_idx(self, idx: int) -> set:
        """
        return set of possible values (scores) for a given index
        :param idx: int
        :return: set
        """
        item = [it for it in self.item_list if it['id'] == idx][0]
        item_score_values = list(zip(*item['score']))[1]
        return set(item_score_values)

    def compute_score(self, values: list) -> tuple:
        """
        Return updated values (with data imputation), filling rate, filling status, and score
        :param values: list of couples (id, value)
        :type values: list[tuple[float]]
        :return: values, filled, score, filling_rate
        :rtype: list[tuple[float]], list[int], float, float
        """
        score = 0
        number_of_items = self.get_number_of_items()
        filled = []
        # Add missing entries
        ids_to_be_added = set(self.get_item_ids()) - set(it[0] for it in values)
        for idx in ids_to_be_added:
            values.append((idx, None))
        values.sort()  # not mandatory but help for debugging
        # loop over all items, impute missing and compute score
        for i, (idx, value) in enumerate(values):
            if (value is None) or (value < 0):
                value = self.impute_missing(idx)
                values[i] = (idx, value)  # update data after imputation
                filled.append((idx, 0))
            else:
                filled.append((idx, 1))
            score += self.get_normalized_score(idx, value) * self.get_weight(idx)
        score = score / self.get_weight_coeff()
        filling_rate = sum([it[1] for it in filled]) / number_of_items
        return values, filled, score, filling_rate

    def compute_prediction(self, data):
        """
        Return predicted score
        :param data:
        :return: predicted score
        """
        normalized_weights = []
        predicted_score = 0
        for i, (idx, value) in enumerate(data["data_current_day"]['values']):
            d = self.get_value_history_idx(idx)
            d.append(value)
            normalized_score = [self.get_normalized_score(idx, v) for v in d]
            normalized_score_trimmed = [n - 1e-6 if n == 1.0 else n for n in normalized_score]
            score_atanh = np.arctanh(normalized_score_trimmed)
            p = np.poly1d(np.polyfit(list(range(len(score_atanh))), score_atanh, 1))
            predicted_normalized_value = np.tanh(p(len(score_atanh)))
            normalized_weights.append(self.get_normalized_weight(idx))
            predicted_score += predicted_normalized_value * self.get_normalized_weight(idx)
        return predicted_score

    # def __set_values_current_day(self, ):  # might be useless
    #     self.__data["data_current_day"]['values'] = self.generate_data()

    def __update_data_current_day(self, raw_values, trust_mode='ari'):
        """
        Update score indicators contained in the data dictionary
        trust_mode can be geometric (geo) or arithmetic (ari)
        """
        values, filled, score, filling_rate = self.compute_score(raw_values)
        data = self.__data
        data["data_current_day"]['values'] = values
        data["data_current_day"]['filled'] = filled
        data["data_current_day"]['filling_rate'] = filling_rate
        data["data_current_day"]['score4today'] = score
        if len(data["trust_index_prev_days"]) == 0:
            if trust_mode=='geo':
                trust_index_prev = 1
            else:
                trust_index_prev = 6
        else:
            if trust_mode=='geo':
                trust_index_prev = np.prod(data["filling_rate_prev_days"])
            else:
                trust_index_prev = np.sum(data["filling_rate_prev_days"])
        if trust_mode=='geo':
            data["data_current_day"]['trust_index'] = filling_rate * trust_index_prev
        else:
            data["data_current_day"]['trust_index'] = (filling_rate + trust_index_prev) / (self.history_length + 1)
        if len(data["real_score_prev_days"]) > 0:
            score4tomorrow = self.compute_prediction(data)  # prediction better at the end
        else:
            score4tomorrow = score  # might be set to None ?
        data["data_current_day"]['score4tomorrow'] = score4tomorrow

    def __flush_current_day_v2(self, ):
        """
        Flush present data to the queue (past data)
        """
        data = self.__data
        if data["data_current_day"]["filled"]:
            data["data_prev_days"].append({"values": data["data_current_day"]['values'],
                                           "filled": data["data_current_day"]["filled"]})
            data["real_score_prev_days"].append(data["data_current_day"]["score4today"])
            data["pred_score_prev_days"].append(data["data_current_day"]["score4tomorrow"])
            data["trust_index_prev_days"].append(data["data_current_day"]["trust_index"])
            data["filling_rate_prev_days"].append(data["data_current_day"]["filling_rate"])
            data["data_current_day"]['values'] = []
            data["data_current_day"]['filled'] = []
            data["data_current_day"]['filling_rate'] = []
            data["data_current_day"]['score4today'] = []
            data["data_current_day"]['score4tomorrow'] = []
            data["data_current_day"]['trust_index'] = []
            print("Je flush les données")
            return
        else:
            if not all([not bool(data["data_current_day"][it]) for it in data["data_current_day"]]):
                print("Je peux pas flush!")
                raise ValueError("We are in a situation where data['data_current_day'] is partially filled and thus "
                                 "cannot be 'flushed'")
                return
        print("Je fais rien dans le flush")

    def __flush_current_day(self, ):
        """
        Flush present data to the queue (past data)
        """
        data = self.__data
        if all([bool(data["data_current_day"][it]) for it in data["data_current_day"]]):
            data["data_prev_days"].append({"values": data["data_current_day"]['values'],
                                           "filled": data["data_current_day"]["filled"]})
            data["real_score_prev_days"].append(data["data_current_day"]["score4today"])
            data["pred_score_prev_days"].append(data["data_current_day"]["score4tomorrow"])
            data["trust_index_prev_days"].append(data["data_current_day"]["trust_index"])
            data["filling_rate_prev_days"].append(data["data_current_day"]["filling_rate"])
            data["data_current_day"]['values'] = []
            data["data_current_day"]['filled'] = []
            data["data_current_day"]['filling_rate'] = []
            data["data_current_day"]['score4today'] = []
            data["data_current_day"]['score4tomorrow'] = []
            data["data_current_day"]['trust_index'] = []
            print("Je flush les données")
            return
        else:
            if not all([not bool(data["data_current_day"][it]) for it in data["data_current_day"]]):
                print("Je peux pas flush!")
                raise ValueError("We are in a situation where data['data_current_day'] is partially filled and thus "
                                 "cannot be 'flushed'")
                return
        print("Je fais rien dans le flush")

    def update(self, raw_values):
        """
        Update score based on raw input data
        :param raw_values: raw values
        """
        if not raw_values: #case when empty list is passed as argument
            raw_values = self.generate_empty_data()
        self.__flush_current_day_v2()
        self.__update_data_current_day(raw_values)

    def update_orig(self, raw_values):
        """
        Update score based on raw input data
        :param raw_values: raw values
        """
        self.__flush_current_day()
        self.__update_data_current_day(raw_values)

    @property
    def score4today(self, ):
        return self.data['data_current_day']['score4today']

    @property
    def score4tomorrow(self, ):
        return self.data['data_current_day']['score4tomorrow']

    @property
    def trustIndex(self, ):
        return self.data['data_current_day']['trust_index']

    @property
    def fillingRate(self, ):
        return self.data['data_current_day']['filling_rate']

    @property
    def data(self):
        return self.__data


