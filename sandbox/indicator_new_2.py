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
    The Score class !!
    """

    def __init__(self, history_length=6, item_list=None):
        self.history_length = history_length
        self.__data = self.get_empty_data()
        if item_list is None:
            item_list = slurpGraph(items_path)
        self.item_list = item_list

    def get_empty_data(self, ):
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

    def get_number_of_items(self, ):
        """
        Return number of items
        :return: number of items
        """
        return len(self.item_list)

    def get_normalized_score(self, item_id, input_score):
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

    def get_weight(self, item_id):
        """
        Return the unnormalized weights
        :param item_id:
        :return:
        """
        return [it['weight'] for it in self.item_list if it['id'] == item_id][0]

    def get_weight_coeff(self, ):
        """
        Return the sum of weights
        :return:
        """
        return sum([it['weight'] for it in self.item_list])

    def get_normalized_weight(self, item_id):
        """
        Return the normalized weights
        :param item_id:
        :return:
        """
        return [it['weight'] for it in self.item_list if it['id'] == item_id][0] / self.get_weight_coeff()

    def generate_data_idx(self, idx):
        """
        Return a generated value based on a given item index
        :param idx:
        :return:
        """
        item = [item for item in self.item_list if item['id'] == idx][0]
        return item['id'], np.random.choice(list(zip(*item['score']))[1], 1, p=item['prob'])[0]

    def get_item_ids(self, ):
        """
        Return list of item ids
        :return: item ids
        """
        return [item['id'] for item in self.item_list]

    def generate_data(self, missing=True):
        """
        Return a list of couples (idx, value) corresponding to the data of one day
        :parameter missing: if True then randomly set 2 missing values. Either None, or -1 or completly missing
        :return: list of tuples (idx, value)
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

    def impute_missing(self, idx, method='mean_nearest'):
        """
        Return a value imputed based on historical data
        [At the moment just randomly generated, true imputation should be implemented]
        :param idx: index of the item
        :return: (idx, value)
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
            value_mean = self.impute_missing(idx, method='mean')
            possible_values = np.array(list(self.get_set_of_possible_values_idx(idx)))
            idx = (np.abs(possible_values - value_mean)).argmin()
            return possible_values[idx]
        elif method.lower() == 'mode':
            values_idx = self.get_value_history_idx(idx)
            if len(values_idx) == 0:
                print(f'Empty history for idx {idx}, generating data based on prior')
                return self.impute_missing(idx, method='prob')
            else:
                vals, counts = np.unique(values_idx, return_counts=True)
                return vals[np.argmax(counts)]
        else:
            raise ValueError(f"Unknown argument {method} imputation method")

    def get_value_history_idx(self, idx, imputed=True):
        if imputed:
            # Don't filter out imputed data
            data_prev_days = self.__data["data_prev_days"]
            values = [d['values'] for d in data_prev_days]
            values_idx = [val[1] for value in values for val in value if val[0] == idx]
        else:
            print(f"Option imputed=False not yet implemented, falling back to imputed=True")
            values_idx = self.get_value_history_idx(idx, imputed=True)
        return values_idx

    def get_set_of_possible_values_idx(self, idx):
        item = [it for it in self.item_list if it['id'] == idx][0]
        item_score_values = list(zip(*item['score']))[1]
        return set(item_score_values)

    def compute_score(self, values):
        """
        Return updates values (imputed), filling rate, filling status, and score for gioven values
        :param values: list of couples (id, value)
        :return: values, filled, score, filling_rate
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

    def __update_data_current_day(self, raw_values):
        """
        Compute global score, filling rate and update input data
        :return:
        """
        values, filled, score, filling_rate = self.compute_score(raw_values)
        data = self.__data
        data["data_current_day"]['values'] = values
        data["data_current_day"]['filled'] = filled
        data["data_current_day"]['filling_rate'] = filling_rate
        data["data_current_day"]['score4today'] = score
        if len(data["trust_index_prev_days"]) == 0:
            trust_index_prev = 1
        else:
            trust_index_prev = np.prod(data["filling_rate_prev_days"])
        data["data_current_day"]['trust_index'] = filling_rate * trust_index_prev
        if len(data["real_score_prev_days"]) > 0:
            score4tomorrow = self.compute_prediction(data)  # prediction better at the end
        else:
            score4tomorrow = score  # might be set to None ?
        data["data_current_day"]['score4tomorrow'] = score4tomorrow

    def __flush_current_day(self, ):
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
        else:
            if not all([not bool(data["data_current_day"][it]) for it in data["data_current_day"]]):
                raise ValueError("We are in a situation where data['data_current_day'] is partially filled and thus "
                                 "cannot be 'flushed'")

    def update(self, raw_values):
        self.__flush_current_day()
        self.__update_data_current_day(raw_values)

    @property
    def score4today(self, ):
        return self.data['data_current_day']['score4today']

    @property
    def score4tomorrow(self, ):
        return self.data['data_current_day']['score4tomorrow']

    @property
    def trust_index(self, ):
        return self.data['data_current_day']['trust_index']

    @property
    def data(self):
        return self.__data



if __name__ == "__main__":
    score = Score()
    print(score.data)
    print(score.score4today)
    score.update(score.generate_data())
    print(score.data)
    print(score.score4today)
    score.update(score.generate_data())
    print(score.data)
    print(score.score4today)
    score.update(score.generate_data())
    print(score.data)
    print(score.score4today)
