import pprint
import numpy as np
from collections import deque

# import random
# random.seed(0)
# np.random.seed(seed=0)

item_lod = [  # items / list of dictionaries
    {
        'id': 0,
        'name': 'douleur',
        'description': 'semble être ou dit être douloureux',
        'score': (('oui', 0), ('non', 4)),
        'weight': 30,
        'prob': (0.25, 0.75)
    },
    {
        'id': 1,
        'name': 'tristesse',
        'description': 'semble être ou dit être triste',
        'score': (('oui', 2), ('non', 4)),
        'weight': 30,
        'prob': (0.15, 0.85)
    },
    {
        'id': 2,
        'name': 'fièvre',
        'description': 'semble être ou dit être fiévreux',
        'score': (('oui', 0), ('non', 4)),
        'weight': 50,
        'prob': (0.05, 0.95)
    },
    {
        'id': 3,
        'name': 'agitation',
        'description': 'Agité et /ou agressif',
        'score': (('oui', 0), ('non', 4)),
        'weight': 60,
        'prob': (0.25, 0.75)
    },
    {
        'id': 4,
        'name': 'fatigue',
        'description': 'semble être ou dit être fatigué',
        'score': (('fatigue intense', 0), ('fatigue moyenne', 2), ('fatigue légère', 3), ('pas de fatigue', 4)),
        'weight': 20,
        'prob': (0.05, 0.15, 0.55, 0.25)
    },
    {
        'id': 5,
        'name': 'déplacement',
        'description': 'déplacement dans la journée qui vient de passer',
        'score': (('0 pas', 0), ('quelques pas en chambre', 1), ('a marché hors de la chambre', 3)),
        'weight': 40,
        'prob': (0.05, 0.25, 0.7)
    },
    {
        'id': 6,
        'name': 'selles_quantité',
        'description': 'selles quantité',
        'score': (('0 croix', 0), ('1 croix', 1), ('2 croix', 2), ('3 croix', 3)),
        'weight': 40,
        'prob': (0.05, 0.15, 0.45, 0.35)
    },
    {
        'id': 7,
        'name': 'selles_texture',
        'description': 'selles texture',
        'score': (('dures', 0), ('liquides', 0), ('normales/molles', 4)),
        'weight': 30,
        'prob': (0.2, 0.05, 0.75)
    },
    {
        'id': 8,
        'name': 'sommeil',
        'description': 'sommeil déclaré par le résident ou constaté par soignant',
        'score': (('mauvais ', 1), ('moyen ', 2), ('bon', 3)),
        'weight': 50,
        'prob': (0.05, 0.75, 0.2)
    },
    {
        'id': 9,
        'name': 'appétit',
        'description': 'appétit : part des repas du jour (récolté dans netsoins si on compte sur la RV)',
        'score': (('0 ', 0), ('1/2', 2), ('3/4', 3), ('tout', 4)),
        'weight': 60,
        'prob': (0.0, 0.1, 0.55, 0.35)
    },
    {
        'id': 10,
        'name': 'hydratation',
        'description': 'hydratation orale du jour (récolté ds netsoins si on compte sur la RV)',
        'score': (('0 ', 0), ('6 ou 7 verres', 1), ('8 ou 9 verres', 3), ('10 verres ou plus', 4)),
        'weight': 80,
        'prob': (0.01, 0.1, 0.75, 0.14)
    },
    {
        'id': 11,
        'name': 'fall',
        'description': 'chute au cours des 7 derniers jours (récolté dans netsoins)',
        'score': (('oui', 0), ('non', 4)),
        'weight': 100,
        'prob': (0.01, 0.99)
    }
]


def get_empty_data(history_length=None):
    if history_length is None:
        history_length = 6
    return {'data_current_day': {'values': [],
                                 'filled': [],
                                 'filling_rate': [],
                                 'score4today': [],
                                 'score4tomorrow': [],
                                 'trust_index': []},
            'data_prev_days': deque(maxlen=history_length),
            'real_score_prev_days': deque(maxlen=history_length),
            'pred_score_prev_days': deque(maxlen=history_length),
            'trust_index_prev_days': deque(maxlen=history_length),
            'filling_rate_prev_days': deque(maxlen=history_length)}


# data_out = {
#     "score4today": 75,
#     "score4tomorrow": 52,
#     "trust_index": 25
# }

def get_number_of_items():
    """
    Return number of items
    :return: number of items
    """
    return len(item_lod)


def get_normalized_score(item_id, input_score):
    """
    Return normalized score such that score <- (score - min(score_list))/(max(score_list)- min(score_list))
    :param item_id: id of the item
    :param input_score: input value
    :return: normalized score [0,1]
    """
    item = [it for it in item_lod if it['id'] == item_id][0]
    item_score_values = set(list(zip(*item['score']))[1])
    if input_score in item_score_values:
        return (input_score - min(item_score_values)) / (max(item_score_values) - min(item_score_values))
    else:
        raise ValueError(
            f"Input value for item {item_id} ({item['name']}) is {input_score} might be one of these: {item_score_values}")


def get_weight(item_id):
    """
    Return the unnormalized weights
    :param item_id:
    :return:
    """
    return [it['weight'] for it in item_lod if it['id'] == item_id][0]


def get_weight_coeff():
    """
    Return the sum of weights
    :return:
    """
    return sum([it['weight'] for it in item_lod])


def get_normalized_weight(item_id):
    """
    Return the normalized weights
    :param item_id:
    :return:
    """
    return [it['weight'] for it in item_lod if it['id'] == item_id][0] / get_weight_coeff()


def generate_data_idx(idx):
    """
    Return a generated value based on a given item index
    :param idx:
    :return:
    """
    item = [item for item in item_lod if item['id'] == idx][0]
    return item['id'], np.random.choice(list(zip(*item['score']))[1], 1, p=item['prob'])[0]


def get_item_ids():
    """
    Return list of item ids
    :return: item ids
    """
    return [item['id'] for item in item_lod]


def generate_data(missing=True):
    """
    Return a list of couples (idx, value) corresponding to the data of one day
    :parameter missing: if True then randomly set 2 missing values. Either None, or -1 or completly missing
    :return: list of tuples (idx, value)
    """
    if missing:
        list_of_item_ids = get_item_ids()
        ids = np.random.choice(list_of_item_ids, 2, replace=False)
    L = []
    for item in item_lod:
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


def impute_missing(idx, data, method='mean_nearest'):
    """
    Return a value imputed based on historical data
    [At the moment just randomly generated, true imputation should be implemented]
    :param idx: index of the item
    :return: (idx, value)
    """
    if method.lower() == 'prob':
        # the value is drawn from a (discrete) probability distribution
        return generate_data_idx(idx)[1]
    elif method.lower() == 'mean':  # should not be used directly since it returns non integer values
        values_idx = get_value_history_idx(idx, data)
        if len(values_idx) == 0:
            print(f'Empty history for idx {idx}, generating data based on prior')
            return impute_missing(idx, data, method='prob')
        else:
            return np.mean(values_idx)
    elif method.lower() == 'mean_nearest':
        value_mean = impute_missing(idx, data, method='mean')
        possible_values = np.array(list(get_set_of_possible_values_idx(idx)))
        idx = (np.abs(possible_values - value_mean)).argmin()
        return possible_values[idx]
    elif method.lower() == 'mode':
        values_idx = get_value_history_idx(idx, data)
        if len(values_idx) == 0:
            print(f'Empty history for idx {idx}, generating data based on prior')
            return impute_missing(idx, method='prob')
        else:
            vals, counts = np.unique(values_idx, return_counts=True)
            return vals[np.argmax(counts)]
    else:
        raise ValueError(f"Unknown argument {method} imputation method")


def get_value_history_idx(idx, data, imputed=True):
    if imputed:
        # Don't filter out imputed data
        data_prev_days = data["data_prev_days"]
        values = [d['values'] for d in data_prev_days]
        values_idx = [val[1] for value in values for val in value if val[0] == idx]
    else:
        print(f"Option imputed=False not yet implemented, falling back to imputed=True")
        values_idx = get_value_history_idx(idx, data, imputed=True)
    return values_idx


def get_set_of_possible_values_idx(idx):
    item = [it for it in item_lod if it['id'] == idx][0]
    item_score_values = list(zip(*item['score']))[1]
    return set(item_score_values)


def compute_score(data):
    """
    Return updates values (imputed), filling rate, filling status, and score for gioven values
    :param values: list of couples (id, value)
    :return: values, filled, score, filling_rate
    """
    values = data["data_current_day"]['values']
    score = 0
    number_of_items = get_number_of_items()
    filled = []
    # Add missing entries
    ids_to_be_added = set(get_item_ids()) - set(it[0] for it in values)
    for idx in ids_to_be_added:
        values.append((idx, None))
    values.sort()  # not mandatory but help for debugging
    # loop over all items, impute missing and compute score
    for i, (idx, value) in enumerate(values):
        if (value is None) or (value < 0):
            value = impute_missing(idx, data)
            values[i] = (idx, value)  # update data after imputation
            filled.append((idx, 0))
        else:
            filled.append((idx, 1))
        score += get_normalized_score(idx, value) * get_weight(idx)
    score = score / get_weight_coeff()
    filling_rate = sum([it[1] for it in filled]) / number_of_items
    return values, filled, score, filling_rate


def compute_prediction(data):
    normalized_weights = []
    predicted_score = 0
    for i, (idx, value) in enumerate(data["data_current_day"]['values']):
        d = get_value_history_idx(idx, data)
        d.append(value)
        normalized_score = [get_normalized_score(idx, v) for v in d]
        normalized_score_trimmed = [n - 1e-6 if n == 1.0 else n for n in normalized_score]
        score_atanh = np.arctanh(normalized_score_trimmed)
        p = np.poly1d(np.polyfit(list(range(len(score_atanh))), score_atanh, 1))
        predicted_normalized_value = np.tanh(p(len(score_atanh)))
        normalized_weights.append(get_normalized_weight(idx))
        predicted_score += predicted_normalized_value * get_normalized_weight(idx)
    return predicted_score


def set_values_current_day(data):
    data["data_current_day"]['values'] = generate_data()
    return data


def update_data_current_day(data):
    """
    Compute global score, filling rate and update input data
    :return:
    """
    values, filled, score, filling_rate = compute_score(data)
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
        score4tomorrow = compute_prediction(data)  # prediction better at the end
    else:
        score4tomorrow = score  # might be set to None ?
    data["data_current_day"]['score4tomorrow'] = score4tomorrow
    return data


def flush_current_day(data):
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
    return data


def runTest(N):
    print('\n========\nDATABASE\n========\n')
    pprint.pprint(item_lod, indent=2)
    history_length = 6
    data = get_empty_data(history_length)
    print('\n==========\nEMPTY DATA\n==========\n')
    pprint.pprint(data, indent=2)

    # N days simulation
    for d in range(N):
        if d > 0:  # Flush data only after the 1st day
            print('\n==========\nFLUSH DATA\n==========\n')
            data = flush_current_day(data)
            pprint.pprint(data, indent=2)
        print('\n============\nGET RAW DATA\n============\n')
        data = set_values_current_day(data)
        pprint.pprint(data, indent=2)
        print('\n===========\nUPDATE DATA\n===========\n')
        data = update_data_current_day(data)
        pprint.pprint(data, indent=2)
        print('\n======================\nPRINT DATA OUT\n======================\n')
        print(f'score4today: {data["data_current_day"]["score4today"]:.3f}')
        print(f'score4tomorrow: {data["data_current_day"]["score4tomorrow"]:.3f}')
        print(f'filling rate: {data["data_current_day"]["filling_rate"]:.3f}')
        print(f'trust index: {data["data_current_day"]["trust_index"]:.3f}')


def run(N):
    pprint.pprint(item_lod, indent=2)
    history_length = 6
    data = get_empty_data(history_length)
    # N days simulation
    for d in range(N):
        if d > 0:  # Flush data only after the 1st day
            data = flush_current_day(data)
        data = set_values_current_day(data)
        data = update_data_current_day(data)


if __name__ == '__main__':
    # print('\n========\nDATABASE\n========\n')
    # pprint.pprint(item_lod, indent=2)
    # history_length = 6
    # data = get_empty_data(history_length)
    # print('\n==========\nEMPTY DATA\n==========\n')
    # pprint.pprint(data, indent=2)
    #
    # # N days simulation
    # N = 20
    # for d in range(N):
    #     if d > 0:  # Flush data only after the 1st day
    #         print('\n==========\nFLUSH DATA\n==========\n')
    #         data = flush_current_day(data)
    #         pprint.pprint(data, indent=2)
    #     print('\n============\nGET RAW DATA\n============\n')
    #     data = set_values_current_day(data)
    #     pprint.pprint(data, indent=2)
    #     print('\n===========\nUPDATE DATA\n===========\n')
    #     data = update_data_current_day(data)
    #     pprint.pprint(data, indent=2)
    #     print('\n======================\nPRINT DATA OUT\n======================\n')
    #     print(f'score4today: {data["data_current_day"]["score4today"]:.3f}')
    #     print(f'score4tomorrow: {data["data_current_day"]["score4tomorrow"]:.3f}')
    #     print(f'filling rate: {data["data_current_day"]["filling_rate"]:.3f}')
    #     print(f'trust index: {data["data_current_day"]["trust_index"]:.3f}')
    #     # print('\n==========\nFLUSH DATA\n==========\n')
    #     # data = flush_current_day(data)
    #     # pprint.pprint(data, indent=2)


    runTest(10)
