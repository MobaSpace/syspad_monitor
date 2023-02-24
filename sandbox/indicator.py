from sandbox.score import IndicatorScorer
from abc import abstractmethod, ABC


class DailyIndicator:
    unit = None

    def __init__(self, values, scores, input_type, day, data, confidence, impact, description):
        self._scorer = IndicatorScorer(values, scores, input_type)  # thresholds defining the bin edges
        self.day = day
        self.data = data
        self.name = description
        self.confidence = confidence
        self.impact = impact
        self.confidence_dict = {'very low': 0.1, 'low': 0.3, 'medium': 0.6, 'high': 1}
        self.impact_dict = {'low': 0.2, 'medium': 0.6, 'high': 1}

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, val):
        self._data = self.validate_data(val)

    @property
    def confidence_weight(self):
        return type(self)._get_val_from_str(self.confidence_dict, self.confidence)

    @property
    def impact_weight(self):
        return type(self)._get_val_from_str(self.impact_dict, self.impact)

    @staticmethod
    def _get_val_from_str(input_dict, str_val):
        if str_val is None:
            val = 1
        elif isinstance(str_val, str):
            val = input_dict[str_val]
        else:
            val = str_val
        return val

    @staticmethod
    def validate_data(data):
        try:
            data[0]
        except TypeError as te:
            return data
        else:
            raise TypeError("data should be a numeric value not an iterable")

    @property
    def unweighted_score(self):
        return self._scorer.get_score(self.data)

    @property
    def weighted_score(self):
        return self.unweighted_score * self.impact_weight


class NightSleepIndicator(DailyIndicator):
    unit = "hour"

    def __init__(self,
                 values=(2, 4, 6, 8, 10),
                 scores=(0, 1, 2, 3, 4, 5),
                 input_type='edges',
                 day=None,
                 description='Sleep Time during night',
                 data=None,
                 confidence='high',
                 impact='high'):
        super().__init__(values, scores, input_type, day, data, confidence, impact, description)


class DaySleepIndicator(DailyIndicator):
    unit = "hour"

    def __init__(self,
                 values=(1, 2, 3, 4),
                 scores=(0, 1, 2, 3, 4),
                 input_type='edges',
                 day=None,
                 description='Sleep Time during day',
                 data=None,
                 confidence='high',
                 impact='medium'):
        super().__init__(values, scores, input_type, day, data, confidence, impact, description)


class WakeupIndicator(DailyIndicator):
    unit = "count"

    def __init__(self,
                 values=(2, 4, 6, 8),
                 scores=(4, 3, 2, 1, 0),
                 input_type='edges',
                 day=None,
                 description='Number of wake-ups during night',
                 data=None,
                 confidence='high',
                 impact='medium'):
        super().__init__(values, scores, input_type, day, data, confidence, impact, description)


class MealIndicator(DailyIndicator):
    unit = "ratio"

    def __init__(self,
                 values=(0, 0.5, 0.75, 1),
                 scores=(0, 1, 2, 3),
                 input_type='values',
                 day=None,
                 description='Part of meal already taken',
                 data=None,
                 confidence='high',
                 impact='high'):
        super().__init__(values, scores, input_type, day, data, confidence, impact, description)


class WalkSpeed(DailyIndicator):
    unit = "km/h"

    def __init__(self,
                 values=(0.7, 1, 1.5, 2),
                 scores=(0, 1, 2, 3, 4),
                 input_type='edges',
                 day=None,
                 description='Walking speed',
                 data=None,
                 confidence='very low',
                 impact='high'):
        super().__init__(values, scores, input_type, day, data, confidence, impact, description)


class WalkDistance(DailyIndicator):
    unit = "km"

    def __init__(self,
                 values=(0.1, 0.5, 1, 3),
                 scores=(0, 1, 2, 3, 4),
                 input_type='edges',
                 day=None,
                 description='Walking speed',
                 data=None,
                 confidence='very low',
                 impact='high'):
        super().__init__(values, scores, input_type, day, data, confidence, impact, description)


class Steps(DailyIndicator):
    unit = "count"

    def __init__(self,
                 values=(100, 200, 300, 400),
                 scores=(0, 1, 2, 3, 4),
                 input_type='edges',
                 day=None,
                 description='Walking speed',
                 data=None,
                 confidence='very low',
                 impact='high'):
        super().__init__(values, scores, input_type, day, data, confidence, impact, description)


class Scorer:
    def __init__(self, indicators):
        self.indicators = indicators

    def get_score(self):
        score = 0
        for indicator in self.indicators:
            score += indicator.weighted_score
        return score

    def get_unweighted_score(self):
        score = 0
        for indicator in self.indicators:
            score += indicator.unweighted_score
        return score


if __name__ == '__main__':
    # Initialize indicators
    night_sleep_duration = NightSleepIndicator(data=5)
    day_sleep_duration = DaySleepIndicator(data=2)
    number_of_wakeups = WakeupIndicator(data=5)
    number_of_meals = MealIndicator(data=0.75)
    walking_speed = WalkSpeed(data=1.7)
    walking_distance = WalkDistance(data=0.4)
    number_of_steps = Steps(data=500)

    # assign values
    print(night_sleep_duration.unweighted_score)
    print(night_sleep_duration.weighted_score)
    scorer = Scorer([night_sleep_duration, day_sleep_duration, number_of_wakeups, number_of_meals, walking_speed,
                     walking_distance, number_of_steps])
    print(scorer.get_unweighted_score(), scorer.get_score())
