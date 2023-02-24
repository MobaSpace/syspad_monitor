from datetime import datetime, timezone, timedelta
import time
import pprint


class DataGenerator:
    def __init__(self, start_date):
        self.start_date = start_date


def parsetime(t):
    try:
        return datetime.strptime(t, '%d/%m/%Y %H:%M:%S')
    except:
        pass
    try:
        return datetime.strptime(t, '%d/%m/%y %H:%M:%S')
    except:
        pass
    try:
        return datetime.strptime(t, '%d/%m/%Y')
    except:
        pass
    try:
        return datetime.strptime(t, '%d/%m/%y')
    except:
        pass
    try:
        return datetime.strptime(t, '%H:%M:%S')
    except:
        pass
    return t


def parsedeltatime(dT):
    try:
        dT = datetime.strptime(dT, '%H:%M:%S') - datetime.strptime('00:00:00', '%H:%M:%S')
    except ValueError as ve:
        pass
    return dT


def data_stream(start_date=None, deltaT=None, real_time=False, max_simulation_time=1.):
    """
    indicator data stream
    :param max_simulation_time: Maximum simulation time in days
    :param start_date: start date
    :param real_time: {True | False | float} If True simulate real_time process, if false
    :param deltaT: the time between two data point
    :return: date, obj
    """
    if start_date is None:
        start_date = datetime.now(timezone.utc)
    else:
        start_date = parsetime(start_date)
    date = start_date
    if deltaT is None:
        deltaT = timedelta(hours=1)
    else:
        deltaT = datetime.strptime(deltaT, '%H:%M:%S') - datetime.strptime('00:00:00', '%H:%M:%S')
    if isinstance(real_time, bool):
        if real_time:
            delay = deltaT.seconds
        else:
            delay = None
    else:
        delay = deltaT
    max_simulation_time = timedelta(days=float(max_simulation_time))
    while True:
        date += deltaT
        if date - start_date > max_simulation_time:
            print(date - start_date - deltaT)
            break
        if delay is not None:
            time.sleep(delay)
        obj = None
        tag = None
        yield date, obj, tag


from numpy.random import choice


class Item:
    instances = []

    def __init__(self, tag, name, description, list_of_candidates, probability_distribution):
        self.list_of_candidates = list_of_candidates
        self.probability_distribution = probability_distribution
        type(self).instances.append(self)

    def draw(self, number_of_items_to_pick=1):
        return choice(self.list_of_candidates, number_of_items_to_pick, p=self.probability_distribution)


class Simulator:
    def __init__(self, delay=None, deltaT='01:00:00', real_time=False, max_simulation_time=30):
        self.dataStream = data_stream(deltaT=deltaT, real_time=real_time, max_simulation_time=max_simulation_time)
        self.delay = delay

    def run(self):
        """
        Run de data stream
        :return:
        """
        for d in self.dataStream:
            print(d)
            if self.delay is not None:
                time.sleep(self.delay)


if __name__ == '__main__':
    # ds = data_stream()
    # for d in ds:
    #     print(d)

    simulator = Simulator()
    simulator.run()
