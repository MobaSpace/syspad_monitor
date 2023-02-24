import logging
import time
from threading import Thread
import datetime

import arrow
from dateutil import tz

from syspad_monitor.database_encry import SysPadDb_encry
from syspad_monitor.task import AlarmConst
from syspad_monitor.task.bedMonitor import BedMonitor


class DeviceMonitor(Thread):
    """
    Cette classe permet de monitorer si les données des personnes qui sont
    surveillés par des IoT sont
    """
    # par défaut 30 minutes de sleep du Thread pour vérifier si les dispositifs sont OK
    REFRESH_TIME = 1800

    def __init__(self, bd: SysPadDb_encry):
        self.__thread_name = f"DEVICES MONITOR"
        Thread.__init__(self, name=self.__thread_name, target=self.run)
        # attribut nécessaire pour mettre à jour les objets PANDAS contenant les données du patient
        self.__bd = bd
        self.stop = False

    def run(self):
        logging.info(
            f"DEVICES-MONITOR --> Process has been started"
        )
        while not self.stop:
            for patId in self.__bd.read_pat_id_from_apis():
                pat_id = int(patId["PatientId"])
                dict_patient = self.__bd.read_one_patient(pat_id)
                chambre = dict_patient["Chambre"]
                numCh = dict_patient["NumCh"]
                events_bed = self.__bd.get_last_bed_events(pat_id)
                # au moins 1 des deux événements existe autrement on considère que le tapis est OK car jamais vu
                if not events_bed[0][0] and not events_bed[0][1]:
                    last_event = datetime.datetime(2100, 1, 1, 0, 0, 0) # une date dans le futur!!!
                elif not events_bed[0][0]:
                    last_event = events_bed[0][1]
                elif not events_bed[0][1]:
                    last_event = events_bed[0][0]
                else:
                    last_event = max(events_bed[0][0], events_bed[0][1])
                sensor = self.__bd.read_sensors(patient_id=pat_id, type="Sleep Monitor")
                if last_event < datetime.datetime.now() - datetime.timedelta(hours=12):
                    if sensor and sensor[0]["EtatOK"]:
                        id_sensor = sensor[0]["Id"]
                        new_alarm = dict(
                            {
                                "id_patient": pat_id,
                                "id_capteur": id_sensor,
                                "priorite": AlarmConst.MEDIUM.value,  # 100-75 haute, 75-50 moyenne, 50-25 basse, 25-0 informative
                                "desc": f"Possible défaut du tapis",
                            }
                        )
                        logging.error(f"DEVICE-MONITOR ===> Alarm: Sleep Monitor linked to pat={pat_id} is KO!!")
                        self.__bd.set_sensor_state(id_sensor, False)
                        # also put NULL on patient posture because not more knonw and this will stop day counter
                        self.__bd.update_patient_posture(patient_id=pat_id, posture=None)
                        self.__bd.insert_alarm(alarm=new_alarm)
                else:
                    if sensor:
                        id_sensor = sensor[0]["Id"]
                        self.__bd.set_sensor_state(id_sensor, True)
                logging.debug(f"DEVICE-MONITOR --> Device Sleep Monitor checked for patient {pat_id}")

                # verifier que l'API est aussi bien renouvellée
                api = self.__bd.read_patient_api(patient_id=pat_id)
                if api and api[0]["ExpirationDate"] < datetime.datetime.now() and api[0]["Provider"] == 'Withings':
                    if sensor and api[0]["EtatOK"]:
                        id_sensor = sensor[0]["Id"]
                        id_api = api[0]["Id"]
                        new_alarm = dict(
                            {
                                "id_patient": pat_id,
                                "id_capteur": id_sensor,
                                "priorite": AlarmConst.MEDIUM.value,  # 100-75 haute, 75-50 moyenne, 50-25 basse, 25-0 informative
                                "desc": f"Défaut communication API",
                            }
                        )
                        logging.error(f"DEVICE-MONITOR ===> Alarm: API for pat={pat_id} is KO!!")
                        self.__bd.set_api_state(id_api, False)
                        self.__bd.insert_alarm(alarm=new_alarm)
                elif api and api[0]["ExpirationDate"] > datetime.datetime.now() and not api[0]["EtatOK"]:
                    id_api = api[0]["Id"]
                    self.__bd.set_api_state(id_api, True)

                # vérification des Trackers pour ce patient
                sensor=self.__bd.read_sensors(patient_id=pat_id, type="Tracker")
                if sensor:
                    sensor_id = sensor[0]["Id"]
                    tracker = self.__bd.get_tracker_lastUpdate(capteurId=sensor_id)[0]
                    h1 = int(dict_patient["Coucher_h"] or 0) * 3600 + int(dict_patient["Coucher_min"] or 0) * 60
                    h2 = int(dict_patient["Lever_h"] or 0) * 3600 + int(dict_patient["Lever_min"] or 0) * 60

                    # vérifier si nous sommes dans la plage de surveillance diurne
                    time_now = arrow.now(tz=tz.gettz("Paris/Europe"))
                    secs_on_day = (
                            time_now.datetime.hour * 3600
                            + time_now.datetime.minute * 60
                            + time_now.datetime.second
                    )
                    if not BedMonitor.is_time_on_interval(h1, h2, secs_on_day): # so that we check only during the day
                        if tracker["LastUpdate"] < datetime.datetime.now() - datetime.timedelta(hours=2):
                            if sensor[0]["EtatOK"]:
                                new_alarm = dict(
                                    {
                                        "id_patient": pat_id,
                                        "id_capteur": sensor_id,
                                        "priorite": AlarmConst.MEDIUM.value,  # 100-75 haute, 75-50 moyenne, 50-25 basse, 25-0 informative
                                        "desc": f"Défaut communication dispositif MoBy",
                                    }
                                )
                                logging.error(f"DEVICE-MONITOR ===> Alarm: Tracker linked to pat={pat_id} is KO!!")
                                self.__bd.set_sensor_state(sensor_id=sensor_id, state=False)
                                self.__bd.insert_alarm(alarm=new_alarm)
                        else:
                            self.__bd.set_sensor_state(sensor_id=sensor_id, state=True)

                    logging.debug(f"DEVICE-MONITOR --> Device Tracker checked for patient {pat_id}")

            time.sleep(self.REFRESH_TIME)
