# coding: utf8

import logging
import time
from threading import Thread, Timer

from dateutil import tz
import arrow

from syspad_monitor.database_encry import SysPadDb_encry
from syspad_monitor.task import PatientConst, AlarmConst


class BedMonitor(Thread):
    # par défaut 10 secondes de sleep du Thread pour vérifier si nouvelles données
    REFRESH_TIME = 10

    def __init__(self, bd: SysPadDb_encry):
        self.__bd = bd
        self.__thread_name = f"BED MONITOR"
        Thread.__init__(self, name=self.__thread_name, target=self.run)
        self.__myTimers = dict()
        self.__myCounters = dict()
        self.stop = False

    def run(self):
        logging.info(f"BED MONITOR --> Started")

        while not self.stop:
            for patient_dict in self.__bd.read_all_patients():
                pat_id = int(patient_dict["Id"])
                numCh = patient_dict["NumCh"]
                nomChambre = patient_dict["Chambre"]
                chambre = str(numCh) + " " + nomChambre

                # pour rappel h1 est un INT avec les secondes après 0h
                h1 = int(patient_dict["Coucher_h"] or 0) * 3600 + int(patient_dict["Coucher_min"] or 0) * 60
                h2 = int(patient_dict["Lever_h"] or 0) * 3600 + int(patient_dict["Lever_min"] or 0) * 60
                max_time_out_bed = int(patient_dict["DureeMaxHorsLit_min"] or 0) * 60
                # vérifier si nous sommes dans la plage de surveillance
                time_now = arrow.now(tz=tz.gettz("Paris/Europe"))
                secs_on_day = (
                        time_now.datetime.hour * 3600
                        + time_now.datetime.minute * 60
                        + time_now.datetime.second
                )
                # surveillance de nuit
                if self.is_time_on_interval(h1, h2, secs_on_day):
                    events_bed = self.__bd.get_last_bed_events(pat_id)
                    last_bed_in = events_bed[0][0]
                    last_bed_out = events_bed[0][1]
                    new_event = events_bed[0][2]
                    # first time they can be "None" or flag is already False
                    if last_bed_out and last_bed_in and new_event:
                        self.__bd.change_patient_flag(
                            pat_id, False, PatientConst.bed_data.value
                        )
                        if last_bed_in < last_bed_out:
                            # launch Timer if it is stopped
                            if pat_id not in self.__myTimers:
                                logging.info(
                                    f"TIMER ===> Countdown STARTED for Pat={pat_id}"
                                )
                                self.__myTimers[pat_id] = MyTimer(self.__bd, pat_id, chambre, max_time_out_bed)
                                self.__myTimers[pat_id].start()

                        elif pat_id in self.__myTimers:
                            logging.info(
                                f"TIMER ===> Countdown STOPPED for Pat={pat_id} because BED_IN > BED_OUT"
                            )
                            self.__myTimers[pat_id].cancel()
                            del self.__myTimers[pat_id]
                    else:
                        if pat_id in self.__myTimers and not self.__myTimers[pat_id].is_runing():
                            logging.info(
                                f"TIMER ===> Countdown REACHED!!! Timer Object deleted for pat={pat_id}"
                            )
                            del self.__myTimers[pat_id]
                        logging.debug(
                            f"BED_MONITOR --> Any new BED event for Pat={pat_id} in H1-H2"
                        )
                    if patient_dict["CumulTempsAllonge"] and int(patient_dict["CumulTempsAllonge"]) != 0:
                        self.__bd.update_patient_lying_day_time(pat_id, 0)
                        if int(patient_dict["CumulTempsAllonge"]) < 0:
                            patient_dict["CumulTempsAllonge"] = int(patient_dict["CumulTempsAllonge"]) + 86400
                        # insert the day on db, hypothesis that h1 is always before midnight
                        self.__bd.insert_day_total_lying_time(patient=pat_id,
                                                              date_jour=arrow.now().format('YYYY-MM-DD'),
                                                              temps_allonge_total=int(patient_dict["CumulTempsAllonge"])
                                                              )
                        if pat_id in self.__myCounters:
                            del self.__myCounters[pat_id]
                        logging.info(f"COUNTER ===> Reset for this day for Pat={pat_id}")
                # surveillance de jour
                else:
                    # cas où le timer tourne encore après passage H2
                    if pat_id in self.__myTimers:
                        logging.info(f"TIMER ===> Countdown STOPPED for Pat={pat_id} because time > H2")
                        self.__myTimers[pat_id].cancel()
                        del self.__myTimers[pat_id]
                        # il faut mettre le flag à True bug 2557
                        self.__bd.change_patient_flag(
                            pat_id, True, PatientConst.bed_data.value
                        )
                    # compteur de temps dans le lit de jour
                    # si négatif cela veut dire que nous avons déjà envoyé une alarme!! Pas la peine d'en traiter
                    # d'autres car le temps Max à déjà été dépassée
                    # on traitera le compteur en négatif ;) mais on pourra avoir le total à la fin!
                    if patient_dict["Posture"] is not None and int(patient_dict["Posture"]) == 0:
                        if pat_id not in self.__myCounters:
                            self.__myCounters[pat_id] = arrow.now().int_timestamp
                            logging.info(f"COUNTER ===> Initialisation for this day for Pat={pat_id}")
                        else:
                            delta_sec = arrow.now().int_timestamp - self.__myCounters[pat_id]
                            self.__myCounters[pat_id] = arrow.now().int_timestamp
                            self.__bd.update_patient_lying_day_time(
                                patient_id=pat_id,
                                amount=int(patient_dict["CumulTempsAllonge"] or 0) + delta_sec
                            )
                    else:
                        # cas ou lon vient de sortir du lit
                        if pat_id in self.__myCounters:
                            delta_sec = arrow.now().int_timestamp - self.__myCounters[pat_id]
                            self.__bd.update_patient_lying_day_time(
                                patient_id=pat_id,
                                amount=int(patient_dict["CumulTempsAllonge"]) + delta_sec
                            )
                            del self.__myCounters[pat_id]
                    if (patient_dict["TempsMaxAllongeJour"] and patient_dict["CumulTempsAllonge"]
                            and int(patient_dict["CumulTempsAllonge"]) > int(patient_dict["TempsMaxAllongeJour"])):
                        new_alarm = dict(
                            {
                                "id_patient": pat_id,
                                "id_capteur": self.__bd.get_id_sensor_for_pat(pat_id),
                                "priorite": AlarmConst.LOW.value,  # 100-75 haute, 50 moyenne, 0 basse
                                "desc": f"Resident allongé trop longtemps",
                            }
                        )
                        logging.info(f"COUNTER ===> Alarm for Pat={pat_id} because too lying time")
                        self.__bd.insert_alarm(alarm=new_alarm)
                        # ceci permet de ne plus traiter l'alarme
                        # et continuer a compter des secondes, l'IHM devra prende en compte ceci
                        self.__bd.update_patient_lying_day_time(
                            patient_id=pat_id,
                            amount=int(patient_dict["CumulTempsAllonge"]) - 86400
                        )
                    # cas où la surveillance a été mise à vide (effet de bord!)
                    if patient_dict["CumulTempsAllonge"] and int(patient_dict["CumulTempsAllonge"]) > 86400:
                        self.__bd.update_patient_lying_day_time(
                            patient_id=pat_id,
                            amount=0
                        )
                    logging.debug(f"BED_MONITOR --> Pat={pat_id} is Out of H1-H2")
            time.sleep(self.REFRESH_TIME)

    @staticmethod
    def is_time_on_interval(begin_time: int, end_time: int, check_time: int) -> bool:
        # If check time is not given, default to current UTC time
        if begin_time == end_time:
            return False
        if begin_time < end_time:
            return begin_time <= check_time <= end_time
        else:  # crosses midnight
            return check_time >= begin_time or check_time <= end_time


class MyTimer:

    def __init__(self, bd: SysPadDb_encry, pat_id: int, room: str, interval: int):
        self.__bd = bd
        self.__pat_id = pat_id
        self.__room = room
        self.__myTimer = Timer(interval, self.__alarm)

    def start(self):
        if not self.__myTimer.is_alive():
            self.__myTimer.start()

    def cancel(self):
        if self.__myTimer.is_alive():
            self.__myTimer.cancel()

    def is_runing(self) -> bool:
        if self.__myTimer.is_alive():
            return True
        return False

    def __alarm(self):
        logging.info(f"TIMER ===> Enregistrement d'alarme lit pour le patient {self.__pat_id}")
        new_alarm = dict(
            {
                "id_patient": self.__pat_id,
                "id_capteur": self.__bd.get_id_sensor_for_pat(self.__pat_id),
                "priorite": AlarmConst.HIGH.value,  # 100 haute, 50 moyenne, 0 basse
                "desc": f"Sortie de lit inattendue",
            }
        )
        self.__bd.insert_alarm(alarm=new_alarm)
        return
