# coding: utf8
import json
import logging
import time
from threading import Thread
import os
import pandas as pd
import pickle
import numpy as np
from syspad_monitor.database_encry import SysPadDb_encry
from syspad_monitor.task import PatientConst, AlarmConst
from syspad_monitor.model.fingerPrinting import loc_fingerprinting
from syspad_monitor.model.feature_extractor import FeatureExtractor


class DataProcessor(Thread):
    """
    Cette classe permet de surveiller les données du patient et enregistrer une alarme dans la
    BD si nécessaire
    """
    # par défaut 10 secondes de sleep du Thread pour vérifier si nouvelles données
    REFRESH_TIME = 10
    def __init__(self, bd: SysPadDb_encry, minutes_out_of_bounds: int, loc_params=None, classifier_params=None):
        self.__bd = bd
        self.__thread_name = f"DATA-PROCESSING"
        Thread.__init__(self, name=self.__thread_name, target=self.run)
        self.stop = False
        self.__conMin = minutes_out_of_bounds
        self.__fgpt = None #fingerprinting
        if loc_params and len(loc_params)==3:
            self.__fgpt = loc_fingerprinting(loc_db=loc_params[0],
                                             label_pos=loc_params[1],
                                             list_restricted_nets=loc_params[2]
                                             )
        elif loc_params and len(loc_params) < 3:
            self.__fgpt = loc_fingerprinting(loc_db=loc_params[0],
                                             label_pos=loc_params[1],
                                             )
        self.__estimator = None
        self.__featureExtractor = None
        if classifier_params is not None:
            try:
                with open(os.path.join("model", classifier_params['file']), 'rb') as f:
                    estimator = pickle.load(f)
                    self.__estimator = estimator
            except EnvironmentError as ee:
                logging.warning(f"Could not load estimator, prediction is disabled")
            lmt, umt, it, fs = classifier_params['lmt'], classifier_params['umt'], classifier_params['it'], classifier_params['fs']
            self.__featureExtractor = FeatureExtractor(lmt, umt, it, fs)
        else:
            logging.warning(f"Could not load estimator, prediction will be disabled")

        self.__battAlarms = []

    def run(self):
        logging.info(f"DATA-PROCESSING --> Started")
        while not self.stop:
            for tracker in self.__bd.get_trackers(): # retourne uniquement les trackers mis à jour
                if tracker["PatientId"]: # traitement seulement si le tracker est attaché à un résident
                    # traitement de la localisation
                    my_pos = None
                    if self.__fgpt:
                        my_pos = self.__fgpt.perform_loc(tracker["LecturesWifi"])

                    # traitement des pas pour la journée en cours
                    self.__bd.insert_day_steps(
                        patient=tracker["PatientId"],
                        date_jour=tracker["LastUpdate"].strftime('%Y-%m-%d'),
                        steps=tracker["NbPas"],
                        activityTime=tracker["ActivityTime"],
                        stepfreq=tracker["VitesseMarche"]
                    )
                    # traitement de la chute
                    if tracker["AccVector"]:
                        # cas ou nous avons le vecteur acc et position complets
                        if len(tracker["AccVector"]) > 2:
                            msg = f"Evenement à risque detecté"
                            lev = AlarmConst.HIGH.value
                        # cas ou nous avons les valeurs [-1000,-1000] qui correspond à une chute n'ayant pu être transmise en temps réel
                        else:
                            msg = f"Evenement à risque survenu il y a quelques minutes"
                            lev = AlarmConst.MEDIUM.value
                        if my_pos:
                            msg += f" autour de {my_pos['Label']}"
                        self.__alarm(
                            pat_id=tracker["PatientId"],
                            message=msg,
                            level=lev,
                            cap_id=tracker["CapteurId"]
                        )
                    # traitement de l'energie
                    # la batterie contient 400mAh pour le moment
                    if tracker["Power"] < 20.0 and tracker["CapteurId"] not in self.__battAlarms:
                        msg = f"Le dispositif MoBy de la chambre {tracker['Chambre']} a une batterie faible"
                        self.__alarm(
                            pat_id=tracker["PatientId"],
                            message=msg,
                            level=AlarmConst.LOW.value,
                            cap_id=tracker["CapteurId"]
                        )
                        self.__battAlarms.append(tracker["CapteurId"])
                    if tracker["Power"] > 20.0 and tracker["CapteurId"] in self.__battAlarms:
                        self.__battAlarms.remove(tracker["CapteurId"])
                # passage à traité de la nouvelle donnée tracker
                self.__bd.set_tracker_flag(devId=tracker["Id"], flag=True)

            self.__splitDataFromRoom()

            time.sleep(self.REFRESH_TIME)

    def __splitDataFromRoom(self):
        # this function will split Block of data from "infoblocchambreview" into single "Observables"
        rooms2split = self.__bd.getRoomsInfoBlocks()
        for room in rooms2split:
            my_id = room["Id"]
            numch = room["NumCh"]
            uriPer = room["UriNetSOINS"]
            releves = room["Data"]["releves"]
            transmission = room["Data"]["transmissions"]
            for rel in releves:
                my_type = rel["type"]
                my_val = rel["value"]
                my_comment = None
                if "comment" in rel:
                    my_comment = rel["comment"]
                self.__bd.set_observable(
                    room=numch,
                    uriPer=uriPer,
                    type_obs=my_type,
                    values=my_val,
                    comment=my_comment
                )

            for trans in transmission:
                my_type = "Transmission"
                my_val = []
                my_val.append(trans["type"].lower())
                my_val.append(trans["value"][0])

                self.__bd.set_observable(
                    room=numch,
                    uriPer=uriPer,
                    type_obs=my_type,
                    values=my_val
                )
            # set flag traite=true
            self.__bd.setRoomBlockFlag(id=my_id, flag=True)
        return

    def __alarm(self, pat_id: int, message: str, level=AlarmConst.HIGH.value, cap_id = None):
        logging.info(
            f"DATA-PROCESSING --> Alarm registration for patient={pat_id} with message={message}"
        )
        new_alarm = dict(
            {
                "id_patient": pat_id,
                "id_capteur": cap_id,
                "priorite": level,  # 100 haute, 50 moyenne, 0 basse
                "desc": message,
            }
        )
        self.__bd.insert_alarm(alarm=new_alarm)