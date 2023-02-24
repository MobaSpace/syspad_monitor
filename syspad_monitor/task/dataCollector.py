# coding: utf8
# Cette classe surveille juste l'arrivée de notifications pour un Patient
# Lors que la notification est reçue, on fait une insertion des nouvelles données dans la BD

import json
import logging
import time
from threading import Thread
import ast
import arrow
import pandas as pd
import zmq
from zmq import ZMQError

from syspad_monitor.database_encry import SysPadDb_encry
from syspad_monitor.model.oauth_api import MyWithingsApi
from syspad_monitor.task import PatientConst

class DataCollector(Thread):
    # par défaut 10 secondes de sleep du Thread pour vérifier les Notifications
    REFRESH_TIME = 10
    # Thread_name sera wgs_notify_usedId pour pouvoir traquer le message dans la queu ZMQ

    def __init__(self, bd: SysPadDb_encry, zmq_port: int):
        self.__thread_name = f"wgs_notify_receiver"
        Thread.__init__(self, name=self.__thread_name, target=self.run)
        # attribut nécessaire pour mettre à jour les objets PANDAS contenant les données du patient
        self.__bd = bd
        # attribut nécessaire pour faire les requêtes
        self.__zmq_queue = zmq.Context().socket(zmq.SUB)
        self.__zmq_queue.connect(f"tcp://localhost:{zmq_port}")
        self.__zmq_queue.setsockopt_string(zmq.SUBSCRIBE, self.__thread_name)
        self.stop = False

    def run(self):
        logging.info(
            f"DATA-COLLECTOR --> Process has been started"
        )
        while not self.stop:
            try:
                event = self.__zmq_queue.recv(flags=zmq.NOBLOCK)
                # si un évènement arrive, extraire le JSON
                _, message = event.split(maxsplit=1)
                json_message = json.loads(message)
            except ZMQError:
                json_message = None
            # execution du traitement
            self.process(json_message)
            logging.debug(f"DATA-COLLECTOR --> Running")
            time.sleep(self.REFRESH_TIME)

    def process(self, json_message: dict):
        """
        C'est le process du threat pour vérifier les notifications
        et proceder a son traitement
        :return:
        """
        if json_message:
            # ========= PARTIE IoT MOBASPACE + LinTO  =================
            # ici c'est le traitement du Smart-Tracker
            if int(json_message["appli"]) == 777:
                # trace de l'information reçue
                logging.debug(f"DATA-COLLECTOR --> Reception of data from Smart-Tracker")
                myDev = json_message["devId"]
                nb_pas = int(json_message["steps"])
                power = int(json_message["power"])
                lect_wifi = None
                if len(json_message["macs"]) > 0:
                    listMACS = json_message["macs"].split(",")
                    listRSSI = json_message["rssis"].split(",")
                    lect_wifi = {}
                    for ii in range(len(listMACS)):
                        lect_wifi[listMACS[ii]] = int(listRSSI[ii])
                shock_posture = None
                if len(json_message["shock"]) > 0:
                    # liste contenant NN elements d'acceleration + NN elements de position angulaire
                    shock_posture = list(ast.literal_eval(json_message["shock"]))
                    shock_posture = shock_posture + list(ast.literal_eval(json_message["position"]))
                vit_marche = float(json_message["speed"])
                temps_marche = int(json_message["uptime"])

                self.__bd.update_tracker(
                    macAdd=myDev,
                    lec_wifi=lect_wifi,
                    nb_pas=nb_pas,
                    acc_vector=shock_posture,
                    power=power,
                    walk_speed=vit_marche,
                    act_time=temps_marche
                )
                self.__bd.set_tracker_state(tracker_mac=myDev, state=True)
                return
            # ici c'est le traitement des information LinTO
            if int(json_message["appli"]) == 888:
                # trace de l'information reçue pour releves/transmissions
                logging.debug(f"DATA-COLLECTOR --> Reception of Voice Data from LinTO")
                myRoom = json_message["chambre"]
                myObs = json_message["type"]
                myVals = json.loads(json_message["values"])
                if myObs == "Questionnaire":
                    self.__bd.set_filledForm(room=myRoom, values=myVals)
                elif myObs == "ChambreEntiere":
                    myUser = json_message["idUser"]
                    self.__bd.setBedroomInfoBlock(room=myRoom, values=myVals, idPer=myUser)
                else:
                    # ceci est l'ancienne méthode
                    # self.__bd.set_observable(myObs, myVals, myRoom)
                    logging.debug(f"DATA-COLLECTOR --> Reception of Voice Data from LinTO in wrong format")
                return


            # ========= TOUTE CETTE PARTIE TRAITE WITHINGS  =================
            # c'est la date de reception de la notification
            if "date" in json_message:
                date_new_int = int(json_message["date"])
            else:
                date_new_int = 0

            # correspondance user wthings et patient Id
            wgs_user_id = json_message["userid"]
            patient_id = self.__bd.get_patient_from_wgs_user(wgs_user_id=wgs_user_id)

            if not patient_id:
                logging.debug(f"DATA-COLLECTOR --> API_user={wgs_user_id} is linked to any patient. NOTHING TO DO")
                return
            try:
                patient_id = int(patient_id[0][0])
            except TypeError:
                logging.debug(f"DATA-COLLECTOR --> API_user={wgs_user_id} is linked to any patient. NOTHING TO DO")
                return

            current_patient = self.__bd.read_one_patient(pat_id=patient_id)
            current_api = self.__bd.read_patient_api(patient_id=patient_id, provider='Withings')[0]

            if int(json_message["appli"]) == 50:

                logging.info(
                    f"Reception BED_IN for patId={patient_id}"
                    f" on room={current_patient['Chambre']}"
                )
                self.__bd.update_bed_notify(
                    patient_id, bed_type=PatientConst.BED_IN.value, noti_date=date_new_int
                )
                self.__bd.update_patient_posture(patient_id, PatientConst.LYING.value)

            elif int(json_message["appli"]) == 51:

                logging.info(
                    f"Reception BED_OUT for patId={patient_id}"
                    f" on room={current_patient['Chambre']}"
                )
                self.__bd.update_bed_notify(
                    patient_id, bed_type=PatientConst.BED_OUT.value, noti_date=date_new_int
                )
                self.__bd.update_patient_posture(patient_id, PatientConst.STANDING.value)

            elif int(json_message["appli"]) == 44:
                # ce sont les données du matelas AURA2
                logging.info(
                    f"Reception AURA2 for patId={patient_id}"
                    f" on room={current_patient['Chambre']}"
                )
                d_ini = arrow.get(int(json_message["startdate"]))
                d_end = arrow.get(int(json_message["enddate"]))

                # partie résumé de la nuit
                new_summary = MyWithingsApi.get_sleep_summary(
                    access_token=current_api['AccessToken'],
                    startdate=d_ini,
                    enddate=d_end)

                if new_summary:
                    d_min = arrow.get(new_summary["DateDebut"])
                    self.__bd.insert_sleep_summary(patient=patient_id,
                                                   date_nuit=d_min.format('YYYY-MM-DD'),
                                                   data=new_summary)

                    # cet element nous permettra de faire le calcul des sorties de lit
                    new_data_api = MyWithingsApi.get_sleep_allseries(
                        access_token=current_api['AccessToken'],
                        startdate=d_ini,
                        enddate=d_end)

                    # partie calcul des trous dans la nuit
                    if new_data_api is not None:
                        my_dict = self.__get_outs_of_bed(new_data_api[["Time"]])
                        self.__bd.insert_bedouts_summary(patient=patient_id,
                                                         date_nuit=d_min.format('YYYY-MM-DD'),
                                                         bedouts_summ=my_dict)

    @staticmethod
    def __get_outs_of_bed(time_series: pd.DataFrame) -> dict:
        """
        This functions evaluates the number of times out of bed during the night
        and also the total time out of bed
        :param time_series: the Pandas dataframe containing the time series
        :return: a dict of integers containing #number of outs, total seconds out of bed,
        timestamp of first BED_IN, timestamp of last BED_OUT
        """
        counts = 0
        total_secs = 0
        last_time = time_series.iloc[0]["Time"].item()
        n_rows = len(time_series.index)
        # details = dict()
        my_list = []
        for ii in range(1, n_rows-1):
            current_time = time_series.iloc[ii]["Time"].item()
            diff_time = current_time - last_time
            if diff_time > 60:
                counts += 1
                total_secs += diff_time
                my_list.append( (str(arrow.get(last_time).format("DD-MM-YYYY HH:mm:ss")), diff_time) )
                # details[str(arrow.get(last_time).time())] = diff_time
            last_time = current_time
        details = dict(my_list)
        return {'counts': counts, 'total_secs': total_secs, 'details': details}
