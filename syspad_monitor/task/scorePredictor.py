# coding: utf8
import json
import logging
import time
import arrow
import datetime
import requests
from datetime import date
from threading import Thread

from syspad_monitor.database_encry import SysPadDb_encry
from syspad_monitor.task import PatientConst, AlarmConst
from syspad_monitor.model.score import Score



class ScorePredictor(Thread):
    """
    Cette classe permet de surveiller les données du patient et enregistrer une alarme dans la
    BD si nécessaire
    """
    # reveil du Thread tous les 15min
    REFRESH_TIME = 900
    def __init__(self, bd: SysPadDb_encry, crm_url=None, crm_key=None):
        self.__bd = bd
        self.__thread_name = f"SCORE-PREDICTOR"
        Thread.__init__(self, name=self.__thread_name, target=self.run)
        self.stop = False
        self.__url = crm_url
        self.__key = crm_key
        self.__type = 'syspad' # this can be différent, for testing this is "teranga"
        self.__items = {} #list of items
        self.__iniID()
        self.__myResidents = dict() # ceci est un objet dictionnaire contenant la correspondante UriResident vs NumChm
        self.__patientScore = {} # this dict contains scores of followed patients
        self.__scoreComputed = False # ceci permet de calculer le Score une fois par jour
        self.__imputationDone = False # ceci permet d'imputer de valeurs en cas de non reception du formulaire

    def run(self):
        logging.info(self.__thread_name + " --> Started")
        while not self.stop:
            currTime = arrow.now().time()
            currDate = arrow.now().date()
            list_of_expected_dates = []
            for day in range(0,7):
                list_of_expected_dates.append(currDate - datetime.timedelta(days=day))
            # recupérer toutes les chambres qui sont suivies pour la prédiction
            rooms = self.__bd.get_roomsId_4_prediction()
            # 23h to do this
            if currTime > datetime.time(23, 0) and not self.__imputationDone:
                for room in rooms:
                    days_to_set = list_of_expected_dates.copy()
                    num_ch = room['NumCh']
                    roomData = self.__bd.get_roomValues_4_prediction(room=num_ch)
                    for data in roomData:
                        if data["Date"] in list_of_expected_dates: # le formulaire n'a pas été rempli
                            days_to_set.remove(data["Date"])
                    for day in days_to_set:
                        # if the day is not set so empty list
                        my_vals = []
                        self.__bd.set_filledForm(room=num_ch, values=my_vals, for_date=day)

                    # now combined imputation with sensors and CRM for all days if necessary!!
                    roomData = self.__bd.get_roomValues_4_prediction(room=num_ch)
                    for data in roomData:
                        my_vals = data["Current_values"]
                        my_day = data["Date"]
                        my_vals = self.__combine(num_ch=num_ch, day=my_day, main_vals=my_vals)
                        self.__bd.set_filledForm(room=num_ch, values=my_vals, for_date=my_day)
                self.__imputationDone = True
            # 23h30 to do this
            if currTime > datetime.time(23, 30) and not self.__scoreComputed and self.__imputationDone:
                tmpScore = {} #this will also erase rooms not used any more by an assigment
                for room in rooms:
                    # à list of "Id", "Date", "Current_values" for the last seven days
                    num_ch = room['NumCh']
                    week_data = self.__bd.get_roomValues_4_prediction(room=num_ch)
                    # this is important in case of reboot or similar
                    if num_ch not in self.__patientScore:
                        # fetch last 7 days data ordered by ASC date
                        myScore = Score()
                        for data in week_data:
                            currentVal = data["Current_values"]
                            currentVal = self.__change2tuples(currentVal)
                            myScore.update(currentVal)
                        data = week_data[-1]
                        tmpScore[num_ch] = myScore
                    # if the room exists, only make the update with the last value
                    else:
                        tmpScore[num_ch] = self.__patientScore[num_ch]
                        data = week_data[-1]
                        currentVal = data["Current_values"]
                        currentVal = self.__change2tuples(currentVal)
                        tmpScore[num_ch].update(currentVal)

                    today = tmpScore[num_ch].score4today
                    tomorrow = tmpScore[num_ch].score4tomorrow
                    trustIndex = tmpScore[num_ch].trustIndex
                    fillingRate = tmpScore[num_ch].fillingRate

                    #update database
                    logging.debug(self.__thread_name + f"Score pour la chambre={num_ch} et le jour={currDate}")
                    self.__bd.update_prediction(id=data["Id"], s4today=today, s4tomorrow=tomorrow, ti=trustIndex, fr=fillingRate)
                self.__patientScore = tmpScore
                self.__scoreComputed = True

            if currTime < datetime.time(1, 0):
                self.__imputationDone = False
                self.__scoreComputed = False

            time.sleep(self.REFRESH_TIME)

    def __combine(self, num_ch:int, day:datetime.date, main_vals:[]):
        combinedVals = main_vals
        sensorVals = self.__imputationFromSensors(num_ch, day)
        list_mainId = []
        for val in main_vals:
            list_mainId.append(val[0])

        list_tmp = []
        for val in sensorVals:
            list_tmp.append(val[0])
        # we get only idex not set in main vals!!! Priority to info comming from user form!!
        list_tmp =  list(set(list_tmp) - set(list_mainId))
        for val in sensorVals:
            for id in list_tmp:
                if val[0] == id:
                    combinedVals.append(val)
        if self.__url and self.__key:
            self.__getResUri() #update correspondance between URI & num_ch
            crmVals = self.__imputationFromCRM(num_ch, day) #here we can obtain somme medical data as temperature...
        else:
            crmVals = self.__imputationFromBD(num_ch, day) #here we've only physiological data
        if crmVals:
            list_tmp = []
            for val in crmVals:
                list_tmp.append(val[0])
            # we get only idex not set in main vals!!! Priority to info comming from user form!!
            list_tmp =  list(set(list_tmp) - set(list_mainId))
            for val in crmVals:
                for id in list_tmp:
                    if val[0] == id:
                        combinedVals.append(val)
        return combinedVals

    def __imputationFromBD(self, num_ch:int, day:datetime.date) ->[]:
        my_vals = []
        #traitement des selles
        journeyObs = self.__bd.get_journeyObservables4room(room=num_ch, day=day, my_type='Selles')
        if journeyObs:
            try:
                idxQ = self.__items["selles_quantité"]
                idxT = self.__items["selles_texture"]

                val_tex = 0
                val_qua = 0
                # here see the obsTransmitter
                for obs in journeyObs:
                    if obs["Valeurs"][0].lower() == 'liquides':
                        val_tex = val_tex - 1
                    if obs["Valeurs"][0].lower() == 'dures':
                        val_tex = val_tex + 1
                    val_qua = val_qua + int(obs["Valeurs"][1])

                val_tex = round(val_tex / len(journeyObs))
                val_qua = round(val_qua / len(journeyObs))
                # this is hard linked with items.py so becareful when changing something...
                my_vals.append((idxQ, val_qua))
                if val_tex != 0:
                    my_vals.append((idxT, 0))
                else:
                    my_vals.append((idxT, 4))

            except KeyError:
                    logging.debug("L'item selles ne fait pas partie du questionnaire")


        # traitement de l'alimentation
        journeyObs = self.__bd.get_journeyObservables4room(room=num_ch, day=day, my_type='Alimentation')
        if journeyObs:
            try:
                idx = self.__items["appétit"]
                val = 0
                # here see the obsTransmitter
                for obs in journeyObs:
                    val = val + float(obs["Valeurs"][1])
                val = val / len(journeyObs)
                val = round(val) # again this is hard related to items.py!!! ce sont le nombre de quarts pris
                if val < 2:
                    my_vals.append((idx, 0))
                else:
                    my_vals.append((idx, val))
            except KeyError:
                logging.debug("L'item appétit ne fait pas partie du questionnaire")

        # traitement de l'hydratation
        journeyObs = self.__bd.get_journeyObservables4room(room=num_ch, day=day, my_type='Hydratation')
        if journeyObs:
            try:
                idx = self.__items["hydratation"]
                val = 0
                for obs in journeyObs:
                    val = val + 25*float(obs["Valeurs"][0])
                if val < 75:
                    my_vals.append((idx, 0))
                elif val >= 75 and val < 150:
                    my_vals.append((idx, 1))
                elif val >= 150 and val < 200:
                    my_vals.append((idx, 3))
                elif val >=200:
                    my_vals.append((idx, 4))
            except KeyError:
                logging.debug("L'item hydratation ne fait pas partie du questionnaire")

        return my_vals

    def __iniID(self):
        self.__items = {}
        items = Score().item_list #list of items
        for it in items:
            self.__items[it["name"]] = int(it["id"])

    def __imputationFromSensors(self, num_ch:int, day:date) ->[]:
        my_vals = []
        patId = self.__bd.get_patId_from_room(room=num_ch)
        if patId:
            logging.debug(self.__thread_name + f"--> La chambre {num_ch} est suivie avec des capteurs")
            # sommeil
            sleep_score = self.__bd.get_sleep_score(patId=patId, mydate=(day-datetime.timedelta(days=1)))
            if sleep_score:
                try:
                    idx = self.__items["sommeil"]
                    if sleep_score > 70:
                        my_vals.append((idx, 3))
                    elif sleep_score > 50:
                        my_vals.append((idx, 2))
                    else:
                        my_vals.append((idx, 1))
                except KeyError:
                    logging.debug("L'item sommeil ne fait pas partie du questionnaire")
            # marche journalière
            step_score = self.__bd.get_step_score(patId=patId, mydate=day)
            if step_score:
                try:
                    idx = self.__items["déplacement"]
                    if step_score > 1000:
                        my_vals.append((idx, 3))
                    elif step_score > 500:
                        my_vals.append((idx, 2))
                    else:
                        my_vals.append((idx, 1))
                except KeyError:
                    logging.debug("L'item déplacement ne fait pas partie du questionnaire")
            # fatigue, on recupère le nombre de secondes au lit en journée
            tiring_score = (self.__bd.get_lying_time(patId=patId, mydate=day) or 0) / 3600.
            if tiring_score:
                try:
                    idx = self.__items["fatigue"]
                    if tiring_score > 6:
                        my_vals.append((idx, 0))
                    elif tiring_score > 4:
                        my_vals.append((idx, 1))
                    elif tiring_score > 3:
                        my_vals.append((idx, 1))
                    else:
                        my_vals.append((idx, 1))
                except KeyError:
                    logging.debug("L'item fatigue ne fait pas partie du questionnaire")
        return my_vals

    def __imputationFromCRM(self, num_ch:int, day:date) ->[]:
        my_vals = []
        if self.__url and self.__key:
            logging.debug(self.__thread_name + f"--> La connexion avec le NetSOINS est possible pour la chambre={num_ch}")
            uriPat = self.__myResidents[num_ch]

            # traitement de l'item fièvre/température
            try:
                idx = self.__items["fièvre"]
                uriRel = self.__bd.get_uri_for_Observable(libelle='Température')
                rels = self.__getRel(releve_uri=uriRel, patient_uri=uriPat, date_deb=day)
                if rels:
                    val_temp = 36.5
                    for rel in rels:
                        if (float(rel) > val_temp):
                            val_temp = float(rel)
                    if val_temp > 38:
                        my_vals.append((idx, 0))
                    else:
                        my_vals.append((idx, 4))
            except KeyError:
                logging.debug("L'item fièvre ne fait pas partie du questionnaire")

            # traitement de l'item selles (il y a quantité et texture!)
            try:
                idxQ = self.__items["selles_quantité"]
                idxT = self.__items["selles_texture"]
                uriRel = self.__bd.get_uri_for_Observable(libelle='Selles')
                rels = self.__getRel(releve_uri=uriRel, patient_uri=uriPat, date_deb=day) # ["M2", "N3", "0",...]
                if rels:
                    val_qua = 0
                    val_tex = 0
                    for rel in rels:
                        if len(rel) > 1:
                            if rel[0] == 'L':
                                val_tex = val_tex - 1
                            elif rel[0] == 'D':
                                val_tex = val_tex + 1
                            val_qua = val_qua + int(rel[-1])
                    val_tex = round(val_tex / len(rels))
                    val_qua = round(val_qua / len(rels))
                    # this is hard linked with items.py so becareful when changing something...
                    my_vals.append((idxQ, val_qua))
                    if val_tex != 0:
                        my_vals.append((idxT, 0))
                    else:
                        my_vals.append((idxT, 4))

            except KeyError:
                logging.debug("L'item selles ne fait pas partie du questionnaire")

            # traitement de l'item 'appétit'
            try:
                idx = self.__items["appétit"]
                uriRel = self.__bd.get_uri_for_Observable(libelle='Alimentation')
                rels = self.__getRel(releve_uri=uriRel, patient_uri=uriPat, date_deb=day) # ["0.5", "0.3", "0.75", "0"...]
                logging.debug(f"Alimentation {rels}")
                if rels:
                    val = 0
                    for rel in rels:
                        val = val + float(rel)
                    val = val / len(rels)
                    val = round(val * 4) # again this is hard related to items.py!!! Dans le CRM NetSOINS ce sont des 0.25/0.5/0.75
                    if val < 2:
                        my_vals.append((idx, 0))
                    else:
                        my_vals.append((idx, val))
            except KeyError:
                logging.debug("L'item appétit/alimentation ne fait pas partie du questionnaire")
            # traitement de l'item 'hydratation'
            try:
                idx = self.__items["hydratation"]
                uriRel = self.__bd.get_uri_for_Observable(libelle='Hydratation')
                rels = self.__getRel(releve_uri=uriRel, patient_uri=uriPat, date_deb=day) # ["12", "20", "15", "0"...] cL

                if rels:
                    val = 0
                    for rel in rels:
                        val = val + int(rel)
                    if val < 75:
                        my_vals.append((idx, 0))
                    elif val >= 75 and val < 150:
                        my_vals.append((idx, 1))
                    elif val >= 150 and val < 200:
                        my_vals.append((idx, 3))
                    elif val >=200:
                        my_vals.append((idx, 4))
            except KeyError:
                logging.debug("L'item hydratation ne fait pas partie du questionnaire")
        return my_vals

    def __getRel(self, releve_uri:str, patient_uri:str, date_deb:date) -> []:
        valeur = []
        headers = {
            'accept': 'application/xml;charset=UTF-8',
        }

        params = (
            ('type', self.__type),
            ('key', self.__key),
            ('output', 'json'),
            ('fields', 'Uri,Resident,Libelle,Releve,Valeur,Commentaire'),
            ('date_debut_prevue', date_deb.isoformat() + ' 00:00:00'),
            ('date_fin_prevue', date_deb.isoformat() + ' 23:59:59'),
            ('UriResident', patient_uri),
            ('UriReleve', releve_uri),
        )

        response = requests.get(self.__url+'/ResidentReleveValeur', headers=headers, params=params)
        dict_str = response.content.decode("UTF-8")
        try:
            my_data = json.loads(dict_str)['ResidentReleveValeur']
            #it can has several measurement of this type during the day
            for rel in my_data:
                valeur.append(rel["Valeur"])
        except KeyError:
            logging.debug(self.__thread_name + f" --> Pas de données Netsoins pour {patient_uri}")
        return valeur

    def __getResUri(self):
        """
        Cette fonction interroge le serveur NetSOINS pour obtenir
        la correspondance entre uri_resident et num_chambre
        et aussi la correspondance libellé_observable et uri
        :return:
        """
        headers = {
            'accept': 'application/xml;charset=UTF-8',
        }
        params = (
            ('type', self.__type), #type can change, for testing it is teranga
            ('key', self.__key),
            ('output', 'json'),
            ('fields', 'EtablissementChambre,Libelle'),
            ('statut_actif', 'tous'),
            ('statut_archive', 'tous'),
            ('situation', 'present'),
        )
        response = requests.get(self.__url + "/Resident", headers=headers, params=params)
        dict_str = response.content.decode("UTF-8")
        my_data = json.loads(dict_str)['Resident']
        for res in my_data:
            try:
                num_ch = int(res['EtablissementChambre']['Libelle'])
                uri_res = res['Uri']
                self.__myResidents[num_ch] = uri_res
            except KeyError:
                print("Pas de chambre sur cette ligne du résident")

    @staticmethod
    def __change2tuples(values: []) -> []:
        # JSON et js ne sais pas envoyer de tuples mais de listes dans des listes
        # hors le module score travaille avec une liste de tuples
        listTuples = []
        for elem in values:
            tuple = (elem[0], elem[1])
            listTuples.append(tuple)
        return listTuples