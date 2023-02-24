import json
import logging
import time
import datetime
from threading import Thread
from syspad_monitor.database_encry import SysPadDb_encry
import requests
import re
import arrow

# ceci doit pouvoir être supprimé fin 2021 car MàJ NetSOINS
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'

class ObsTransmitter(Thread):
    """
    Cette classe permet de transmettre les observations faites par le personnel vers
    le logiciel NetSOINS
    """
    # par défaut 60 secondes de sleep du Thread pour vérifier si nouvelles données
    REFRESH_TIME = 60

    def __init__(self, bd: SysPadDb_encry, crm_url: str, crm_key: str):
        self.__bd = bd
        self.__thread_name = f"OBS-TRANSMITTER"
        Thread.__init__(self, name=self.__thread_name, target=self.run)
        # Pour le moment le crm en iterface est NetSOINS
        self.__url = crm_url # https://test.netsoins.org/webservice.php/teranga
        self.__key = crm_key
        self.__type = 'syspad'
        self.__staticURI = None #toutes les URI ont une partie statique qu'il faut garder --> teranga://demo.netsoins.org/Releve#
        self.__myResidents = dict() # ceci est un objet dictionnaire contenant la correspondante UriResident vs NumChm
        self.__myResExtended = dict() # ceci contient des information étendues (nom+prénom vis-à-vis de la chambre)
        self.__myPersonnels = dict() # ceci est un objet dictionnaire avec les NOM/Prenom <--> URI
        self.__myTransmissions = dict() # ceci est un objet dictionnaire contenant la correspondance UriTrans vs Libellé
        self.__uriSyspad = None # ceci est l'URI du personnel correspondant à SySPAD pour faire les transmissions
        if(self.__url and self.__key):
            self.__iniUriSyspad() # initialise la valeur de uriSyspad
            self.__getPersUri() # mise à jour du personnel NetSOINS
            self.__getResUri() # mise à jour des résidents NetSOINS
            self.__getResExtendedUri()
            self.__getTransUri() # mise à jours des uri/transmissions NetSOINS
            self.__getReleveUri() # obtention des uri/libellés des observables ou relevés --> Mise à jour de la BD et du dict
            self.__associateCaregiversURI() # actualisation des URI des possibles nouveaux utilisateurs SySPAD
            self.stop = False
        else:
            self.stop = True

    def run(self):
        if (not self.stop):
            logging.info(f"OBS-TRANSMITTER --> Started")
        else:
            logging.info(f"OBS-TRANSMITTER --> Not Started because any URL and KEY available")
        done_for_the_day = False
        nights_done = False
        days_done = False
        while not self.stop:
            # mise à jour des données NetSOINS pour les résidents chaque mi-nuit
            # et aussi pour le personnel NetSOINS et la cohérence avec SySPAD users
            myTimeNow = datetime.datetime.now().time()
            if not done_for_the_day and myTimeNow > datetime.time(23, 50):
                self.__getResUri()
                # self.__getResExtendedUri()
                self.__getTransUri()
                self.__getPersUri()
                done_for_the_day = True
            if done_for_the_day and myTimeNow < datetime.time(0, 10):
                done_for_the_day = False

            # traitement des observables enregistrés via LinTO
            for observable in self.__bd.get_all_observables():
                tout_ok = False
                if observable['Type'] == 'Transmission':
                    tout_ok = self.__transmitTrans(observable)
                else:
                    tout_ok = self.__transmitReleve(observable)
                if not tout_ok:
                    logging.error(f"OBS-TRANSMITTER --> Problème de transmission vers NetSOINS")
                else:
                    # passage de l'observable à traité
                    self.__bd.change_obs_flag(obs_id=observable['Id'])
                # effacer la ligne de la table si c'est une donnée médicale
                #if self.__isMedical(observable['Type']):
                    #self.__bd.erase_observable(obs_id=observable['Id'])

            # traitement des observables enregistrés via Withings SLEEP (dans table Nuits)
            # this is done at 12h in order to wait for all night data
            if not nights_done and myTimeNow > datetime.time(12, 0):
                for id in self.__bd.get_ids_from_nights():
                    all_good = True
                    all_good = all_good and self.__transmitNight(night=self.__bd.get_SLEEP_from_nights(id)[0])
                    if all_good:
                        # update table
                        logging.info(f"Updating night flag to NuitTraitee...")
                        self.__bd.update_night_flagTraitee(night_id=id)
                nights_done = True
            elif nights_done and myTimeNow < datetime.time(0, 10):
                nights_done = False

            # traitement des observables enregistrés via Withings DAY & MoBY
            # this is done at 23h50
            if not days_done and myTimeNow > datetime.time(23,50):
                for day in self.__bd.get_days():
                    if self.__transmitDay(day):
                        # effacer la ligne de la table
                        logging.info(f"Updating day flag to JourTraite...")
                        self.__bd.update_day_flagTraitee(day_id=day['Id'], flag=True)
                        # self.__bd.erase_day(day['Id'])
            elif days_done and myTimeNow < datetime.time(0, 10):
                days_done = False

            # if a new SySPAD user has been declared in the System
            self.__associateCaregiversURI()

            time.sleep(self.REFRESH_TIME)

    def __isMedical(self, my_type:str) -> bool:
        isMedical = True
        type = my_type.lower()
        nonMedicalTypes = [
            'alimentation',
            'hydratation',
            'selles',
            'diurèse',
            'urine',
            'urines',
            'suivi mictionnel',
        ]
        if type in nonMedicalTypes:
            isMedical = False
        return isMedical

    def __getDARmessage(self, message:str) -> []:
        """
        Cette fonction permet de découper le message dicté dans LinTO pour
        obtenir les 3 champs DAR = DONNEE/ACTION/RESULTAT typique des transmissions
        ciblés
        :param message:
        :return:
        """
        dar = []
        d = ''
        a = ''
        r = ''
        resultat = re.search(r'Résultat\.|Résultats\.', message, re.IGNORECASE)
        donnee = re.search(r'Donner\.|Donnée\.|Donné\.|Données\.|Donnés\.', message, re.IGNORECASE)
        action = re.search(r'Action\.|Actions\.', message, re.IGNORECASE)
        if action:
            if resultat and donnee:
                if action.end() > resultat.start() and action.end() > donnee.start():
                    a = message[action.end():]
                elif resultat.start() < action.end() < donnee.start():
                    a = message[action.end():donnee.start()]
                elif donnee.start() < action.end() < resultat.start():
                    a = message[action.end():resultat.start()]
                elif action.end() < resultat.start() and action.end() < donnee.start():
                    a = message[action.end():min(resultat.start(), donnee.start())]
            elif donnee:
                if donnee.start() > action.end():
                    a = message[action.end():donnee.start()]
                else:
                    a = message[action.end():]
            else:
                a = message[action.end():]
        if donnee:
            if resultat and action:
                if donnee.end() > resultat.start() and donnee.end() > action.start():
                    d = message[donnee.end():]
                elif resultat.start() < donnee.end() < action.start():
                    d = message[donnee.end():action.start()]
                elif action.start() < donnee.end() < resultat.start():
                    d = message[donnee.end():resultat.start()]
                elif donnee.end() < resultat.start() and donnee.end() < action.start():
                    d = message[donnee.end():min(resultat.start(), action.start())]
            elif action:
                if action.start() > donnee.end():
                    d = message[donnee.end():action.start()]
                else:
                    d = message[donnee.end():]
            else:
                d = message[donnee.end():]
        if resultat:
            if donnee and action:
                if resultat.end() > donnee.start() and resultat.end() > action.start():
                    r = message[resultat.end():]
                elif donnee.start() < resultat.end() < action.start():
                    r = message[resultat.end():action.start()]
                elif action.start() < resultat.end() < donnee.start():
                    d = message[resultat.end():donnee.start()]
                elif resultat.end() < donnee.start() and resultat.end() < action.start():
                    d = message[resultat.end():min(donnee.start(), action.start())]
            elif action:
                if action.start() > resultat.end():
                    r = message[resultat.end():action.start()]
                else:
                    r = message[resultat.end():]
            else:
                r = message[resultat.end():]
        dar.append(d.strip())
        dar.append(a.strip())
        dar.append(r.strip())
        return dar

    # fonction pour une transmission
    def __transmitTrans(self, observable: dict) -> bool:
        headers = {
            'accept': 'application/json;charset=UTF-8',
            'Content-Type': 'application/json',
        }
        params = (
            ('type', self.__type),
            ('key', self.__key),
            ('input', 'json'),
            ('output', 'json'),
        )
        my_list = observable['Valeurs'] # [type_trans, message_trans]

        # l'Uri du soignant est dans la table Observables
        uriPer = observable['UriPersonnel']
        if not uriPer or uriPer == "":
            uriPer = self.__uriSyspad
            logging.info(f"OBS-TRANSMITTER --> URI Personnel not Exist... using default SySPAD URI")

        try:
            uriRes = self.__myResidents[int(observable['Chambre'])]
        except KeyError:
            logging.info(f"OBS-TRANSMITTER --> La chambre {observable['Chambre']} n'existe pas --> Transmission vers NetSOINS abandonée")
            # ici il faut généré une entrée dans les transactions pour dire ça ne c'est pas bien passé
            self.__bd.insert_transaction(
                code=f"ABANDON Transmission",
                uri=uriPer,
                detail=f"Ch={observable['Chambre']}, {my_list}"
            )
            return True
        key_trans = my_list[0].lower().replace('&eacute;', 'e').replace('&egrave;', 'e').replace('é', 'e').replace('è', 'e')
        if key_trans in self.__myTransmissions: # la transmission est ciblée
            isNew = False
            IdExterne = self.__checkExistingTransmission(
                uriTransCible=self.__myTransmissions[key_trans],
                uriRes=uriRes
            )
            if not IdExterne:
                IdExterne = f"TR_DAR_{observable['Id']}"
                isNew = True

            message_DAR = self.__getDARmessage(my_list[1])
            mydata = [
                {

                    "IdentifiantExterne": IdExterne,
                    "Module": 1,
                    "UriResident": uriRes,
                    "UriTransmissionCible": self.__myTransmissions[key_trans],
                    "DateDebut": f"{observable['Date'].isoformat()}",
                    "TransmissionMessage": [
                        {
                            "IdentifiantExterne": f"TR_DONNEE_{observable['Id']}",
                            "Message": message_DAR[0],
                            "Date": f"{observable['Date'].isoformat()}",
                            "Type": "DONNEE",
                            "UriPersonnel": uriPer
                        },
                        {
                            "IdentifiantExterne": f"TR_ACTION_{observable['Id']}",
                            "Message": message_DAR[1],
                            "Date": f"{observable['Date'].isoformat()}",
                            "Type": "ACTION",
                            "UriPersonnel": uriPer
                        },
                        {
                            "IdentifiantExterne": f"TR_RESULTAT_{observable['Id']}",
                            "Message": message_DAR[2],
                            "Date": f"{observable['Date'].isoformat()}",
                            "Type": "RESULTAT",
                            "UriPersonnel": uriPer
                        },

                    ]
                }
            ]
        else: # la transmission est de type libre = NARRATIVE
            isNew = True
            mydata = [
                {
                    "IdentifiantExterne": f"TR_NARRATIVE_{observable['Id']}",
                    "Module": 1,
                    "UriResident": uriRes,
                    "DateDebut": f"{observable['Date'].isoformat()}",
                    "TransmissionMessage": [
                        {
                            "IdentifiantExterne": f"TR_NAR_{observable['Id']}",
                            "Message": my_list[1],
                            "Date": f"{observable['Date'].isoformat()}",
                            "Type": "NARRATIVE",
                            "UriPersonnel": uriPer
                        }
                    ]
                }
            ]
        response = requests.post(self.__url + '/Transmission', headers=headers, params=params, data=json.dumps(mydata))
        mycode = json.loads(response.text)
        if response.status_code == 200 and mycode['WS-Code'] == 0:
            logging.info(f"Transation vers NETSOINS OK --> Id = {mydata[0]['IdentifiantExterne']}")
            # Save the transaction Id on database in case it is a new one
            if isNew:
                self.__bd.insert_transaction(
                    code=f"WS-Code={mycode['WS-Code']}",
                    uri=mydata[0]["IdentifiantExterne"],
                    detail=mycode['Transmission'][0]['Uri']
                )
            return True
        else:
            logging.error(f"OBS-TRANSMITTER --> Error sending observation {mydata} with code {mycode['WS-Code']}:{mycode['Erreurs']}")
            # Save the transation Id on database
            self.__bd.insert_transaction(
                code=f"WS-Code={mycode['WS-Code']}",
                uri=mydata[0]["IdentifiantExterne"],
                detail=mycode['Erreurs'])
            if response.status_code == 200:
                return True
            return False

    def __transmitDay(self, day: dict) -> bool:
        #TODO
        return True

    def __transmitNight(self, night: dict) -> bool:
        headers = {
            'accept': 'application/json;charset=UTF-8',
            'Content-Type': 'application/json',
        }
        params = (
            ('type', self.__type),
            ('key', self.__key),
            ('input', 'json'),
            ('output', 'json'),
        )
        try:
            uriRes = self.__myResidents[int(night['NumCh'])]
        except KeyError:
            logging.info(f"OBS-TRANSMITTER --> URI Resident not Exist for room {night['NumCh']}")
            # return True to pass the night to Traitée
            return True
        except TypeError:
            logging.info(f"OBS-TRANSMITTER --> NumCh not defined")
            return True

        try:
            uriReleve = self.__bd.get_uri_for_Observable(libelle="Sommeil")
        except KeyError:
            logging.info(f"OBS-TRANSMITTER --> URI for libelle 'Sommeil' does not exist ")
            # return True to pass the night to Traitée
            return True
        try:
            comment = f"Début = {arrow.get(night['DateDebut']).format('DD-MM-YY HH:mm')};\n " \
                      f"Fin = {arrow.get(night['DateFin']).format('DD-MM-YY HH:mm')};\n "
            comment += f"Score = {night['ScoreNuit']}%;\n Nombre de sorties = {night['NbSorties']};\n "
            comment += f"Temps Reveille = {night['DureeReveilAuLit']};\n"
        except TypeError:
            logging.info(f"OBS-TRANSMITTER --> Night INFO has wrong format...Erasing from DB")
            # return True to pass the night to Traitée
            return True

        # verification que la nuit est complète sur les infos minimales
        if not night['DureeSommeil'] or not night['DateFin']:
            logging.info(f"OBS-TRANSMITTER --> Night INFO has wrong format...Erasing from DB")
            # return True to pass the night to Traitée
            return True

        # comment has to be completed
        jour12h = night['DateFin'].replace(hour=12, minute=0, second=0).isoformat()
        mydata = [{
            "IdentifiantExterne": f"SLEEP_{night['Id']}", # ceci est une clé propre à nous qui doit être unique à chaque fois
            "UriResident": uriRes,
            "UriReleve": uriReleve,
            "UriPersonnel": self.__uriSyspad,
            "Valeur": str(round(night['DureeSommeil'].seconds/3600,1)), # night
            "DateFait": jour12h, # f"{night['DateFin'].isoformat()}",
            "DatePrevue": jour12h, #f"{night['DateFin'].isoformat()}",
            "Commentaire": "Relevé SySPAD \n" + comment
        }]
        response = requests.post(self.__url + '/ResidentReleveValeur', headers=headers, params=params, data=json.dumps(mydata))
        mycode = json.loads(response.text)
        if response.status_code == 200 and mycode['WS-Code'] == 0:
            logging.info(f"Transation vers NETSOINS OK --> Id = {mydata[0]['IdentifiantExterne']}")
            # Save the transation Id on database
            self.__bd.insert_transaction(
                code=f"WS-Code={mycode['WS-Code']}",
                uri=mydata[0]["IdentifiantExterne"],
                detail=mycode['ResidentReleveValeur'][0]['Uri']
            )
            return True
        else:
            logging.error(f"OBS-TRANSMITTER --> Error sending observation {mydata} with code {mycode['WS-Code']}:{mycode['Erreurs']}")
            self.__bd.insert_transaction(
                code=f"WS-Code={mycode['WS-Code']}",
                uri=mydata[0]["IdentifiantExterne"],
                detail=mycode['Erreurs'])
            if response.status_code == 200:
                return True
            return False

    def __transmitReleve(self, observable: dict) -> bool:
        headers = {
            'accept': 'application/json;charset=UTF-8',
            'Content-Type': 'application/json',
        }
        params = (
            ('type', self.__type),
            ('key', self.__key),
            ('input', 'json'),
            ('output', 'json'),
        )
        strings_values = self.__values2NetsoinsFormat(type_obs=observable['Type'], values=observable['Valeurs'])

        # l'Uri du soignant est dans la BD Observables
        uriPer = observable['UriPersonnel']
        if not uriPer or uriPer == "":
            uriPer = self.__uriSyspad
            logging.info(f"OBS-TRANSMITTER --> URI Personnel not Exist... using default SySPAD URI")

        try:
            uriRes = self.__myResidents[int(observable['Chambre'])]
        except KeyError:
            logging.info(f"OBS-TRANSMITTER --> URI Resident not Exist for room {observable['Chambre']}")
            # return True to pass the relevée to Traitée
            self.__bd.insert_transaction(
                code=f"ABANDON Relevé",
                uri=uriPer,
                detail=f"Ch={observable['Chambre']}, {observable['Type']} - {strings_values}"
            )
            return True

        my_commentaire = ''
        if len(strings_values[1] ) > 0:
            my_commentaire += strings_values[1]
        if observable['Commentaire'] and len(observable['Commentaire']) > 0:
            if len(my_commentaire) > 0:
                my_commentaire += ' - '
            my_commentaire += observable['Commentaire']

        mydata = [{
            "IdentifiantExterne": f"REL_{observable['Id']}", # ceci est une clé propre à nous qui doit être unique à chaque fois
            "UriResident": uriRes,
            "UriReleve": observable['URI'],
            "UriPersonnel": uriPer,
            "Valeur": strings_values[0],
            "DatePrevue": f"{observable['Date'].isoformat()}",
            "DateFait": f"{observable['Date'].isoformat()}",
            "Commentaire": my_commentaire
        }]
        response = requests.post(self.__url + '/ResidentReleveValeur', headers=headers, params=params, data=json.dumps(mydata))
        mycode = json.loads(response.text)
        if response.status_code == 200 and mycode['WS-Code'] == 0:
            logging.info(f"Transation vers NETSOINS OK --> Id = {mydata[0]['IdentifiantExterne']}")
            # Save the transation Id on database
            self.__bd.insert_transaction(
                code=f"WS-Code={mycode['WS-Code']}",
                uri=mydata[0]["IdentifiantExterne"],
                detail=mycode['ResidentReleveValeur'][0]['Uri']
            )
            return True
        else:
            logging.error(f"OBS-TRANSMITTER --> Error sending observation {mydata} with code {mycode['WS-Code']}:{mycode['Erreurs']}")
            self.__bd.insert_transaction(
                code=f"WS-Code={mycode['WS-Code']}",
                uri=mydata[0]["IdentifiantExterne"],
                detail=mycode['Erreurs'])
            if response.status_code == 200:
                return True
            return False

    def __getTransUri(self):
        """
        Cette fonction interroge le serveur NetSOINS pour obtenir
        la correspondance entre uri_transmission et libellé
        :return:
        """
        headers = {
            'accept': 'application/xml;charset=UTF-8',
        }
        params = (
            ('type', self.__type),
            ('key', self.__key),
            ('output', 'json'),
            ('fields', 'Uri,Libelle'),
        )
        response = requests.get(self.__url + "/TransmissionCible", headers=headers, params=params)
        dict_str = response.content.decode("UTF-8")
        my_data = json.loads(dict_str)['TransmissionCible']
        mycode = json.loads(dict_str)['WS-Code']

        if response.status_code != 200 or mycode != 0:
            logging.error(f"OBS-TRANSMITTER --> Error retriving URI/Trans code {mycode['WS-Code']}:{mycode['Erreurs']}")
            return

        for trans in my_data:
            try:
                # tout en minuscule, pas d'accents!!
                libelle = trans['Libelle'].lower().replace('&eacute;', 'e').replace('&egrave;', 'e').replace('é', 'e').replace('è', 'e')
                uri_trans = trans['Uri']
                if '/' in libelle: #cas avec deux mots pour un même besoin (ex. nausées / vomissements)
                    subLibelles = libelle.split("/")
                    for subli in subLibelles:
                        self.__myTransmissions[subli.strip()] = uri_trans
                else:
                    self.__myTransmissions[libelle] = uri_trans
            except KeyError:
                print("Pas de chambre sur cette ligne du résident")

    def __iniUriSyspad(self):
        """
        Pour le moment un seul utilisateur fera toutes ces transmissions
        l'utilisateur fictif SYSPAD
        :return:
        """
        headers = {
            'accept': 'application/json;charset=UTF-8',
        }

        params = (
            ('type', self.__type),
            ('key', self.__key),
            ('output', 'json'),
            ('fields', 'Uri,Nom'),
        )

        response = requests.get(self.__url + '/Personnel', headers=headers, params=params)
        dict_str = response.content.decode("UTF-8")
        my_data = json.loads(dict_str)['Personnel']
        for per in my_data:
            try:
                if per["Nom"] == "SYSPAD":
                    self.__uriSyspad = per["Uri"]
                    self.__staticURI = "teranga://" + re.search('teranga://(.*)/', self.__uriSyspad).group(1)
                    break
            except KeyError:
                logging.debug("Problème de clé sur la recupération de l'Uri ")
        if (not self.__uriSyspad):
            logging.debug("OBS-TRANSMITTER --> Impossible de recupérer l'URI pour le personnel SySPAD")
            logging.debug("OBS-TRANSMITTER --> Création du personnel SySPAD...")
            self.__createUriSyspad()

        # add this info to user admin

    def __checkExistingTransmission(self, uriTransCible: str, uriRes: str) -> str:
        """
        Cette fonction permet de vérifier si une Transmission Ciblée est déjà ouverte dans NetSOINS
        :param uriTrans:
        :param uriRes:
        :return:
        """
        headers = {
            'accept': 'application/json;charset=UTF-8',
        }

        params = (
            ('type', self.__type),
            ('key', self.__key),
            ('output', 'json'),
            ('fields', 'Uri,Statut,UriResident,UriTransmissionCible,DateModificationCreation,DateDebut,DateFin'),
            ('date_debut', arrow.now().shift(months=-1).isoformat()),
            ('date_fin', arrow.now().isoformat()),
            ('module', '1'),
            ('UriResident', uriRes),
        )
        response = requests.get(self.__url + "/Transmission", headers=headers, params=params)
        dict_str = response.content.decode("UTF-8")
        try:
            my_data = json.loads(dict_str)['Transmission']
            for trans in my_data:
                if "UriTransmissionCible" in trans:
                    if trans["Statut"] != "C" and trans["UriTransmissionCible"]==uriTransCible:
                        idExterne = self.__bd.existingTransaction(uri=trans['Uri'])
                        if idExterne:
                            return idExterne
        except KeyError:
            return None
        return None

    def __createUriSyspad(self):
        headers = {
            'accept': 'application/json;charset=UTF-8',
        }

        params = (
            ('type', self.__type),
            ('key', self.__key),
            ('input', 'json'),
            ('output', 'json'),
        )
        mydata = [{
            "IdentifiantExterne": f"CREATION_SYSPAD", # ceci est une clé propre à nous qui doit être unique à chaque fois
            "Actif": '1',
            "Nom": 'SYSPAD',
            "Prenom": 'SYSPAD',
            "Commentaire": "Création automatique par SySPAD"
        }]

        response = requests.post(self.__url + '/Personnel', headers=headers, params=params, data=json.dumps(mydata))
        mycode = json.loads(response.text)
        if response.status_code == 200 and mycode['WS-Code'] == 0:
            logging.debug(f"Création utilisateur SYSPAD OK")
            self.__uriSyspad = mycode['Personnel'][0]['Uri']
            # calcul de la partie statique
            self.__staticURI = "teranga://" + re.search('teranga://(.*)/', self.__uriSyspad).group(1)

    def __getResExtendedUri(self):
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
            ('type', self.__type),
            ('key', self.__key),
            ('output', 'json'),
            ('fields', 'Nom,Prenom,EtablissementChambre,Libelle'),
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
                prenom = "Prénom" # dans l'IHM on aura affiché "prenom [espace] nom"
                nom = "Nom"
                if res['Prenom'] and res['Nom']:
                    prenom = res['Prenom']
                    nom = res['Nom']
                self.__myResExtended[num_ch] = [uri_res, prenom, nom]
            except KeyError:
                print("Pas de chambre sur cette ligne du résident")
            except ValueError:
                logging.debug(f"Numero de chambre non numerique " + res['EtablissementChambre']['Libelle'])

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
            ('type', self.__type),
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
            except ValueError:
                logging.debug(f"Numero de chambre non numerique " + res['EtablissementChambre']['Libelle'])

    def __getReleveUri(self):
        """
        Cette fonction mets à jour la table des types d'Observables avec le URI correspondant à chaque établissement
        :return:
        """
        headers = {
            'accept': 'application/xml;charset=UTF-8',
        }

        params = (
            ('type', self.__type),
            ('key', self.__key),
            ('output', 'json'),
            ('fields', 'Uri,Libelle'),
        )

        response = requests.get(self.__url+'/Releve', headers=headers, params=params)
        dict_str = response.content.decode("UTF-8")
        my_data = json.loads(dict_str)['Releve']
        for rel in my_data:
            libelle = rel['Libelle'].replace('&eacute;','é').replace('&egrave;', 'è').replace('&sup2;','2')
            uri = rel['Uri']
            self.__bd.update_uri_typeObservable(libelle=libelle, new_uri=uri)
        # ajout si besoin de celui dedié aux transmission
        self.__bd.update_uri_typeObservable(libelle="Transmission", new_uri=None)

    def __getPersUri(self):
        self.__myPersonnels = ObsTransmitter.getCaregiversList(self.__url, self.__type, self.__key)

    def __findUriPersByNameSurname(self, nom:str, prenom:str) -> str:
        for personnel in self.__myPersonnels:
            try:
                if personnel['Nom'].lower() == nom.lower() and personnel['Prenom'].lower() == prenom.lower():
                    return personnel['Uri']
            except KeyError:
                logging.debug(f"OBS-TRANSMITTER --> Il manque qqch")
        logging.debug(f"OBS-TRANSMITTER --> Personnel {prenom}/{nom} n'existe pas dans NetSOINS...")
        return None


    def __associateCaregiversURI(self):
        # get the list of all users declared in SySPAD that has uri=null
        syspadUsers = self.__bd.getUsersWithoutURI()
        for user in syspadUsers:
            uri = self.__findUriPersByNameSurname(nom=user["UserSurname"], prenom=user["UserFirstname"])
            if uri:
                self.__bd.setURItoUser(user["Id"], uri)


    def getResidentName(self, numCh:int) -> []:
        prenom_nom = ["Non", "disponible"]
        if numCh in self.__myResExtended:
            prenom_nom[0] = self.__myResExtended[numCh][1]
            prenom_nom[1] = self.__myResExtended[numCh][2]
        return prenom_nom

    @staticmethod
    def getCaregiversList(url:str, type:str, key:str, restrictedTo=None) -> []:
        """
        Cette méthode permet de recupérer la liste du Personnel de l'EHPAD
        :return: un objet de type liste/array
        """
        headers = {
            'accept': 'application/xml;charset=UTF-8',
        }
        params = (
            ('type', type),
            ('key', key),
            ('output', 'json'),
            ('fields', 'Uri,Actif,Nom,Prenom'),
        )
        response = requests.get(url + "/Personnel", headers=headers, params=params)
        dict_str = response.content.decode("UTF-8")
        my_data = json.loads(dict_str)['Personnel']

        if restrictedTo:
            res_data = []
            for elem in my_data:
                if elem['Nom'] in restrictedTo:
                    res_data.append(elem)
        else:
            res_data = my_data

        return res_data


    @staticmethod
    def getReleveList(url:str, type:str, key:str) -> []:
        """
        Cette méthode permet de recupérer la liste de releves possibles dans l'EHPAD
        :param url:
        :param type:
        :param key:
        :return:
        """
        headers = {
            'accept': 'application/xml;charset=UTF-8',
        }

        params = (
            ('type', type),
            ('key', key),
            ('output', 'json'),
            ('fields', 'Libelle'),
        )

        response = requests.get(url+'/Releve', headers=headers, params=params)
        dict_str = response.content.decode("UTF-8")
        my_data = json.loads(dict_str)['Releve']
        mycode = json.loads(dict_str)['WS-Code']

        if response.status_code != 200 or mycode != 0:
            logging.error(f"OBS-TRANSMITTER --> Error retriving Releve Libelle code {mycode['WS-Code']}:{mycode['Erreurs']}")
            return []
        res_data = []
        for elem in my_data:
            res_data.append(elem["Libelle"].replace('&eacute;','é').replace('&egrave;', 'è').replace('&sup2;','2'))

        return res_data

    @staticmethod
    def getTransmissionList(url:str, type:str, key:str) -> []:
        """
        Cette méthode permet de recupérer la liste de releves possibles dans l'EHPAD
        :param url:
        :param type:
        :param key:
        :return:
        """
        headers = {
            'accept': 'application/xml;charset=UTF-8',
        }
        params = (
            ('type', type),
            ('key', key),
            ('output', 'json'),
            ('fields', 'Libelle'),
        )
        response = requests.get(url + "/TransmissionCible", headers=headers, params=params)
        dict_str = response.content.decode("UTF-8")
        my_data = json.loads(dict_str)['TransmissionCible']
        mycode = json.loads(dict_str)['WS-Code']

        if response.status_code != 200 or mycode != 0:
            logging.error(f"OBS-TRANSMITTER --> Error retriving Transsmission Libelle code {mycode['WS-Code']}:{mycode['Erreurs']}")
            return []
        res_data = []
        for elem in my_data:
            res_data.append(elem["Libelle"].replace('&eacute;', 'é').replace('&egrave;', 'è').replace('&sup2;','2'))

        return res_data

    def __values2NetsoinsFormat(self, type_obs:str, values:[]) -> []:
        str_ret = ''
        com_ret = ''
        my_list = values
        one_val_obs = [
            "Température",
            "Pulsations",
            "Fréquence respiratoire",
            "Saturation O2",
            "Poids",
            "Taille",
            "Glycémie capillaire",
            "Glycémie"
        ]
        # intention LinTO with only one reading
        if type_obs in one_val_obs:
            str_ret = str(my_list[0])
        #intention LinTO tension
        elif type_obs == "Tension":
            str_ret = f"{my_list[0]}/{my_list[1]}"
        #intention LinTO manger
        elif type_obs == "Alimentation":
            try:
                str_ret = str( float(my_list[1])/4. ) # on parle en quarts pour les repas
            except ValueError:
                str_ret = "0"
            com_ret = "Repas type: " + my_list[0]
        #intention LinTO urines
        elif type_obs == "Urines" or type_obs == "Urine":
            if my_list[0].lower() == "zéro" or my_list[0].lower() == "rien":
                str_ret = '0'
            elif my_list[0].lower() == "peu":
                str_ret = '1'
            elif my_list[0].lower() == "normale":
                str_ret = '2'
            else:
                str_ret = '3'
        #intention LinTO protection
        elif type_obs == "Suivi mictionnel":
            if my_list[0] == "sèche":
                str_ret = 'R'
            elif my_list[0] == "humide":
                str_ret = 'M0'
            else:
                str_ret = 'M1'
        #intention LinTO selles
        elif type_obs == "Selles":
            if my_list[0].lower() == "liquides":
                str_ret = 'L'
            elif my_list[0].lower() == "molles":
                str_ret = 'M'
            elif my_list[0].lower() == "dures":
                str_ret = 'D'
            else: # case 'normales'
                str_ret = 'N'
            str_ret = str_ret + str(my_list[1])
        # intention LinTO hydratation
        elif type_obs == "Hydratation":
            try:
                # 25 correspond à 25cl qui est la valeur par défaut dans Netsoins
                str_ret = str(round(25*float(my_list[0])))
            except Exception:
                logging.error("OBS-TRANSMITTER --> Can't convert Hydratation value to a float")

        return [str_ret, com_ret]
