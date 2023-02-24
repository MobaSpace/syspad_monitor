import configparser
import json
import logging
import time
from os import path
from threading import Thread
import arrow

import requests
import zmq
from flask import Flask, request, jsonify

from syspad_monitor.model.score import Score
from syspad_monitor.database_encry import SysPadDb_encry
from syspad_monitor.model.oauth_api import MyWithingsApi
from syspad_monitor.task.alarmSender import AlarmSender
from syspad_monitor.task.bedMonitor import BedMonitor
from syspad_monitor.task.dataCollector import DataCollector
from syspad_monitor.task.dataProcessing import DataProcessor
from syspad_monitor.task.tokensUpdater import TokensUpdater
from syspad_monitor.task.deviceMonitor import DeviceMonitor
from syspad_monitor.task.obsTransmitter import ObsTransmitter
from syspad_monitor.task.scorePredictor import ScorePredictor

class SysPad(Thread):

    REFRESH_TIME = 60

    def __init__(self):
        self.__thread_name = f"syspad_general_process"
        Thread.__init__(self, name=self.__thread_name, target=self.run)
        self.__db = db
        self.__aSender = AlarmSender(
            bd=self.__db,
            params_mail=my_params["smtp"],
            params_sms=my_params["sms"],
            time_out=my_params["alarm"],
            xmessages=my_params["amessages"],
            serv_url=my_params["ehpad_server"],
            firebase=my_params["firebase"]
        )
        self.__apis = MyWithingsApi(bd=self.__db, withings_evt_url=my_params["withings"])
        self.__updater = TokensUpdater(database=self.__db, api=self.__apis)
        self.__collector = DataCollector(bd=self.__db, zmq_port=my_params["ports"]["zmq"])
        self.__bedmonitor = BedMonitor(bd=self.__db)
        self.__dataprocessor = DataProcessor(bd=self.__db, minutes_out_of_bounds=int(my_params["minutes_outbounds"]),
                                             classifier_params=my_params["class_params"])
        self.__devmonitor = DeviceMonitor(bd=self.__db)
        self.__obstransmitter = ObsTransmitter(
            bd=self.__db,
            crm_url=my_params["ehpad_crm"]["url"],
            crm_key=my_params["ehpad_crm"]["key"]
        )
        self.__predictor = ScorePredictor(bd=self.__db) #, crm_url=my_params["ehpad_crm"]["url"], crm_key=my_params["ehpad_crm"]["key"])

    def getResNames(self, room:int) -> []:
        return self.__obstransmitter.getResidentName(numCh=room)

    def run(self):
        # effacer les données des qui ont plus de 7j
        self.__db.erase_old_days(before_days=7)
        self.__db.erase_old_nights(before_days=7)
        self.__db.erase_old_alarms(before_days=7)
        self.__db.erase_old_observables(before_days=7)
        self.__db.erase_old_scores(before_days=7)

        # création et démarrage du process de surveillance de la validité des Tokens pour chaque API
        # démarrage du processus d'envoi des alarmes vers Mail, SMS, voice_call
        logging.info(f"SYSPAD --> Starting all processes...It will take 20 secs...")
        self.__updater.start()
        time.sleep(20)  # mettre 20s apres test
        self.__aSender.start()
        self.__collector.start()
        self.__bedmonitor.start()
        self.__dataprocessor.start()
        self.__devmonitor.start()
        self.__obstransmitter.start()
        self.__predictor.start()

        time_to_erase = arrow.now().shift(days=1)

        while True:
            logging.info(f"SYSPAD --> WatchDog des processus...")
            all_proc_ok = True

            if not self.__updater.is_alive():
                logging.error(f"SYSPAD ===> Tokens Updater IS DOWN!!! Starting thread again...")
                self.__updater = TokensUpdater(database=self.__db, api=self.__apis)
                self.__updater.start()
                all_proc_ok = False
            if not self.__aSender.is_alive():
                logging.error(f"SYSPAD ===> Alarm Sender IS DOWN!!! Starting thread again...")
                self.__aSender = AlarmSender(
                    bd=self.__db,
                    params_mail=my_params["smtp"],
                    params_sms=my_params["sms"],
                    time_out=my_params["alarm"],
                    serv_url=my_params["ehpad_server"],
                    firebase=my_params["firebase"]
                )
                self.__aSender.start()
                all_proc_ok = False
            if not self.__collector.is_alive():
                logging.error(f"SYSPAD ===> Data Collector IS DOWN!!! Starting thread again...")
                self.__collector = DataCollector(bd=self.__db, zmq_port=my_params["ports"]["zmq"])
                self.__collector.start()
                all_proc_ok = False
            if not self.__bedmonitor.is_alive():
                logging.error(f"SYSPAD ===> Bed Monitor IS DOWN!!! Starting thread again...")
                self.__bedmonitor = BedMonitor(bd=self.__db)
                self.__bedmonitor.start()
                all_proc_ok = False
            if not self.__dataprocessor.is_alive():
                logging.error(f"SYSPAD ===> Data Processor IS DOWN!!! Starting thread again...")
                self.__dataprocessor = DataProcessor(bd=self.__db,
                                                     minutes_out_of_bounds=int(my_params["minutes_outbounds"]))
                self.__dataprocessor.start()
                all_proc_ok = False
            if not self.__devmonitor.is_alive():
                logging.error(f"SYSPAD ===> Devices Monitor IS DOWN!!! Starting thread again...")
                self.__devmonitor = DeviceMonitor(bd=self.__db)
                self.__devmonitor.start()
                all_proc_ok = False
            if not self.__predictor.is_alive():
                logging.error(f"SYSPAD ===> Score Predictor IS DOWN!!! Starting thread again...")
                self.__predictor = ScorePredictor(bd=self.__db) #, crm_url=my_params["ehpad_crm"]["url"], crm_key=my_params["ehpad_crm"]["key"])
                self.__predictor.start()
                all_proc_ok = False
            if not self.__obstransmitter.is_alive() and my_params["ehpad_crm"]["url"]:
                logging.error(f"SYSPAD ===> Observable Transmitter IS DOWN!!! Starting thread again...")
                self.__obstransmitter = ObsTransmitter(
                    bd=self.__db,
                    crm_url=my_params["ehpad_crm"]["url"],
                    crm_key=my_params["ehpad_crm"]["key"]
                )
                self.__obstransmitter.start()
                all_proc_ok = False

            if all_proc_ok:
                logging.info(f"SYSPAD --> All processes are running OK!")
            else:
                logging.info(f"SYSPAD --> Something went wrong but has been fixed!")

            # netoyage de la BD tout ce qui est vieux de plus de 7j
            if arrow.now() > time_to_erase:
                logging.info(f"SYSPAD --> Erasing all data older than 7 days...")
                self.__db.erase_old_days(before_days=7)
                self.__db.erase_old_nights(before_days=7)
                self.__db.erase_old_alarms(before_days=7)
                self.__db.erase_old_observables(before_days=7)
                # update next time to erase
                time_to_erase = arrow.now().shift(days=1)

            time.sleep(self.REFRESH_TIME)


def load_config() -> dict:
    config = configparser.ConfigParser()
    if not path.exists("/etc/syspad_monitor.conf"):  # here set the config file on the system, maybe on /etc/..
        print(
            f"/!\\ WARNING /!\\ Pas de fichier INI --> Les valeurs de configuration par défaut seront utilisés!!!!!!"
        )
        # TODO
    else:
        config.read("/etc/syspad_monitor.conf")
        classifier_params = {"file": config.get("CLASSIFIER", 'file'),
                             "lmt": float(config.get("THRESHOLDS", 'lmt')),
                             "umt": float(config.get("THRESHOLDS", 'umt')),
                             "it": float(config.get("THRESHOLDS", 'it')),
                             "fs": float(config.get("THRESHOLDS", 'fs'))}
        smtp_params = {
            "host": config.get("MAIL", "host"),
            "port": config.get("MAIL", "port"),
            "user": config.get("MAIL", "user"),
            "pwd": config.get("MAIL", "pass"),
            "from": config.get("MAIL", "from"),
        }
        db_params = {
            "host": config.get("DATABASE", "host"),
            "port": config.get("DATABASE", "port"),
            "user": config.get("DATABASE", "user"),
            "pwd": config.get("DATABASE", "pass"),
            "name": config.get("DATABASE", "dbname"),
        }
        sms_params = {
            "user": config.get("SMS", "user"),
            "password": config.get("SMS", "pass"),
        }
        ports_params = {"web": config.get("PORTS", "web"), "zmq": config.get("PORTS", "zmq"), "host": config.get("PORTS", "host")}
        alarm_repeat = config.get("ALARM_REPEAT", "sendingEach")
        alarm_times = config.get("ALARM_REPEAT", "xtimes")
        alarm_messages = config.get("ALARM_REPEAT", "xmessages")
        loglevel = config.get("LOGGING", "loglevel")
        withings = config.get("WITHINGS", "api")
        ehpad_alarm_server = config.get("EHPAD_ALARM_SERVER", "url")
        minutes_outbounds = config.get("PHYSIOLOGY", "minutes_out_of_bounds")
        crm_info = {"url": config.get("NETSOINS", "url"), "key": config.get("NETSOINS", "key"), "rest2": config.get("NETSOINS", "restrictedTo").split(";")}
        firebase = {"topic": config.get("FIREBASE", "topic"), "key": config.get("FIREBASE", "key")}
        my_params_dict = {
            "smtp": smtp_params,
            "database": db_params,
            "sms": sms_params,
            "alarm": alarm_repeat,
            "atimes": alarm_times,
            "amessages": alarm_messages,
            "ports": ports_params,
            "logs": loglevel,
            "withings": withings,
            "ehpad_server": ehpad_alarm_server,
            "minutes_outbounds": minutes_outbounds,
            "ehpad_crm": crm_info,
            "firebase": firebase,
            "class_params": classifier_params,
        }
        return my_params_dict


my_params = load_config()
logging.basicConfig(
    format="%(levelname)s: %(message)s from %(module)s %(funcName)s",
    level=my_params["logs"],
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Configuration de la journalisation des messages et erreurs
logging.getLogger("SYSPAD_M").addHandler(logging.StreamHandler())

# La connexion à la base de données
db = SysPadDb_encry(params=my_params["database"], alarm_times=int(my_params["atimes"]))
# Établir la connexion à la base de données
db.connect()

# L'instance au système
syspad = SysPad()

# L'instance de Flask
app = Flask(__name__)

# L'instance de ZMQ
zmq_socket = zmq.Context().socket(zmq.PUB)


# Route par défaut
@app.route("/")
def index():
    return "The SySPAD Server is UP!!"


# Route pour obtenir la checklist
@app.route('/api/checklist', methods=['HEAD', 'GET', 'POST'])
def get_checklist():
    if request.method == 'GET' or request.method == 'POST':
        logging.debug(f"THE JSON IS: {request}")
        content = request.form.to_dict(flat=True)
        logging.debug(f"THE JSON IS: {content}")
        result = []
        if content.get('appli') == '777':
            result = db.checklist_from_db(content.get('chambre'), content.get('periode'))
            if len(result) < 1:
                result =['Pas de checklist disponible dans SySPAD pour ce résident', 'Ceci es un objet virtuel']
            return jsonify(result)
    return ""


# Route pour obtenir la liste de personnel EHPAD
@app.route('/api/caregiverlist', methods=['HEAD', 'GET', 'POST'])
def get_caregiverslist():
    if request.method == 'GET' or request.method == 'POST':
        content = request.form.to_dict(flat=True)
        if content.get('appli') == '777' and content.get('type')=='syspad':
            logging.debug("Demande LinTO pour fourniture de liste du personnel")
            # attention à changer la tabulation ci-dessous pour mettre une sécurité
            result = ObsTransmitter.getCaregiversList(
                url=my_params["ehpad_crm"]["url"],
                key=my_params["ehpad_crm"]["key"],
                type='syspad',
                restrictedTo=my_params["ehpad_crm"]["rest2"]
            )
            return jsonify(result)
    return ""


# Route pour obtenir le questionnaire lié aux résidents
@app.route('/api/form', methods=['HEAD', 'GET', 'POST'])
def get_form():
    if request.method == 'GET' or request.method == 'POST':
        content = request.form.to_dict(flat=True)
        if content.get('appli') == '777':
            logging.debug("Demande LinTO pour obtention du questionnaire patient")
            # ceci va donner la liste avec les questions, etc... on envoie tout! C'est linTO qui fera le tri
            result = Score().item_list
            return jsonify(result)
    return ""

# Route pour obtenir la liste de releves
@app.route('/api/relevelist', methods=['HEAD', 'GET', 'POST'])
def get_relevelist():
    if request.method == 'GET' or request.method == 'POST':
        content = request.form.to_dict(flat=True)
        if content.get('appli') == '777':
            logging.debug("Demande LinTO pour obtention de la liste de relevés")
            # ceci va donner la liste des releves disponiubles sur cet EHPAD
            result = ObsTransmitter.getReleveList(
                url=my_params["ehpad_crm"]["url"],
                key=my_params["ehpad_crm"]["key"],
                type='syspad'
            )
            return jsonify(result)
    return jsonify([])

# Route pour obtenir la liste des cibles des transmissions
@app.route('/api/transmissionlist', methods=['HEAD', 'GET', 'POST'])
def get_transmissionlist():
    if request.method == 'GET' or request.method == 'POST':
        content = request.form.to_dict(flat=True)
        if content.get('appli') == '777':
            logging.debug("MAIN --> Demande LinTO pour obtention de la liste de Transmissions Cible")
            result = ObsTransmitter.getTransmissionList(
                url=my_params["ehpad_crm"]["url"],
                key=my_params["ehpad_crm"]["key"],
                type='syspad'
            )
            return jsonify(result)
    return jsonify([])

# Route pour obtenir le nom-prénom du résident qui est dans la Ch XX
@app.route('/api/residentinroom', methods=['HEAD', 'GET', 'POST'])
def get_resident_in_room():
    if request.method == 'GET' or request.method == 'POST':
        content = request.form.to_dict(flat=True)
        if content.get('appli') == '777' and my_params["ehpad_crm"]["url"] and my_params["ehpad_crm"]["key"]:
            residentRoom = content.get('resRoom')
            logging.debug(f"MAIN --> Demande LinTO pour obtention du résident de la chambre {residentRoom}")
            result = syspad.getResNames(room=int(residentRoom))
            return jsonify(result)
    return jsonify(["Non", "disponible"])

# Route pour obtenir la liste de chambres visités par un utilisateur
@app.route('/api/roomsvisited', methods=['HEAD', 'GET', 'POST'])
def get_roomsvisited():
    if request.method == 'GET' or request.method == 'POST':
        content = request.form.to_dict(flat=True)
        if content.get('appli') == '777':
            userId = content.get('idUser')
            logging.debug(f"MAIN --> Demande LinTO liste de Ch visités par {userId}")
            result = db.getRoomsVisited4User(idPer=userId)
            if result:
                return jsonify(result)
    return jsonify([])

# évènement withings
@app.route("/withings_evt", methods=["HEAD", "POST"])
def withings_evt():
    """Point d'entrée web des notifications Withings/LinTO et MoBY
    :return: parser
    """
    try:
        if request.method == "POST":
            logging.debug(request.headers)
            request_data = request.form.to_dict(flat=True)
            logging.debug(f"form : {request_data}")
            zmq_message = f"wgs_notify_receiver {json.dumps(request_data)}"
            logging.debug(f"Message envoyé vers ZMQ: {zmq_message}")
            zmq_socket.send_string(zmq_message)
    except AttributeError:
        logging.info(f"Requete inconnue : {request.values}")

    return ""


@app.before_first_request
def activate_job():
    return "Premier démarrage de Flask"


def start_runner(port):
    def start_loop():
        not_started = True
        while True:
            if not_started:
                try:
                    r = requests.get(f"http://127.0.0.1:{port}/")
                    if r.status_code == 200:
                        logging.debug("FLASK --> le serveur est ACTIF")
                        not_started = False
                    logging.debug(r.status_code)
                except requests.exceptions.RequestException:
                    logging.debug("FLASK --> le serveur n'a pas encore démarré")
            else:
                logging.debug("FLASK --> en attente de notifications externes")
            time.sleep(20)

    logging.debug("Flask, démarrage du monitoring")
    thread = Thread(target=start_loop)
    thread.start()


def main():
    # Démarrage du process principal de surveillance des MàJ de la BD
    syspad.start()
    # Démarre le serveur ZMQ
    zmq_socket.bind(f"tcp://*:{my_params['ports']['zmq']}")
    logging.info(f"Le serveur ZMQ est démarré sur le port {my_params['ports']['zmq']}")
    start_runner(my_params["ports"]["web"])
    app.run(debug=False, port=my_params["ports"]["web"], host=my_params['ports']['host'])


if __name__ == "__main__":
    main()
