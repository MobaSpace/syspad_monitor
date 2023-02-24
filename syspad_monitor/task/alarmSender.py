import json
import logging
import re
import smtplib
import time
import urllib.error
import urllib.parse
import urllib.request
from email.message import EmailMessage
from smtplib import SMTPException
from threading import Thread
from pyfcm import FCMNotification

import arrow
import requests

from syspad_monitor.database_encry import SysPadDb_encry
from syspad_monitor.task import AlarmConst

class AlarmSender(Thread):
    """
    Cette classe est chargée de surveiller la table d'alarmes
    """
    # par défaut 5 secondes de sleep du Thread pour vérifier les Alarmes dans la BD
    REFRESH_TIME = 5
    SENDING_TIMEOUT = 180

    def __init__(self, bd: SysPadDb_encry, params_mail: dict, params_sms: dict, time_out: int, xmessages=1, serv_url=None, firebase=None):
        self.__bd = bd
        self.__thread_name = f"alarm_sender"
        Thread.__init__(self, name=self.__thread_name, target=self.run)
        self.__lastSending = dict()
        self.SENDING_TIMEOUT = int(time_out)
        self.__host = params_mail["host"]
        self.__port = params_mail["port"]
        self.__mail_from = params_mail["from"]
        self.__smtp_pass = params_mail["pwd"]
        self.__sms_user = params_sms["user"]
        self.__sms_password = params_sms["password"]
        self.__serverURL = serv_url
        self.__xm = int(xmessages)
        self.__fb = firebase

    def run(self):
        logging.info(f"Démarrage de la tache de surveillance des alarmes dans la BD")
        while True:
            # JSON retourné donc c'est un dict sur toutes les alarmes où le counter > 0
            alarm_list = self.__bd.read_alarms()
            now = arrow.now().int_timestamp
            for alarm in alarm_list:
                #               0    1       2        3            4            5         6      7
                #  liste avec (Id, NbNotis, Chambre, NumCh, Description, Creation, Priorite, Appel)
                alarm_id = alarm["Id"]
                counts = alarm["NbNotifications"]

                if (str(alarm_id) not in self.__lastSending
                        or now > self.__lastSending[f"{alarm_id}"] + self.SENDING_TIMEOUT):
                    self.__lastSending[str(alarm_id)] = now

                    # send SMS in case telephone is defined
                    phone_exp = r"^(33)[1-9][0-9]{8}$"

                    # send Voice Alarm in case telephone is defined
                    if alarm["Appel"] and re.search(phone_exp, alarm["Appel"]) and alarm["Priorite"] >= AlarmConst.HIGH.value: # Alarm.HIGH.value = 85 so 75 is enought
                        my_message = alarm["Description"] + f" résident chambre {alarm['NumCh']}"
                        voix_ok = self.__send_voice(tel_num=alarm["Appel"], message=my_message)
                    else:
                        voix_ok = True

                    # send to HTTP Alarm server if present (NOT USED BY THE MOMENT)
                    if self.__serverURL:
                        http_ok = self.__send_http_post(room=alarm[2],
                                                        alarm_date=alarm[6],
                                                        message=alarm[3],
                                                        priority=alarm[7])
                    else:
                        http_ok = True

                    # sent to Firebase for Android App notification if defined
                    # if self.__fb and self.__fb["topic"] and self.__fb["key"]:
                    if self.__fb and (self.__fb['topic'] or alarm["CanalNotif"]) and self.__fb["key"]:
                        firebase_ok = self.__sendAndroidNotify(priority=alarm["Priorite"], user_topic=alarm["CanalNotif"])
                    else:
                        firebase_ok = True

                    # decrement counter, a l'avenir il faudra avoir un compteur par type d'envoi
                    if http_ok or voix_ok or firebase_ok:
                        self.__bd.set_alarm_counter(alarm_id, counts - 1)
                        if counts - 1 == 0:
                            del self.__lastSending[f"{alarm_id}"]
            time.sleep(self.REFRESH_TIME)

    def __sendAndroidNotify(self, priority:int, user_topic=None) -> bool:
        api_key = self.__fb['key']
        topic = self.__fb['topic']
        if user_topic:
            topic = user_topic
        push_service = FCMNotification(api_key=api_key)
        str_pr = "informative"
        color_code = "gris"
        if priority > 75:
            str_pr = "haute"
            color_code = "rouge"
        elif 50 < priority <= 75:
            str_pr = "moyenne"
            color_code = "orange"
        elif 25 < priority <= 50:
            str_pr = "basse"
            color_code = "jaune"

        message_body = f"Alarme SySPAD avec priorité {str_pr}, code couleur {color_code}."
        result = push_service.notify_topic_subscribers(topic_name=topic, message_title="SySPAD", message_body=message_body)
        if result['success'] > 0:
            logging.debug(f"ALARM_SENDER ==> Notification Android envoyé correctement")
            return True
        else:
            logging.debug(f"ALARM_SENDER ==> Erreur d'envoi sur FireBase Android")
            return False

    def __sendmail(self, mail_to: str, subject: str, message: str) -> bool:
        try:
            # Construction du message
            msg = EmailMessage()
            msg["From"] = self.__mail_from
            msg["To"] = mail_to
            msg["Subject"] = subject
            msg.set_payload(message.encode("utf8"))
            logging.debug(f"SMTP, envoi du mail {message}")
            # Envoi du message.
            s = smtplib.SMTP(self.__host, self.__port)
            s.login(self.__mail_from, self.__smtp_pass)
            s.send_message(msg)
            s.quit()
            ok = True
        except SMTPException:
            ok = False
            logging.error(f"ALARM_SENDER ==> SMTP, impossible de se connecter à {self.__host}:{self.__port}")
        return ok

    def __send_http_post(self, room: str, alarm_date: str, message: str, priority: int) -> bool:
        data = {
            "Chambre": room,
            "DateAlarme": alarm_date,
            "Message": message,
            "NiveauPriorite": str(priority),
        }
        reponse = requests.post(self.__serverURL, data=data).content
        dict_str = reponse.decode("UTF-8")
        mydata = json.loads(dict_str)
        status = mydata.get("status")
        if status != 200:
            logging.error(f"ALARM_SENDER => HTTP/POST ERROR: status={status}")
            return False
        logging.debug(f"ALARM_SENDER => HTTP/POS OK")
        return True

    def __send_sms(self, tel_num: str, message: str) -> bool:
        sender = 'Mobaspace'
        clase = "sms"
        metodo = "sendsms"
        scheduledatetime = ""
        ok = False
        s_url = "http://www.afilnet.com/api/http/?class=" + clase + "&method=" + metodo + \
                "&user=" + self.__sms_user + \
                "&password=" + self.__sms_password + \
                "&from=" + sender + "&to=" + tel_num + \
                "&sms=" + urllib.parse.quote_plus(message) + \
                "&scheduledatetime=" + scheduledatetime
        try:
            result = json.loads(urllib.request.urlopen(s_url).read().decode('utf-8'))
            if result and "status" in result:
                if result["status"] == "ERROR" and "error" in result:
                    ok = False
                    logging.error(f"SMS, erreur envoi à {tel_num} : {result['error']}")
                else:
                    ok = True
        except urllib.error.URLError as error:
            ok = False
            logging.error(f"SMS, erreur envoi à {tel_num} : {error}")
        return ok

    def __send_voice(self, tel_num: str, message: str) -> bool:
        sender = 'Mobaspace'
        clase = "voice"
        metodo = "sendvoice"
        scheduledatetime = ""
        language = "fr"
        ok = False
        s_url = "http://www.afilnet.com/api/http/?class=" + clase + "&method=" + metodo + \
                "&user=" + self.__sms_user + \
                "&password=" + self.__sms_password + \
                "&from=" + sender + "&to=" + tel_num + \
                "&message=" + urllib.parse.quote_plus((message + ". ")*self.__xm) + \
                "&scheduledatetime=" + scheduledatetime + \
                "&language=" + language
        try:
            result = json.loads(urllib.request.urlopen(s_url).read().decode('utf-8'))
            if result and "status" in result:
                if result["status"] == "ERROR" and "error" in result:
                    ok = False
                    logging.error(f"Voix, erreur envoi à {tel_num} : {result['error']}")
                else:
                    ok = True
        except urllib.error.URLError as error:
            ok = False
            logging.error(f"Voix, erreur envoi à {tel_num} : {error}")
        return ok
