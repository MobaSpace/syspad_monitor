# coding: utf8
"""
TODO

:author: Sergio
:date: 10/11/2020

"""
import ast
import json
import logging
from typing import Optional

import arrow
import pandas as pd
import requests

from syspad_monitor.database_encry import SysPadDb_encry


class MyWithingsApi:
    """
    Cette classe modelise l'API Withings
    """

    HR_SERIE = "heart_rate"
    BP_SERIE = "blood_presure"
    BATTERIE = "batterie_level"
    BM_SERIE = "body_mass"
    SL_SERIE = "sleep"

    def __init__(self,
                 bd: SysPadDb_encry,
                 withings_evt_url: str
                 ):
        self.__bd = bd
        self.withings_evt_url = withings_evt_url

    def set_notify(self, access_token: str) -> bool:
        all_ok = True
        applis_list = ['44', '50', '51']
        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        for appli in applis_list:
            if not self.needToSetURL(access_token, appli):
                logging.debug(f"API--> The callback URL = {self.withings_evt_url} is already SET for appli {appli}... Nothing TODO for this subscription!")
                continue

            data = {
                'action': 'subscribe',
                'callbackurl': self.withings_evt_url,
                'appli': appli,
                'comment': 'subscription'
            }

            response = requests.post('https://wbsapi.withings.net/notify', headers=headers, data=data)
            if response.status_code != requests.codes.ok:
                logging.error(f"API--> WGS SERVER ERROR: {response.status_code} on {response.url}")
                all_ok = False
                continue

            dict_str = response.content.decode("UTF-8")
            mydata = ast.literal_eval(dict_str)
            status = mydata.get("status")

            if status != 0:
                logging.error(f"API (appli = {appli})--> ERROR WGS SUBSCRIBE POST: {dict_str} for data={data}")
                all_ok = False
                continue

            logging.debug(f"API--> OK subscription for appli={appli} with request={data}")
        return all_ok

    def needToSetURL(self, access_token:str, appli: str) -> bool:
        """
        This function check if the URL in the conf file is the same already present in Withings Server
        Also it erases old subscriptions not matching the actual callback configuration
        :param access_token: the string with the last and valid access token
        :return: TRUE/FALSE (True also if any callback is defined in Withings - first time)
        """

        headers = {
            "Authorization": f"Bearer {access_token}",
        }
        data = {
            'action': 'list',
            'appli': appli
        }
        response = requests.post('https://wbsapi.withings.net/notify', headers=headers, data=data)
        if response.status_code != requests.codes.ok:
            logging.error(f"API--> WGS SERVER ERROR: {response.status_code} on {response.url}")
            return False

        dict_str = response.content.decode("UTF-8")
        mydata = ast.literal_eval(dict_str)
        status = mydata.get("status")
        if status != 0:
            logging.error(f"API--> ERROR WGS GETTING NOTI-LIST POST: {dict_str}")
            return False
        profiles = mydata.get("body").get("profiles")
        need = True
        if profiles:
            logging.debug(f"API--> There are {len(profiles)} different callbacks profiles for appli {appli}")
            for profile in profiles:
                oldURL = profile.get("callbackurl").replace('\\', '') # url is like that 'https:\\/\\/mysyspad.mobaspace.com\\/withings_evt'
                if oldURL != self.withings_evt_url:
                    self.unset_notify(access_token, oldURL, appli)
                else:
                    need = False
        return need

    def unset_notify(self, access_token: str, old_url:str, appli: str) -> bool:

        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        data = {
            'action': 'revoke',
            'callbackurl': old_url,
            'appli': appli,
        }
        response = requests.post('https://wbsapi.withings.net/notify', headers=headers, data=data)
        if response.status_code != requests.codes.ok:
            logging.error(f"API--> WGS SERVER ERROR: {response.status_code} on {response.url}")
            return False
        dict_str = response.content.decode("UTF-8")
        mydata = ast.literal_eval(dict_str)
        status = mydata.get("status")
        if status != 0:
            logging.error(f"API--> ERROR WGS REVOKE POST: {dict_str}")
            return False
        logging.debug(f"API--> OK REVOKE for {appli} with request {data}")

        return True

    def refresh_credentials(self, api_id: int):
        current_tokens = self.__bd.read_tokens_keys(api_id=api_id)[0]
        client_id = current_tokens['ClientId']
        client_sec = current_tokens['ClientSecret']
        refresh_token = current_tokens['RefreshToken']
        data = {
            "action": "requesttoken",
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_sec,
            "refresh_token": refresh_token,
        }
        response = requests.post("https://wbsapi.withings.net/v2/oauth2", data=data)
        if response.status_code != requests.codes.ok:
            logging.error(f"API--> WGS SERVER ERROR: {response.status_code}  on {response.url}")
            return
        dict_str = response.content.decode("UTF-8")
        new_creds = ast.literal_eval(dict_str)
        if new_creds.get("status") != 0:
            logging.error(
                f"La mise à jour des tokens n'a pas été possible pour l'API Id={api_id} -> error_code = {new_creds.get('status')}"
            )
            return
        new_creds = new_creds.get("body")
        access_token = new_creds.get("access_token")
        refresh_token = new_creds.get("refresh_token")
        token_expiry = new_creds.get("expires_in") + arrow.now().int_timestamp
        # ajout dans la BD
        self.__bd.update_token(
            api_id,
            access_token,
            refresh_token,
            token_expiry,
        )

        # s'abonner encore aux notifications
        self.set_notify(access_token=access_token)

    @staticmethod
    def get_sleep_allseries(access_token: str, startdate: arrow, enddate: arrow) -> Optional[pd.DataFrame]:
        """
        Cette fonction permet de recuperer les données de la nuit avec le tapis Sleep
        :param access_token:
        :param startdate:
        :param enddate:
        :return:
        """
        headers = {
            "Authorization": f"Bearer {access_token}",
        }
        params = dict(action="get", startdate=f"{startdate.int_timestamp}",
                      enddate=f"{enddate.int_timestamp}",
                      data_fields="hr,rr,snoring")
        response = requests.get(
            "https://wbsapi.withings.net/v2/sleep", headers=headers, params=params
        )
        if response.status_code != requests.codes.ok:
            logging.error(f"API--> WGS SERVER ERROR: {response.status_code}  on {response.url}")
            return None
        dict_str = response.content.decode("UTF-8")
        mydata = ast.literal_eval(dict_str)
        status = mydata.get("status")
        if status != 0:
            logging.error(f"ERROR WGS SLEEP-DATA GET: {dict_str}")
            return None
        mydata = mydata.get("body").get("series")
        if len(mydata) < 1:
            logging.error(f"NO SLEEP-DATA for day {startdate}")
            return None
        time_list = []
        sleep_state_list = []
        hr_list = []
        rr_list = []
        snoring_list = []
        for ii in mydata:
            start_time = ii["startdate"]
            endd_time = ii["enddate"]
            state = ii["state"]
            hr = ii.get("hr")
            rr = ii.get("rr")
            snoring = ii.get("snoring")
            for jj in range(start_time, endd_time, 60):
                current_time = jj
                time_list.append(current_time)
                sleep_state_list.append(state)
                hr_list.append(hr.get(f"{current_time}"))
                rr_list.append(rr.get(f"{current_time}"))
                snoring_list.append(snoring.get(f"{current_time}"))
        sleep_data_frame = pd.DataFrame(
            {
                "Time": time_list,
                "SleepState": sleep_state_list,
                "HR": hr_list,
                "RR": rr_list,
                "SN": snoring_list,
            }
        )
        # les donnes ranges par timeStamp
        sleep_data_frame = sleep_data_frame.sort_values("Time").reset_index(drop=True)
        return sleep_data_frame

    @staticmethod
    def get_sleep_summary(access_token: str, startdate: arrow, enddate: arrow) -> Optional[dict]:
        """
        Cette fonction permet de recuperer les données de la nuit avec le tapis Sleep
        :param access_token:
        :param startdate:
        :param enddate:
        :return:
        """

        headers = {
            "Authorization": f"Bearer {access_token}",
        }
        all_fields = ("breathing_disturbances_intensity,deepsleepduration"
                      ",durationtosleep,durationtowakeup,hr_average,hr_max"
                      ",hr_min,lightsleepduration,remsleepduration,rr_average"
                      ",rr_max,rr_min,sleep_score,snoring,snoringepisodecount"
                      ",wakeupcount,wakeupduration"
                      )

        params = dict(action="getsummary", startdateymd=startdate.format('YYYY-MM-DD'),
                      enddateymd=enddate.format('YYYY-MM-DD'), data_fields=all_fields)
        response = requests.get(
            "https://wbsapi.withings.net/v2/sleep", headers=headers, params=params
        )
        if response.status_code != requests.codes.ok:
            logging.error(f"API--> WGS SERVER ERROR: {response.status_code}  on {response.url}")
            return None
        dict_str = response.content.decode("UTF-8")
        mydata = json.loads(dict_str)
        status = mydata.get("status")
        if status != 0:
            logging.error(f"ERROR WGS SLEEP-SUMMARY GET: {dict_str}")
            return None
        if len(mydata.get("body").get("series")) < 1:
            logging.error(f"NO DATA for day {startdate.format('YYYY-MM-DD')}")
            return None
        datedebut = mydata.get("body").get("series")[-1].get("startdate")
        datefin = mydata.get("body").get("series")[-1].get("enddate")
        mydata = mydata.get("body").get("series")[-1].get("data")
        summary = {'DateDebut': datedebut,
                   'DateFin': datefin,
                   'FCMoy': mydata.get('hr_average'),
                   'FCMax': mydata.get('hr_max'),
                   'FCMin': mydata.get('hr_min'),
                   'FRMoy': mydata.get('rr_average'),
                   'FRMax': mydata.get('rr_max'),
                   'FRMin': mydata.get('rr_min'),
                   'ScoreNuit': mydata.get('sleep_score'),
                   'NbReveils': mydata.get('wakeupcount'),
                   'DureeReveilAuLit': mydata.get('wakeupduration'),
                   'DureeSommeil': (mydata.get('remsleepduration')
                                    + mydata.get('lightsleepduration')
                                    + mydata.get('deepsleepduration'))
                   }
        return summary
