import logging
import time
from threading import Thread

from syspad_monitor.database_encry import SysPadDb_encry
from syspad_monitor.model.oauth_api import MyWithingsApi


class TokensUpdater(Thread):
    # 5 minutes
    REFRESH_TIME = 300

    def __init__(self, database: SysPadDb_encry, api: MyWithingsApi):
        self.__thread_name = f"TOKENS"
        Thread.__init__(self, name=self.__thread_name, target=self.run)
        self.__bd = database
        self.__api = api

    def run(self):
        logging.info(f"Le process {self.__thread_name} a démarré")
        while True:
            # obtention de la liste d'API qui vont périmer ou sont déjà perimées
            expired_list = self.__bd.get_expired_api_ids()
            logging.debug(
                f"{self.__thread_name} --> Il y a {len(expired_list)} APIS à mettre à jour"
            )
            for elem in expired_list:
                api_id = int(elem[0])
                logging.debug(f"{self.__thread_name} --> Mise à jour API={api_id}")
                self.__api.refresh_credentials(api_id=api_id)
            time.sleep(self.REFRESH_TIME)
