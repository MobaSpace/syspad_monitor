# coding: utf8
"""
Classe pour gérer la base de données de SysPAD
:author: all
"""

import json
import logging
import threading
import time
from datetime import datetime, timedelta
import arrow
from typing import Union

import psycopg2
import psycopg2.extensions
import psycopg2.extras

from syspad_monitor.task import PatientConst


class SysPadDb:
    """ Classe pour gérer la base de donnée Mobaspace """
    def __init__(self, params: dict, alarm_times: int):
        """ Initialize the database instance

            :param params: the database connection parameters
            :type params: dict
        """
        self.__cnx = None
        self.__params = params
        self.__connected = False
        # heart beat thread
        self.__thread = threading.Thread(target=self.__heart_beat, args=())
        self.__thread.daemon = True
        self.__atimes = alarm_times

    def connect(self):
        """ Connect to the PostgreSQL database server for compute tasks
        """
        logging.info(
            f"BASE DONNEES --> connecting to {self.__params['name']} on {self.__params['host']} "
        )
        try:
            self.__cnx = psycopg2.connect(
                "host={host} port={port} user={user} password={pwd} dbname={name} \
                 application_name=syspad".format(**self.__params)
            )
            self.__connected = True
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)

    def disconnect(self):
        """ Close database connexion
        """
        logging.info("BASE DONNEES --> disconnecting")
        if self.__connected:
            self.__cnx.close()
            self.__connected = False
            logging.info("Database, disconnected")

    def set_lastdata_processed(self, patient_id: int, timeStamp: int) -> None:
        if not self.__connected:
            self.connect()
        update = ' UPDATE mobaspace_data."Patients" SET "DernierIndiceTraite"=%s WHERE "Id"=%s;'
        logging.debug(f"BASE DONNEES --> mise à jour du dernier indice {timeStamp} traité pour {patient_id}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(update, (timeStamp, patient_id))
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def get_lastdata_processed(self, patient_id: int) -> int:
        """
        Récupération de la date timeStamp de la dernière donnée traitée
        :param patient_id:
        :return:
        """
        if not self.__connected:
            self.connect()
        # création de la requête SQL
        select = """SELECT "DernierIndiceTraite" FROM mobaspace_data."Patients" WHERE "Id" = %s"""
        logging.debug(
            f"BASE DONNEES --> récupération de la dernière data traitée sur le patient {patient_id}"
        )
        cur = None
        result = dict()
        try:
            cur = self.__cnx.cursor()
            cur.execute(select, (patient_id,))
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result[0][0]

    def set_alarm_counter(self, alarm_id: int, counter_val: int):
        if not self.__connected:
            self.connect()
        update = ' UPDATE mobaspace_data."Alarmes" SET "NbNotifications"=%s WHERE "Id"=%s;'
        logging.debug(f"BASE DONNEES --> mise à jour du compteur de l'alarme {alarm_id}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(update, (counter_val, alarm_id))
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def read_alarms(self):
        if not self.__connected:
            return None
        select = """
            SELECT "Alarmes"."Id", "NbNotifications", "Chambre", "Description", "EmailAlarmes", "SMS",
             "Alarmes"."Creation", "Alarmes"."Priorite", "Appel"
            FROM mobaspace_data."Alarmes" 
            INNER JOIN mobaspace_data."Patients" ON
             (mobaspace_data."Alarmes"."PatientId" = mobaspace_data."Patients"."Id")
            INNER JOIN mobaspace_data."ContactsPatients" ON
             (mobaspace_data."Patients"."Id" = mobaspace_data."ContactsPatients"."PatientId")
            INNER JOIN mobaspace_data."AspNetUsers"
             ON (mobaspace_data."ContactsPatients"."UtilisateurId" = mobaspace_data."AspNetUsers"."Id")
            WHERE "NbNotifications" > 0;
        """
        logging.debug(f"BASE DONNEES --> récupération des alarmes restant à traiter")
        cur = None
        result = dict()
        try:
            cur = self.__cnx.cursor()
            cur.execute(select)
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def get_uri_for_Observable(self, libelle:str) -> str:
        select = """
            SELECT "URI" 
            FROM mobaspace_data."TypeObservable"
            WHERE "Type" = %s;
            """
        logging.debug(f"BASE DONNEES --> récupération de l'uri pour l'observable {libelle}")
        cur = None
        result = dict()
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (libelle,))
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result


    def update_uri_typeObservable(self, libelle:str, new_uri:str):
        """
        Fonction pour mettre à jour les URI des différents types d'observables
        :param libelle: le libelle de l'observable correspondant à Type
        :param new_uri: le uri de l'observable chez NetSOINS
        :return:
        """
        if not self.__connected:
            self.connect()
        insert = (
            'INSERT INTO mobaspace_data."TypeObservable"'
            ' ("Type", "URI")'
            ' VALUES(%s, %s)'
            ' ON CONFLICT ("Type") DO UPDATE'
            ' SET "URI" = EXCLUDED."URI"'
        )
        logging.debug(f"BASE DONNEES --> insertion nouveau type d'observable NetSOINS {libelle}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(
                insert,
                (
                    libelle,
                    new_uri,
                ),
            )
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(f"BASE DONNEES pgerror {error}")
            except AttributeError:
                logging.error("BASE DONNEES AttributeError")
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()

    def get_all_observables(self) -> Union[list, psycopg2.extras.DictCursor]:
        """
        Fonction pour recupérer tous les observables non traités
        :return:
        """
        select = """SELECT "Observables"."Id", "TypeObservable"."Type", "TypeObservable"."URI", "Valeurs", "Date", "Chambre" 
            FROM mobaspace_data."Observables"
            INNER JOIN mobaspace_data."TypeObservable"
            ON (mobaspace_data."Observables"."TypeId" = mobaspace_data."TypeObservable"."Id")
            WHERE "ObservableTraite" = FALSE;"""
        logging.debug(f"BASE DONNEES --> récupération des observables restant à envoyer vers NetSOINS")
        cur = None
        result = dict()
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select)
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def erase_observable(self, obs_id: int):
        if not self.__connected:
            self.connect()
        delete = (
            'DELETE from mobaspace_data."Observables"'
            ' WHERE "Id"=%s'
        )
        logging.debug(f"BASE DONNEES --> effacage de l'observable {obs_id}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(delete, (obs_id,) )
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def change_obs_flag(self, obs_id: int, flag=True):
        """
        Cette fonction change la valeur du flag 'Traite' dans la table d'observables
        :param obs_id: l'identifiant de la ligne
        :return:
        """
        if not self.__connected:
            self.connect()
        update = (
            'UPDATE mobaspace_data."Observables"'
            ' SET "ObservableTraite"=%s'
            ' WHERE "Id"=%s'
        )
        logging.debug(f"BASE DONNEES --> mise à jour du flag de l'observable {obs_id}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(update, (flag, obs_id))
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def set_observable(self, type_obs: str, values: [], room: int):
        """
        Cette fonction permet d'insérer un nouveau observable dans la BD SySPAD
        :param type_obs: le type d'observation par ex. 'temperature'
        :param values: les valeurs, c'est une liste qui peut contenir différents types int, float, str
        :param room: la chambre
        :return:
        """
        pat_id = self.get_patId_from_room(room=room)
        obs_id = self.get_obsTypeId(type=type_obs)
        my_json = json.dumps(values)
        my_date = datetime.now()

        if not self.__connected:
            self.connect()
        insert = (
            'INSERT INTO mobaspace_data."Observables" '
            ' ("TypeId", "Valeurs", "Date", "PatId", "Chambre") '
            ' VALUES(%s, %s, %s, %s, %s);'
        )
        logging.debug(f"BASE DONNEES --> insertion d'un nouveau observable")
        cur = None
        # le nombre de notifications par défaut est mis à 3
        try:
            cur = self.__cnx.cursor()
            cur.execute(
                insert,
                (
                    obs_id,
                    my_json,
                    my_date,
                    pat_id,
                    room
                ),
            )
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()

    def get_obsTypeId(self, type:str) -> int:
        """
        Recupère le ID du type d'observable donné en entrée
        :param type:
        :return:
        """
        if not self.__connected:
            self.connect()
        select = """SELECT "Id" FROM mobaspace_data."TypeObservable" WHERE "Type" = %s"""
        logging.debug(f"BASE DONNEES --> récupération Id de l'observable {type}")
        cur = None
        result = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(select, (type,))
            result = cur.fetchone()[0]
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
                return result

    def existingTransaction(self, uri:str) -> str:
        if not self.__connected:
            self.connect()
        select = """
        SELECT "URI" FROM mobaspace_data."Transactions"
        WHERE "DetailRetour" = %s
        """
        cur = None
        result = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(select, (uri,))
            result = cur.fetchone()[0]
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
                return result

    def insert_transaction(self, code:str, uri:str, detail=None):
        if not self.__connected:
            self.connect()
        insert = (
            'INSERT INTO mobaspace_data."Transactions" '
            ' ("Date", "URI", "CodeRetour", "DetailRetour") '
            ' VALUES(%s, %s, %s, %s);'
        )
        logging.debug(f"BASE DONNEES --> insertion transaction")
        cur = None
        # le nombre de notifications par défaut est mis à 3
        try:
            cur = self.__cnx.cursor()
            cur.execute(
                insert,
                (
                    datetime.now(),
                    uri,
                    code,
                    detail
                ),
            )
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()

    def get_patId_from_room(self, room:int) -> int:
        """
        Recupère le ID d'un patient
        :param room: la chambre du patient
        :return: le ID
        """
        if not self.__connected:
            self.connect()

        # création de la requête SQL
        select = """SELECT "Id" FROM mobaspace_data."Patients" WHERE "NumCh" = %s"""
        logging.debug(f"BASE DONNEES --> récupération Id du patient de la chambre {room}")
        cur = None
        result = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(select, (room,))
            result = cur.fetchone()[0]
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
                return result

    def get_patient_histo(self, patient_id: int) -> dict:
        """
        Récupération de l'objet dict JSON du patient en question
        :param patient_id:
        :return:
        """
        if not self.__connected:
            self.connect()

        # création de la requête SQL
        select = """SELECT "Historique" FROM mobaspace_data."Patients" WHERE "Id" = %s"""
        logging.debug(f"BASE DONNEES --> récupération historique du patient {patient_id}")
        cur = None
        result = dict()
        try:
            cur = self.__cnx.cursor()
            cur.execute(select, (patient_id,))
            result = cur.fetchone()[0]
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
                return result

    def get_patient_from_wgs_user(self, wgs_user_id: str) -> []:
        if not self.__connected:
            self.connect()

        # création de la requête SQL
        select = """
        SELECT "PatientId" FROM mobaspace_data."OAuth2Apis"
        WHERE "ApiUserId"=%s
        """

        logging.debug(
            f"BASE DONNEES --> Correspondance Patient <--> Withings user"
        )
        cur = None
        result = dict()
        try:
            cur = self.__cnx.cursor()
            cur.execute(select, (wgs_user_id,))
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def update_patient_lying_day_time(self, patient_id, amount: int):
        update = (
            'UPDATE mobaspace_data."Patients"'
            ' SET "CumulTempsAllonge"=%s'
            ' WHERE "Id"=%s'
        )
        logging.debug(f"BASE DONNEES --> mise à jour Compteur-Jour du patient {patient_id},"
                      f" nouvelle valeur {amount}secs")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(update, (amount, patient_id))
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def update_patient_posture(self, patient_id: int, posture: int):
        update = (
            'UPDATE mobaspace_data."Patients"'
            ' SET "Posture"=%s'
            ' WHERE "Id"=%s'
        )
        logging.debug(f"BASE DONNEES --> mise à jour Posture du patient {patient_id}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(update, (posture, patient_id))
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def get_id_sensor_for_pat(self, patient_id: int):
        select = """
        SELECT "CapteurId" FROM mobaspace_data."ApisCapteurs"
        INNER JOIN mobaspace_data."OAuth2Apis" on ("OAuth2Apis"."Id" = "ApiId")
        WHERE "PatientId"=%s
        """
        cur = None
        result = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(select, (patient_id,))
            result = cur.fetchone()[0]
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def update_patient_histo(self, patient_id: int, histo: json, flag=True):
        update = (
            'UPDATE mobaspace_data."Patients"'
            ' SET "Historique"=%s, "NouvellesDonneesHisto"=%s'
            ' WHERE "Id"=%s'
        )

        logging.debug(f"BASE DONNEES --> mise à jour de l'historique du partient {patient_id}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(update, (histo, flag, patient_id))
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def update_bed_notify(self, patient_id: int, bed_type: int, noti_date: int):
        if bed_type == PatientConst.BED_IN.value:
            update = (
                'UPDATE mobaspace_data."Patients"'
                ' SET "NouvellesDonneesLit"=%s , "DernierCoucher"=to_timestamp(%s)'
                ' WHERE "Id"=%s'
            )
        else:
            update = (
                'UPDATE mobaspace_data."Patients" '
                ' SET "NouvellesDonneesLit"=%s , "DernierLever"=to_timestamp(%s)'
                ' WHERE "Id"=%s'
            )
        logging.debug(
            f"BASE DONNEES --> mise à jour pour BED_IN/OUT ({bed_type}) du partient {patient_id}"
        )
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(update, (True, noti_date, patient_id))
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def check_patient_flags(self, patient_id: int) -> []:
        if not self.__connected:
            self.connect()
        select = """SELECT "NouvellesDonneesHisto", "NouvellesDonneesLit"
            FROM mobaspace_data."Patients" 
            WHERE "Id" = %s"""
        logging.debug(f"BASE DONNEES --> vérification FLAGS du patient {patient_id}")
        cur = None
        result = []
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (patient_id,))
            result = cur.fetchone()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
                return result

    def change_patient_flag(self, patient_id: int, flag: bool, mytype: str):
        """
        Permet de changer le flag d'un patient, normalement utilisé par le process qui traite les données
        :param patient_id:
        :param flag: True ou False mais normalement
        :param mytype:
        :return:
        """
        if mytype == PatientConst.bed_data.value:
            update = (
                'UPDATE mobaspace_data."Patients" SET "NouvellesDonneesLit"=%s WHERE "Id"=%s'
            )
        elif mytype == PatientConst.sleep_data.value:
            update = (
                'UPDATE mobaspace_data."Patients" '
                ' SET "NouvellesDonneesHisto"=%s '
                ' WHERE "Id"=%s'
            )
        elif mytype == PatientConst.tracker_data.value:
            update = (
                'UPDATE mobaspace_data."Patients" '
                ' SET "NouvellesDonneesTracker"=%s '
                ' WHERE "Id"=%s'
            )
        else:
            update = (
                'UPDATE mobaspace_data."Patients" SET "NouvellesDonneesBPM"=%s WHERE "Id"=%s'
            )

        logging.debug(
            f"BASE DONNEES --> mise à jour du Flag NouvellesDonnées {type} du partient {patient_id}"
        )
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(update, (flag, patient_id))
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def get_last_bed_events(self, patient_id: int):
        if not self.__connected:
            self.connect()
        # création de la requête SQL
        select = """SELECT "DernierCoucher", "DernierLever", "NouvellesDonneesLit"
            FROM mobaspace_data."Patients" WHERE "Id" = %s"""
        logging.debug(
            f"BASE DONNEES --> récupération des derniers évenements lit du patient {patient_id}"
        )
        cur = None
        result = dict()
        try:
            cur = self.__cnx.cursor()
            cur.execute(select, (patient_id,))
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
                return result

    def is_api_in_table(self, api_id: int) -> bool:
        result = False
        if not self.__connected:
            self.connect()

        select = """SELECT EXISTS(SELECT 1 FROM mobaspace_data."OAuth2Apis"
            WHERE "Id"=%s AND "PatientId" IS NOT NULL LIMIT 1)"""
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(select, (api_id,))
            result = cur.fetchall()[0][0]
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return result

    def get_patient_limits(self, pat_id: int):
        if not self.__connected:
            self.connect()
        select = """ SELECT "FreqCardiaqueMin_bpm", "FreqCardiaqueMax_bpm", "FreqRespMin_bpm",
            "FreqRespMax_bpm", "Coucher_h", "Coucher_min", "Lever_h", "Lever_min", DureeMaxHorsLit_min"
            FROM mobaspace_data."Patients" WHERE "Id"=%s """
        logging.debug(f"BASE DONNEES --> récupération des limites du patient {pat_id}")
        cur = None
        result = dict()
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (pat_id,))
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.Error as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def read_one_patient(self, pat_id: int) -> Union[list, psycopg2.extras.DictCursor]:
        result = []
        if not self.__connected:
            self.connect()
        select = """ SELECT * FROM mobaspace_data."Patients" WHERE "Id"=%s """
        logging.debug("BASE DONNEES --> récupération d'un patients")
        cur = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (pat_id,))
            result = cur.fetchone()
            self.__cnx.commit()
        except psycopg2.Error as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def read_all_patients(self) -> Union[list, psycopg2.extras.DictCursor]:
        """
        Lecture de la liste des patients

        :return: liste des patients
        :rtype: json
        """
        result = []
        if not self.__connected:
            self.connect()
        select = """ SELECT * FROM mobaspace_data."Patients" """
        logging.debug("BASE DONNEES --> récupération de la liste des patients")
        cur = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select)
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.Error as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def read_pat_id_from_apis(self) -> Union[list, psycopg2.extras.DictCursor]:
        """
        Lecture de la liste des patients

        :return: liste des patients
        :rtype: json
        """
        result = []
        if not self.__connected:
            self.connect()
        select = """ SELECT "PatientId" FROM mobaspace_data."OAuth2Apis" WHERE "PatientId" IS NOT NULL """
        logging.debug("BASE DONNEES --> récupération liste patients liés à des APIs")
        cur = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select)
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.Error as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def read_tokens_keys(self, api_id: int) -> Union[list, psycopg2.extras.DictCursor]:
        result = []
        if not self.__connected:
            self.connect()
        select = """ SELECT "AccessToken", "RefreshToken", "ClientId",
            "ClientSecret"
            FROM mobaspace_data."OAuth2Apis" WHERE "Id" = %s """
        logging.debug(
            f"BASE DONNEES --> récupération tokens API={api_id}"
        )
        cur = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (api_id,))
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def read_patient_api(self, patient_id: int, provider="Withings") -> []:
        """
        Lecture des API liés au patient

        :param patient_id: numéro du patient
        :param provider: le provides Withings/Mobaspace
        :type patient_id: int
        :return: liste des API
        :rtype: list
        """
        result = []
        if not self.__connected:
            self.connect()
        select = """ SELECT "Id", "Provider", "AccessToken", "RefreshToken", "ExpirationDate", "ApiUserId", "ClientId",
            "ClientSecret"
            FROM mobaspace_data."OAuth2Apis" WHERE "PatientId" = %s AND "Provider" = %s """
        logging.debug(
            f"BASE DONNEES --> récupération de la liste des API pour le patient {patient_id}"
        )
        cur = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (patient_id, provider,))
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def get_expired_api_ids(self, provider="Withings"):
        """
        Cette fonction retourne les Id des apis qui ont un access_token qui va expirer dans 15min
        :return: liste avec les Id des apis concernées
        """
        result = []
        if not self.__connected:
            self.connect()
        my_date = datetime.now() + timedelta(minutes=15)
        select = """ 
            SELECT "Id"
            FROM mobaspace_data."OAuth2Apis" 
            WHERE "ExpirationDate" < %s 
            AND "Provider" = %s;
            """
        logging.debug(f"BASE DONNEES --> récupération des APIs arrivant à expiration")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(select, (my_date, provider,))
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def set_sensor_state(self, sensor_id:int, state:bool):
        if not self.__connected:
            self.connect()
        update = (
            'UPDATE mobaspace_data."Capteurs" SET "EtatOK"=%s WHERE "Id"=%s'
        )
        logging.debug(f"BASE DONNEES --> mise à jour état Capteur {sensor_id}, nouvelle valeur {state}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(update, (state, sensor_id))
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def read_sensors(self, patient_id: int, type: str) -> Union[list, psycopg2.extras.DictCursor]:
        """
        Lecture de la liste des capteurs pour un patient

        :type patient_id: int
        :return: liste des capteurs avec leur types
        :rtype: list
        """
        result = []
        if not self.__connected:
            self.connect()
        select = """SELECT mobaspace_data."Capteurs"."Id", mobaspace_data."Capteurs"."EtatOK" 
                FROM mobaspace_data."Capteurs"
                INNER JOIN mobaspace_data."ApisCapteurs" on (mobaspace_data."Capteurs"."Id" = "CapteurId")
                INNER JOIN mobaspace_data."OAuth2Apis" on ("ApiId" = mobaspace_data."OAuth2Apis"."Id")
                WHERE mobaspace_data."OAuth2Apis"."PatientId" = %s
                AND   mobaspace_data."Capteurs"."Type"= %s """
        logging.debug(f"BASE DONNEES --> récupération du Capteure de Type={type} associé au Patient={patient_id}")
        cur = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (patient_id, type,))
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def update_token(self, api_id: int, access_token: str, refresh_token: str, expiry_date: float):
        """
        Mets à jour les tokens d'une API
        :param api_id: l'identificateur API
        :param access_token: le nouveau access token
        :param refresh_token: le nouveau refresh token
        :param expiry_date: elle est donnée en TimeStamp
        :return:
        """
        update = (
            'UPDATE mobaspace_data."OAuth2Apis" '
            ' SET "AccessToken"=%s, "RefreshToken"=%s, "ExpirationDate"=%s '
            ' WHERE "Id"=%s'
        )
        logging.debug(
            f"BASE DONNEES --> mise à jour des tokens API={api_id} : access={access_token},"
            f" refresh={refresh_token}"
        )
        cur = None
        my_date = datetime.fromtimestamp(expiry_date)
        try:
            cur = self.__cnx.cursor()
            cur.execute(update, (access_token, refresh_token, my_date, api_id))
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def insert_alarm(self, alarm):
        """ Insere une alarme dans la base de donnée
            :param alarm: L'alarme à insérer
        """
        if not self.__connected:
            self.connect()
        insert = (
            'INSERT INTO mobaspace_data."Alarmes" '
            ' ("CapteurId", "PatientId", "Priorite", "Description", '
            ' "Creation", "NbNotifications") '
            " VALUES(%s, %s, %s, %s, %s, %s);"
        )
        counts = 0
        if alarm["priorite"] > 75:
            counts = self.__atimes
        create_date = datetime.now()
        logging.debug(f"BASE DONNEES --> insertion de l'alarme {alarm['desc']} à {create_date}")
        cur = None
        # le nombre de notifications par défaut est mis à 3
        try:
            cur = self.__cnx.cursor()
            cur.execute(
                insert,
                (
                    alarm["id_capteur"],
                    alarm["id_patient"],
                    alarm["priorite"],
                    alarm["desc"],
                    create_date,
                    counts,
                ),
            )
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()

    def insert_day_total_lying_time(self, patient: int, date_jour: str, temps_allonge_total: int):
        if not self.__connected:
            self.connect()
        insert = (
            'INSERT INTO mobaspace_data."Jours"'
            ' ("PatientId", "DateJour", "TempsAllongeTotal")'
            ' VALUES(%s, %s, %s)'
            ' ON CONFLICT ("PatientId", "DateJour") DO UPDATE'
            ' SET "TempsAllongeTotal" = EXCLUDED."TempsAllongeTotal"'
        )
        logging.debug(f"BASE DONNEES --> insertion des données de JOUR patient {patient} pour {date_jour}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(
                insert,
                (
                    patient,
                    date_jour,
                    temps_allonge_total,
                ),
            )
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(f"BASE DONNEES pgerror {error}")
            except AttributeError:
                logging.error("BASE DONNEES AttributeError")
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()

    def insert_day_steeps(self, patient: int, date_jour: str, steeps: int):
        if not self.__connected:
            self.connect()
        insert = (
            'INSERT INTO mobaspace_data."Jours"'
            ' ("PatientId", "DateJour", "NbPas")'
            ' VALUES(%s, %s, %s)'
            ' ON CONFLICT ("PatientId", "DateJour") DO UPDATE'
            ' SET "NbPas" = mobaspace_data."Jours"."NbPas" + EXCLUDED."NbPas"'
        )
        logging.debug(f"BASE DONNEES --> insertion des données de JOUR patient {patient} pour {date_jour}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(
                insert,
                (
                    patient,
                    date_jour,
                    steeps,
                ),
            )
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(f"BASE DONNEES pgerror {error}")
            except AttributeError:
                logging.error("BASE DONNEES AttributeError")
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()

    def insert_sleep_summary(self, patient: int, date_nuit: str, data: dict):
        """ Insère ou met à jour les données de nuit dans la base de donnée
            :param patient: id patient
            :param date_nuit: date de la nuit
            :param data: les données venant de getsummary
        """
        if not self.__connected:
            self.connect()
        insert = (
            'INSERT INTO mobaspace_data."Nuits"'
            ' ("PatientId", "DateNuit", "DateDebut", "DateFin", "FCMoy", "FCMax", "FCMin", "FRMoy", "FRMax", "FRMin",'
            ' "ScoreNuit", "NbReveils", "DureeReveilAuLit", "DureeSommeil", "NuitTraitee")'
            ' VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,'
            ' make_interval(0,0,0,0,0,0,%s), make_interval(0,0,0,0,0,0,%s), False)'
            ' ON CONFLICT ("PatientId", "DateNuit") DO UPDATE'
            ' SET "DateDebut" = EXCLUDED."DateDebut", "DateFin" = EXCLUDED."DateFin", "FCMoy" = EXCLUDED."FCMoy"'
            ', "FCMax" = EXCLUDED."FCMax", "FCMin" = EXCLUDED."FCMin"'
            ', "FRMoy" = EXCLUDED."FRMoy", "FRMax" = EXCLUDED."FRMax", "FRMin" = EXCLUDED."FRMin"'
            ', "ScoreNuit" = EXCLUDED."ScoreNuit", "NbReveils" = EXCLUDED."NbReveils"'
            ', "DureeReveilAuLit" = EXCLUDED."DureeReveilAuLit"'
            ', "DureeSommeil" = EXCLUDED."DureeSommeil"'
            ', "NuitTraitee" = EXCLUDED."NuitTraitee"'
        )
        logging.debug(f"BASE DONNEES --> insertion des données de NUIT patient {patient} pour {date_nuit}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(
                insert,
                (
                    patient,
                    date_nuit,
                    datetime.utcfromtimestamp(data["DateDebut"]),
                    datetime.utcfromtimestamp(data["DateFin"]),
                    data["FCMoy"],
                    data["FCMax"],
                    data["FCMin"],
                    data["FRMoy"],
                    data["FRMax"],
                    data["FRMin"],
                    data["ScoreNuit"],
                    data["NbReveils"],
                    data["DureeReveilAuLit"],
                    data["DureeSommeil"],
                ),
            )
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(f"BASE DONNEES pgerror {error}")
            except AttributeError:
                logging.error("BASE DONNEES AttributeError")
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()

    def insert_bedouts_summary(self, patient: int, date_nuit: str, bedouts_summ: dict):
        if not self.__connected:
            self.connect()
        insert = (
            'INSERT INTO mobaspace_data."Nuits"'
            ' ("PatientId", "DateNuit",'
            ' "NbSorties", "DureeReveilHorsLit", "DetailSorties", "NuitTraitee")'
            ' VALUES(%s, %s, %s, make_interval(0,0,0,0,0,0,%s), %s, False)'
            ' ON CONFLICT ("PatientId", "DateNuit") DO UPDATE'
            ' SET "NbSorties" = EXCLUDED."NbSorties", "DureeReveilHorsLit" = EXCLUDED."DureeReveilHorsLit", '
            ' "DetailSorties" = EXCLUDED."DetailSorties", '
            ' "NuitTraitee" = EXCLUDED."NuitTraitee"'
        )
        logging.debug(f"BASE DONNEES --> insertion des sorties-lit du patient {patient} pour {date_nuit}")
        cur = None
        try:
            logging.debug(f'BASE DONNEES insertion des sorties-lit {bedouts_summ["counts"]}')
            cur = self.__cnx.cursor()
            cur.execute(
                insert,
                (
                    patient,
                    date_nuit,
                    int(bedouts_summ["counts"]),
                    int(bedouts_summ["total_secs"]),
                    json.dumps(bedouts_summ["details"]),
                ),
            )
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(f"BASE DONNEES pgerror {error}")
            except AttributeError:
                logging.error("BASE DONNEES AttributeError")
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()

    def get_FR_from_nights(self, id:int) -> Union[list, psycopg2.extras.DictCursor]:
        result = []
        if not self.__connected:
            self.connect()
        select = """
                SELECT "Nuits"."Id", "DateDebut", "DateFin",
                "FRMin", "FRMoy", "FRMax",
                "NumCh" 
                FROM mobaspace_data."Nuits"
                INNER JOIN mobaspace_data."Patients" on (mobaspace_data."Nuits"."PatientId" = mobaspace_data."Patients"."Id")
                WHERE "Nuits"."Id" = %s
                """
        logging.debug(f"BASE DONNEES --> récupération données sommeil nuit")
        cur = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (id,))
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def get_FC_from_nights(self, id:int) -> Union[list, psycopg2.extras.DictCursor]:
        result = []
        if not self.__connected:
            self.connect()
        select = """
                SELECT "Nuits"."Id", "DateDebut", "DateFin",
                "FCMin", "FCMoy", "FCMax",
                "NumCh" 
                FROM mobaspace_data."Nuits"
                INNER JOIN mobaspace_data."Patients" on (mobaspace_data."Nuits"."PatientId" = mobaspace_data."Patients"."Id")
                WHERE "Nuits"."Id" = %s
                """
        logging.debug(f"BASE DONNEES --> récupération données sommeil nuit")
        cur = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (id,))
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def get_SLEEP_from_nights(self, id:int) -> Union[list, psycopg2.extras.DictCursor]:
        result = []
        if not self.__connected:
            self.connect()
        select = """
                SELECT "Nuits"."Id", "DateDebut", "DateFin", "ScoreNuit",
                "DureeReveilAuLit", "DureeReveilHorsLit", "Nuits"."DureeSommeil",
                "NbSorties", "DetailSorties", "NumCh" 
                FROM mobaspace_data."Nuits"
                INNER JOIN mobaspace_data."Patients" on (mobaspace_data."Nuits"."PatientId" = mobaspace_data."Patients"."Id")
                WHERE "Nuits"."Id" = %s
                """
        logging.debug(f"BASE DONNEES --> récupération données sommeil nuit")
        cur = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (id,))
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result



    def erase_old_nights(self, before_days:int):
        date_lim = datetime.now().date() - timedelta(days=before_days)
        if not self.__connected:
            self.connect()
        delete = (
            'DELETE from mobaspace_data."Nuits"'
            ' WHERE "DateNuit"<%s'
        )
        logging.debug(f"BASE DONNEES --> effacage des nuits antérieurs à {date_lim}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(delete, (date_lim,) )
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def erase_night(self, id:int) -> None:
        if not self.__connected:
            self.connect()
        delete = (
            'DELETE from mobaspace_data."Nuits"'
            ' WHERE "Id"=%s'
        )
        logging.debug(f"BASE DONNEES --> effacage de la nuit {id}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(delete, (id,) )
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def update_night_flagTraitee(self, night_id:int, flag=True):
        if not self.__connected:
            self.connect()
        update = (
            'UPDATE mobaspace_data."Nuits" '
            ' SET "NuitTraitee"=%s '
            ' WHERE "Id"=%s'
        )
        logging.debug(
            f"BASE DONNEES --> mise à jour flag NuitTraitee={flag} pour la nuit avec Id={night_id}"
        )
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(update, (flag, night_id))
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def get_ids_from_nights(self) -> Union[list, psycopg2.extras.DictCursor]:
        result = []
        if not self.__connected:
            self.connect()
        select = """
                SELECT "Id"
                FROM mobaspace_data."Nuits"
                WHERE "NuitTraitee"=False
                """
        logging.debug(f"BASE DONNEES --> récupération données ID des nuits")
        cur = None
        try:
            cur = self.__cnx.cursor()#cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, )
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def get_days(self) -> Union[list, psycopg2.extras.DictCursor]:
        result = []
        if not self.__connected:
            self.connect()
        select = """
                SELECT "Jours"."Id", "NumCh", "DateJour", "TempsAllongeTotal"
                FROM mobaspace_data."Jours"
                INNER JOIN mobaspace_data."Patients" on (mobaspace_data."Patients"."Id" = mobaspace_data."Jours"."PatientId")
                WHERE mobaspace_data."Jours"."JourTraite" = False
                """
        logging.debug(f"BASE DONNEES --> récupération données Jours des patients")
        cur = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, )
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def erase_day(self, id:int) -> None:
        if not self.__connected:
            self.connect()
        delete = (
            'DELETE from mobaspace_data."Jours"'
            ' WHERE "Id"=%s'
        )
        logging.debug(f"BASE DONNEES --> effacage du jour {id}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(delete, (id,) )
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def erase_old_days(self, before_days:int):
        date_lim = datetime.now().date() - timedelta(days=before_days)
        if not self.__connected:
            self.connect()
        delete = (
            'DELETE from mobaspace_data."Jours"'
            ' WHERE "DateJour"<%s'
        )
        logging.debug(f"BASE DONNEES --> effacage des jours antérieurs à {date_lim}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(delete, (date_lim,) )
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def update_day_flagTraitee(self, day_id:int, flag=True):
        if not self.__connected:
            self.connect()
        update = (
            'UPDATE mobaspace_data."Jours" '
            ' SET "JourTraite"=%s '
            ' WHERE "Id"=%s'
        )
        logging.debug(
            f"BASE DONNEES --> mise à jour flag JourTraite={flag} pour le jour avec Id={day_id}"
        )
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(update, (flag, day_id))
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def erase_old_alarms(self, before_days:int):
        date_lim = datetime.now().date() - timedelta(days=before_days)
        if not self.__connected:
            self.connect()
        delete = (
            'DELETE from mobaspace_data."Alarmes"'
            ' WHERE "Creation"<%s'
        )
        logging.debug(f"BASE DONNEES --> effacage des Alarmes antérieurs à {date_lim}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(delete, (date_lim,) )
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def erase_old_observables(self, before_days:int):
        date_lim = datetime.now().date() - timedelta(days=before_days)
        if not self.__connected:
            self.connect()
        delete = (
            'DELETE from mobaspace_data."Observables"'
            ' WHERE "Date"<%s'
        )
        logging.debug(f"BASE DONNEES --> effacage des Observables antérieurs à {date_lim}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(delete, (date_lim,) )
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def get_tracker_lastUpdate(self, capteurId:int) -> Union[list, psycopg2.extras.DictCursor]:
        if not self.__connected:
            self.connect()
        select = """
                SELECT "Id", "LastUpdate"
                FROM mobaspace_data."Trackers"
                WHERE "CapteurId" = %s;
        """
        logging.debug(f"BASE DONNEES --> Récupération date dernière mise à jour du Tracker correspondant au capteur={capteurId}")
        cur = None
        result = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (capteurId,))
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def get_trackers(self) -> Union[list, psycopg2.extras.DictCursor]:
        if not self.__connected:
            self.connect()
        select = """
                SELECT "Trackers"."Id", "LecturesWifi", "NbPas", 
                        "AccVector", "Identifiant" as "AdresseMAC", 
                        "LastUpdate", "Patients"."Id" as "PatientId", 
                        "Chambre", "NumCh", "Power", "Trackers"."CapteurId" as "CapteurId"
                FROM mobaspace_data."Trackers"
                INNER JOIN mobaspace_data."Capteurs" ON (mobaspace_data."Trackers"."CapteurId" = mobaspace_data."Capteurs"."Id")
                INNER JOIN mobaspace_data."ApisCapteurs" ON (mobaspace_data."Capteurs"."Id" = mobaspace_data."ApisCapteurs"."CapteurId")
                INNER JOIN mobaspace_data."OAuth2Apis" ON (mobaspace_data."ApisCapteurs"."ApiId" = mobaspace_data."OAuth2Apis"."Id")
                LEFT JOIN mobaspace_data."Patients" ON (mobaspace_data."OAuth2Apis"."PatientId" = mobaspace_data."Patients"."Id")
                WHERE "Traite" = False;
                """
        logging.debug(f"BASE DONNEES --> Récupération données Trackers non traités et attachés à un résident")
        cur = None
        result = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, )
            result = cur.fetchall()
            self.__cnx.commit()
        except psycopg2.DatabaseError as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
            return result

    def set_tracker_flag(self, devId:int, flag:bool):
        if not self.__connected:
            self.connect()
        update = (
            'UPDATE mobaspace_data."Trackers" '
            ' SET "Traite"=%s '
            ' WHERE "Id"=%s'
        )
        logging.debug(
            f"BASE DONNEES --> mise à jour flag tracker pour le dispositif avec Id={devId}"
        )
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(update, (flag, devId))
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    def update_tracker(self, macAdd:str, lec_wifi:dict, nb_pas:int, acc_vector:[], power:float):
        if not self.__connected:
            self.connect()
        update = """
            UPDATE mobaspace_data."Trackers"
            SET "LecturesWifi"=%s,
            "NbPas"=%s,
            "AccVector"=%s,
            "LastUpdate"=%s,
            "Traite"=False,
            "Power"="Power" - %s
            WHERE(
                SELECT t1."Id"
                from mobaspace_data."Trackers" as t1
                inner join mobaspace_data."Capteurs" as t2
                on (t1."CapteurId" = t2."Id")
                where "Identifiant" = %s
            ) = "Id";
        """

        logging.debug(
            f"BASE DONNEES --> mise à jour des données tracker pour le dispositif avec Id={macAdd}"
        )
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(update, (
                json.dumps(lec_wifi),
                nb_pas,
                acc_vector,
                datetime.now(),
                power,
                macAdd
            ))
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return

    @property
    def is_connected(self):
        """ Returns the database state

            :return: true is the client is connected to the database, false else
            :rtype: bool
        """
        return self.__connected

    def monitoring(self):
        """ Starts the database monitoring method
        """
        self.__thread.start()

    def __heart_beat(self):
        """ Check database connection and try to reconnect every 10s
        """
        while True:
            if not self.__heart_beat_check():
                # try to reconnect
                self.connect()
            time.sleep(10)

    def __heart_beat_check(self):
        """ Heart beat method, check if database is alive and update its state
        """
        result = False
        if not self.__connected:
            return result
        select = "SELECT 1"
        logging.debug("BASE DONNEES --> Heart beat check")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(select)
            self.__cnx.commit()
            result = True
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return result

    def checklist_from_db(self, chambre, periode):
        check_list = []
        if not self.__connected:
            return check_list
        select = (
            'SELECT "CheckLists"."Check_Item"'
            ' FROM "CheckLists"'
            ' JOIN "Patients" ON "Patients"."Id" = "CheckLists"."PatientId"'
            ' WHERE "Patients"."Chambre" = %s'
            ' AND %s = ANY ("CheckLists".programme)'
            ' ORDER BY "CheckLists"."Rang"'
        )
        logging.debug("BASE DONNEES --> Checklist from db")
        cur = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(select, (str(chambre), str(periode)))
            check_list = cur.fetchall()
            self.__cnx.commit()
        except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
            try:
                self.__cnx.rollback()
                logging.error(error.pgerror)
            except AttributeError:
                logging.error(error)
        finally:
            if isinstance(cur, psycopg2.extensions.cursor):
                cur.close()
        return check_list
