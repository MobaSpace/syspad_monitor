# coding: utf8
"""
Classe pour gérer la base de données de SysPAD
:author: all
"""

import json
import logging
import threading
import time
import datetime as dt
from datetime import datetime, timedelta, date
import arrow
from typing import Union

import psycopg2
import psycopg2.extensions
import psycopg2.extras

from syspad_monitor.task import PatientConst, AlarmConst


class SysPadDb_encry:
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


    def set_alarm_counter(self, alarm_id: int, counter_val: int):
        if not self.__connected:
            self.connect()
        update = ' UPDATE mobaspace_data."alarmesview" SET "NbNotifications"=%s WHERE "Id"=%s;'
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

    def read_alarms(self) -> Union[list, psycopg2.extras.DictCursor]:
        if not self.__connected:
            self.connect()
        select = """
            SELECT "alarmesview"."Id" AS "Id", "NbNotifications", "Chambre", "NumCh", "Description",
             "alarmesview"."Creation", "alarmesview"."Priorite", "Appel", "CanalNotif"
            FROM mobaspace_data."alarmesview" 
            INNER JOIN mobaspace_data."patientsview" ON
             (mobaspace_data."alarmesview"."PatientId" = mobaspace_data."patientsview"."Id")
            LEFT JOIN mobaspace_data."ContactsPatients" ON
             (mobaspace_data."patientsview"."Id" = mobaspace_data."ContactsPatients"."PatientId")
            LEFT JOIN mobaspace_data."AspNetUsers"
             ON (mobaspace_data."ContactsPatients"."UtilisateurId" = mobaspace_data."AspNetUsers"."Id")
            WHERE "NbNotifications" > 0;
        """
        logging.debug(f"BASE DONNEES --> récupération des alarmes restant à traiter")
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
            return result[0]['URI']


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

    def get_journeyObservables4room(self, room:int, day:datetime.date, my_type:str) -> Union[list, psycopg2.extras.DictCursor]:
        """
        Fonction pour recupérer les observables d'une chambre pour un jour donnée
        :rtype: object
        :return:
        """

        date_deb = datetime.combine(day, dt.time(0, 0))
        date_fin = datetime.combine(day, dt.time(23, 59, 59))

        select = """SELECT "Valeurs" 
            FROM mobaspace_data."observablesview"
            INNER JOIN mobaspace_data."TypeObservable"
            ON (mobaspace_data."observablesview"."TypeObservableId" = mobaspace_data."TypeObservable"."Id")
            WHERE "Chambre" = %s AND "Date" > %s AND "Date" < %s AND "TypeObservable"."Type" = %s"""

        logging.debug(f"BASE DONNEES --> récupération des observables restant à envoyer vers NetSOINS")
        cur = None
        result = dict()
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (room, date_deb, date_fin, my_type))
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

    def get_all_observables(self) -> Union[list, psycopg2.extras.DictCursor]:
        """
        Fonction pour recupérer tous les observables non traités
        :return:
        """
        select = """SELECT "observablesview"."Id", "TypeObservable"."Type", 
            "TypeObservable"."URI", "Valeurs", "Date", "Chambre", "UriPersonnel", "Commentaire"
            FROM mobaspace_data."observablesview"
            INNER JOIN mobaspace_data."TypeObservable"
            ON (mobaspace_data."observablesview"."TypeObservableId" = mobaspace_data."TypeObservable"."Id")
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
            'DELETE from mobaspace_data."observablesview"'
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
            'UPDATE mobaspace_data."observablesview"'
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

    def set_observable(self, room: int, uriPer:str, type_obs: str, values: [], comment=None):
        """
        Cette fonction permet d'insérer un nouveau observable dans la BD SySPAD
        :param type_obs: le type d'observation par ex. 'temperature'
        :param values: les valeurs, c'est une liste qui peut contenir différents types int, float, str
        :param room: la chambre
        :return:
        """
        pat_id = self.get_patId_from_room(room=room)
        obs_id = self.get_obsTypeId(type=type_obs)
        if not obs_id:
            logging.error(f"BASE DONNEES --> l'observable {type_obs} ne semble pas exister")
            return
        my_json = json.dumps(values)
        my_date = datetime.now()

        if not self.__connected:
            self.connect()
        insert = (
            'INSERT INTO mobaspace_data."observablesview" '
            ' ("TypeObservableId", "Valeurs", "Date", "PatientId", "Chambre", "ObservableTraite", "Commentaire", "UriPersonnel") '
            ' VALUES(%s, %s, %s, %s, %s, False, %s, %s);'
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
                    room,
                    comment,
                    uriPer,
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
            ' VALUES(%s, %s, %s, %s)'
            ' ON CONFLICT ("URI") DO UPDATE'
            ' SET "Date" = EXCLUDED."Date",'
            ' "CodeRetour" = EXCLUDED."CodeRetour",'
            ' "DetailRetour" = EXCLUDED."DetailRetour"'
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
        select = """SELECT "Id" FROM mobaspace_data."patientsview" WHERE "NumCh" = %s"""
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
            'UPDATE mobaspace_data."patientsview"'
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
            'UPDATE mobaspace_data."patientsview"'
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

    def get_id_sensor_for_pat(self, patient_id: int, provider='Withings'):
        select = """
        SELECT "CapteurId" FROM mobaspace_data."ApisCapteurs"
        INNER JOIN mobaspace_data."OAuth2Apis" on ("OAuth2Apis"."Id" = "ApiId")
        WHERE "PatientId"=%s AND "Provider"=%s
        """
        cur = None
        result = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(select, (patient_id, provider,))
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


    def update_bed_notify(self, patient_id: int, bed_type: int, noti_date: int):
        if bed_type == PatientConst.BED_IN.value:
            update = (
                'UPDATE mobaspace_data."patientsview"'
                ' SET "NouvellesDonneesLit"=%s , "DernierCoucher"=to_timestamp(%s)'
                ' WHERE "Id"=%s'
            )
        else:
            update = (
                'UPDATE mobaspace_data."patientsview" '
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
        select = """SELECT "NouvellesDonneesLit"
            FROM mobaspace_data."patientsview" 
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
                'UPDATE mobaspace_data."patientsview" SET "NouvellesDonneesLit"=%s WHERE "Id"=%s'
            )
        elif mytype == PatientConst.sleep_data.value:
            update = (
                'UPDATE mobaspace_data."patientsview" '
                ' SET "NouvellesDonneesHisto"=%s '
                ' WHERE "Id"=%s'
            )
        elif mytype == PatientConst.tracker_data.value:
            update = (
                'UPDATE mobaspace_data."patientsview" '
                ' SET "NouvellesDonneesTracker"=%s '
                ' WHERE "Id"=%s'
            )
        else:
            return

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
            FROM mobaspace_data."patientsview" WHERE "Id" = %s"""
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
        select = """ SELECT "Coucher_h", "Coucher_min", "Lever_h", "Lever_min", DureeMaxHorsLit_min"
            FROM mobaspace_data."patientsview" WHERE "Id"=%s """
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
        select = """ SELECT * FROM mobaspace_data."patientsview" WHERE "Id"=%s """
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
        select = """ SELECT * FROM mobaspace_data."patientsview" """
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
            "ClientSecret", "EtatOK"
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

    def set_tracker_state(self, tracker_mac:str, state=True):
        if not self.__connected:
            self.connect()
        update = (
            'UPDATE mobaspace_data."Capteurs" SET "EtatOK"= %s WHERE "Identifiant"=%s'
        )
        logging.debug(f"BASE DONNEES --> mise à jour état Capteur {tracker_mac}, nouvelle valeur {state}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(update, (state, tracker_mac))
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

    def set_api_state(self, api_id:int, state:bool):
        if not self.__connected:
            self.connect()
        update = (
            'UPDATE mobaspace_data."OAuth2Apis" SET "EtatOK"=%s WHERE "Id"=%s'
        )
        logging.debug(f"BASE DONNEES --> mise à jour état API {api_id}, nouvelle valeur {state}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(update, (state, api_id))
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
            'INSERT INTO mobaspace_data."alarmesview" '
            ' ("CapteurId", "PatientId", "Priorite", "Description", '
            ' "Creation", "NbNotifications") '
            " VALUES(%s, %s, %s, %s, %s, %s);"
        )
        counts = 1
        if alarm["priorite"] >= AlarmConst.HIGH.value: #AlarmConst.HIGH.value = 85 so gt 75 is enought to set more counts!!
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

    def update_prediction(self, id:int, s4today:float, s4tomorrow:float, ti:float, fr:float):
        if not self.__connected:
            self.connect()
        update = (
            'UPDATE mobaspace_data."ScoreForme"'
            ' SET "TauxDeRemp"=%s, "ScoreJour"=%s, "ScorePred"=%s, "IndiceDeConfiance"=%s'
            ' WHERE "Id"=%s'
        )
        logging.debug(f"BASE DONNEES --> update des scores pour l'Id={id}")
        cur = None
        # le nombre de notifications par défaut est mis à 3
        try:
            cur = self.__cnx.cursor()
            cur.execute(
                update,
                (
                    fr, #fillingRate
                    s4today,
                    s4tomorrow,
                    ti, #trustIndex
                    id, #primary key
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

    def get_roomValues_4_prediction(self, room:int) -> Union[list, psycopg2.extras.DictCursor]:
        if not self.__connected:
            self.connect()
        select = (
            'SELECT "Id", "Date", "Current_values" from mobaspace_data."ScoreForme"'
            ' WHERE "NumCh" = %s AND "Date" > CURRENT_DATE - 7'
            ' ORDER BY "Date" ASC'
        )
        logging.debug(f"BASE DONNEES --> Recupération des chambres qui sont suivies pour la prédiction")
        cur = None
        result = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (room,))
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

    def get_roomsId_4_prediction(self) -> []:
        if not self.__connected:
            self.connect()
        select = (
            'SELECT DISTINCT "NumCh" from mobaspace_data."ScoreForme"'
        )
        logging.debug(f"BASE DONNEES --> Recupération des chambres qui sont suivies pour la prédiction")
        cur = None
        result = None
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

    def set_filledForm(self, room: int, values: [], for_date=None):
        if not self.__connected:
            self.connect()
        date_jour=arrow.now().format('YYYY-MM-DD')
        if for_date:
            date_jour = for_date
        insert = (
            'WITH upsert AS (UPDATE mobaspace_data."ScoreForme"'
            ' SET "Current_values" = %s'
            ' WHERE "NumCh" = %s AND "Date" = %s RETURNING *)'
            ' INSERT INTO mobaspace_data."ScoreForme"'
            ' ("NumCh", "Date", "Current_values")'
            ' SELECT %s, %s, %s'
            ' WHERE NOT EXISTS (SELECT 1 FROM upsert)'
        )

        logging.debug(f"BASE DONNEES --> insertion QUESTIONNAIRE du Jour chambre {room}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(
                insert,
                (
                    json.dumps(values),
                    room,
                    date_jour,
                    room,
                    date_jour,
                    json.dumps(values),
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
        return

    def insert_day_total_lying_time(self, patient: int, date_jour: str, temps_allonge_total: int):
        if not self.__connected:
            self.connect()
        insert = (
            'WITH upsert AS (UPDATE mobaspace_data."joursview"'
            ' SET "TempsAllongeTotal" = %s'
            ' WHERE "PatientId" = %s AND "DateJour" = %s RETURNING *)'
            ' INSERT INTO mobaspace_data."joursview"'
            ' ("PatientId", "DateJour", "TempsAllongeTotal")'
            ' SELECT %s, %s, %s'
            ' WHERE NOT EXISTS (SELECT 1 FROM upsert)'
        )
        logging.debug(f"BASE DONNEES --> insertion des données de JOUR patient {patient} pour {date_jour}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(
                insert,
                (
                    temps_allonge_total,
                    patient,
                    date_jour,
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

    def setURItoUser(self, userId: str, uri: str):
        if not self.__connected:
            self.connect()
        update = (
            'UPDATE mobaspace_data."AspNetUsers"'
            ' SET "UriNetSOINS"=%s'
            ' WHERE "Id"=%s'
        )
        cur = None
        # le nombre de notifications par défaut est mis à 3
        try:
            cur = self.__cnx.cursor()
            cur.execute(
                update,
                (
                    uri, #uri
                    userId, #userId
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

    def getRoomsVisited4User(self, idPer:str) -> Union[list, psycopg2.extras.DictCursor]:

        if not self.__connected:
            self.connect()
        curDay = datetime.today().date()
        select = (
            'SELECT "NumCh", "Date", "Data" from mobaspace_data."infoblocchambreview"'
            ' WHERE "UriNetSOINS" = %s'
            ' AND "Date" >= %s;'
        )
        cur = None
        result = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (idPer, curDay))
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

    #obtenition des blocs d'information non traités
    def getRoomsInfoBlocks(self) -> Union[list, psycopg2.extras.DictCursor]:
        if not self.__connected:
            self.connect()
        select = (
            'SELECT "Id", "NumCh", "Data", "UriNetSOINS" from mobaspace_data."infoblocchambreview"'
            ' WHERE "Traite" = false'
        )
        cur = None
        result = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select,)
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

    def setRoomBlockFlag (self, id:int, flag:bool):
        if not self.__connected:
            self.connect()
        update = (
            'UPDATE mobaspace_data."infoblocchambreview"'
            ' SET "Traite"=%s'
            ' WHERE "Id"=%s'
        )
        cur = None
        # le nombre de notifications par défaut est mis à 3
        try:
            cur = self.__cnx.cursor()
            cur.execute(
                update,
                (
                    flag, #normally set to True
                    id, #id of the entry on DB
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

    #insertion d'une nouvelle ligne block chambre
    def setBedroomInfoBlock(self, room:int, values: dict, idPer:str):
        if not self.__connected:
            self.connect()
        # this lines are here just to test
        # will be removed
        my_date = datetime.utcnow()
        insert = (
            'INSERT INTO mobaspace_data."infoblocchambreview" '
            ' ("UriNetSOINS", "NumCh", "Data", "Date", "Traite") '
            ' VALUES(%s, %s, %s, %s, false);'
        )
        cur = None
        # le nombre de notifications par défaut est mis à 3
        try:
            cur = self.__cnx.cursor()
            cur.execute(
                insert,
                (
                    idPer,
                    room,
                    json.dumps(values),
                    my_date,
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
        return

    def getUsersWithoutURI(self) -> Union[list, psycopg2.extras.DictCursor]:
        if not self.__connected:
            self.connect()
        select = (
            'SELECT "Id", "UserFirstname", "UserSurname" from mobaspace_data."AspNetUsers"'
            ' WHERE "UriNetSOINS" IS NULL'
            ' AND "Linked2NetSOINS" = TRUE'
        )
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


    def get_day_stepfreqs(self, patient:int, date_jour: str) -> []:
        if not self.__connected:
            self.connect()
        my_list = []

        select = (
            'SELECT "VitesseMarcheMoyenne" from mobaspace_data."joursview"'
            ' WHERE "PatientId" = %s AND "DateJour" = %s'
        )
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(select, (patient, date_jour))
            result = cur.fetchone()
            if result:
                my_list = result[0]
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
        return my_list

    def insert_day_steps(self, patient: int, date_jour: str, steps: int, activityTime: int, stepfreq: float):
        if not self.__connected:
            self.connect()
        # recuperation de la liste si existante
        stepfreq_list = self.get_day_stepfreqs(patient, date_jour)
        try:
            if stepfreq > 0:
                stepfreq_list.append(stepfreq)
        except AttributeError:
            stepfreq_list = []
            if stepfreq > 0:
                stepfreq_list.append(stepfreq)

        insert = (
            'WITH upsert AS (UPDATE mobaspace_data."joursview"'
            ' SET "NbPas" = "NbPas" + %s, "TempsTotalActivite" = "TempsTotalActivite" + %s, "VitesseMarcheMoyenne" = %s'
            ' WHERE "PatientId" = %s AND "DateJour" = %s RETURNING *)'
            ' INSERT INTO mobaspace_data."joursview"'
            ' ("PatientId", "DateJour", "NbPas", "TempsTotalActivite", "VitesseMarcheMoyenne")'
            ' SELECT %s, %s, %s, %s, %s'
            ' WHERE NOT EXISTS (SELECT 1 FROM upsert)'
        )

        logging.debug(f"BASE DONNEES --> insertion des données de JOUR patient {patient} pour {date_jour}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(
                insert,
                (
                    steps,
                    activityTime,
                    stepfreq_list,
                    patient,
                    date_jour,
                    patient,
                    date_jour,
                    steps,
                    activityTime,
                    stepfreq_list
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
            'WITH upsert AS (UPDATE mobaspace_data."nuitsview"'
            ' SET "DateDebut" = %s, "DateFin" = %s, "ScoreNuit" = %s, "NbReveils" = %s,'
            ' "DureeReveilAuLit" = make_interval(0,0,0,0,0,0,%s), "DureeSommeil" = make_interval(0,0,0,0,0,0,%s), "NuitTraitee" = False'
            ' WHERE "PatientId" = %s AND "DateNuit" = %s RETURNING *)'
            ' INSERT INTO mobaspace_data."nuitsview"'
            ' ("PatientId", "DateNuit", "DateDebut", "DateFin", '
            ' "ScoreNuit", "NbReveils", "DureeReveilAuLit", "DureeSommeil", "NuitTraitee")'
            ' SELECT %s, %s, %s, %s, %s, %s,'
            ' make_interval(0,0,0,0,0,0,%s), make_interval(0,0,0,0,0,0,%s), False'
            ' WHERE NOT EXISTS (SELECT 1 FROM upsert)'
        )

        logging.debug(f"BASE DONNEES --> insertion des données de NUIT patient {patient} pour {date_nuit}")
        cur = None
        try:
            cur = self.__cnx.cursor()
            cur.execute(
                insert,
                (
                    datetime.utcfromtimestamp(data["DateDebut"]),
                    datetime.utcfromtimestamp(data["DateFin"]),
                    data["ScoreNuit"],
                    data["NbReveils"],
                    data["DureeReveilAuLit"],
                    data["DureeSommeil"],
                    patient,
                    date_nuit,
                    patient,
                    date_nuit,
                    datetime.utcfromtimestamp(data["DateDebut"]),
                    datetime.utcfromtimestamp(data["DateFin"]),
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
            'WITH upsert AS (UPDATE mobaspace_data."nuitsview"'
            ' SET "NbSorties" = %s, "DureeReveilHorsLit" = make_interval(0,0,0,0,0,0,%s), "DetailSorties" = %s, "NuitTraitee" = False'
            ' WHERE "PatientId" = %s AND "DateNuit" = %s RETURNING *)'
            'INSERT INTO mobaspace_data."nuitsview"'
            ' ("PatientId", "DateNuit",'
            ' "NbSorties", "DureeReveilHorsLit", "DetailSorties", "NuitTraitee")'
            ' SELECT %s, %s, %s, make_interval(0,0,0,0,0,0,%s), %s, False'
            ' WHERE NOT EXISTS (SELECT 1 FROM upsert)'
        )
        logging.debug(f"BASE DONNEES --> insertion des sorties-lit du patient {patient} pour {date_nuit}")
        cur = None
        try:
            logging.debug(f'BASE DONNEES insertion des sorties-lit {bedouts_summ["counts"]}')
            cur = self.__cnx.cursor()
            cur.execute(
                insert,
                (
                    int(bedouts_summ["counts"]),
                    int(bedouts_summ["total_secs"]),
                    json.dumps(bedouts_summ["details"]),
                    patient,
                    date_nuit,
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

    def get_sleep_score(self, patId:int, mydate:date) -> int:
        result = None
        myts = mydate
        if not self.__connected:
            self.connect()
        select = """
                SELECT "ScoreNuit"
                FROM mobaspace_data."nuitsview"
                WHERE mobaspace_data."nuitsview"."PatientId" = %s AND mobaspace_data."nuitsview"."DateNuit"= %s
                """
        logging.debug(f"BASE DONNEES --> récupération données sommeil nuit")
        cur = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (patId, myts))
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

    def get_step_score(self, patId:int, mydate:date) -> int:
        result = None
        if not self.__connected:
            self.connect()
        select = """
                SELECT "NbPas"
                FROM mobaspace_data."joursview"
                WHERE mobaspace_data."joursview"."PatientId" = %s AND mobaspace_data."joursview"."DateJour"= %s
                """
        logging.debug(f"BASE DONNEES --> récupération données sommeil nuit")
        cur = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (patId, mydate))
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

    def get_lying_time(self, patId:int, mydate:date) -> int:
        result = None
        if not self.__connected:
            self.connect()
        select = """
                SELECT "TempsAllongeTotal",
                FROM mobaspace_data."joursview"
                WHERE mobaspace_data."joursview"."PatientId" = %s AND mobaspace_data."joursview"."DateJour"= %
                """
        logging.debug(f"BASE DONNEES --> récupération données sommeil nuit")
        cur = None
        try:
            cur = self.__cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(select, (patId, mydate))
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

    def get_SLEEP_from_nights(self, id:int) -> Union[list, psycopg2.extras.DictCursor]:
        result = []
        if not self.__connected:
            self.connect()
        select = """
                SELECT "nuitsview"."Id", "DateDebut", "DateFin", "ScoreNuit",
                "DureeReveilAuLit", "DureeReveilHorsLit", "nuitsview"."DureeSommeil",
                "NbSorties", "DetailSorties", "NumCh" 
                FROM mobaspace_data."nuitsview"
                INNER JOIN mobaspace_data."patientsview" on (mobaspace_data."nuitsview"."PatientId" = mobaspace_data."patientsview"."Id")
                WHERE "nuitsview"."Id" = %s
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
            'DELETE from mobaspace_data."nuitsview"'
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

    def erase_old_scores(self, before_days:int):
        date_lim = datetime.now().date() - timedelta(days=before_days)
        if not self.__connected:
            self.connect()
        delete = (
            'DELETE from mobaspace_data."ScoreForme"'
            ' WHERE "Date"<%s'
        )
        logging.debug(f"BASE DONNEES --> effacage des ScoreForme antérieurs à {date_lim}")
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
            'DELETE from mobaspace_data."nuitsview"'
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
            'UPDATE mobaspace_data."nuitsview" '
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
                FROM mobaspace_data."nuitsview"
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
                SELECT "joursview"."Id", "NumCh", "DateJour", "TempsAllongeTotal", "NbPas"
                FROM mobaspace_data."joursview"
                INNER JOIN mobaspace_data."patientsview" on (mobaspace_data."patientsview"."Id" = mobaspace_data."joursview"."PatientId")
                WHERE mobaspace_data."joursview"."JourTraite" = False
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
            'DELETE from mobaspace_data."joursview"'
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
            'DELETE from mobaspace_data."joursview"'
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
            'UPDATE mobaspace_data."joursview" '
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
            'DELETE from mobaspace_data."alarmesview"'
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
            'DELETE from mobaspace_data."observablesview"'
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
                FROM mobaspace_data."trackersview"
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
                SELECT "trackersview"."Id", "LecturesWifi", "NbPas", "ActivityTime", "VitesseMarche",
                        "AccVector", "Identifiant" as "AdresseMAC", 
                        "LastUpdate", "patientsview"."Id" as "PatientId", 
                        "Chambre", "NumCh", "Power", "trackersview"."CapteurId" as "CapteurId"
                FROM mobaspace_data."trackersview"
                INNER JOIN mobaspace_data."Capteurs" ON (mobaspace_data."trackersview"."CapteurId" = mobaspace_data."Capteurs"."Id")
                INNER JOIN mobaspace_data."ApisCapteurs" ON (mobaspace_data."Capteurs"."Id" = mobaspace_data."ApisCapteurs"."CapteurId")
                INNER JOIN mobaspace_data."OAuth2Apis" ON (mobaspace_data."ApisCapteurs"."ApiId" = mobaspace_data."OAuth2Apis"."Id")
                LEFT JOIN mobaspace_data."patientsview" ON (mobaspace_data."OAuth2Apis"."PatientId" = mobaspace_data."patientsview"."Id")
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
            'UPDATE mobaspace_data."trackersview" '
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

    def update_tracker(self, macAdd:str, lec_wifi:dict, nb_pas:int, acc_vector:[], power:float, walk_speed:float, act_time:int):
        if not self.__connected:
            self.connect()
        update = """
            UPDATE mobaspace_data."trackersview"
            SET "LecturesWifi"=%s,
            "NbPas"=%s,
            "AccVector"=%s,
            "LastUpdate"=%s,
            "Traite"=False,
            "Power"=%s,
            "ActivityTime"=%s,
            "VitesseMarche"=%s
            WHERE(
                SELECT t1."Id"
                from mobaspace_data."trackersview" as t1
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
                act_time,
                walk_speed,
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
