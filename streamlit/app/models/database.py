import psycopg2
import os

from psycopg2 import InterfaceError, OperationalError
from psycopg2.errors import UndefinedTable, InFailedSqlTransaction
import abc

class PostgresqlDatabaseInterface(abc.ABC):
    
    def __init__(self, user, password, host, port, database):
        if self.__connect_to_database(user, password, host, port, database):
            self.cursor = self.connection.cursor()
        else:
            self.cursor = None

    def __connect_to_database(self, user, password, host, port, database):

        try:
            self.connection = psycopg2.connect(
                user=user,
                password=password,
                host=host,
                port=port,
                database=database
            )
            self.connection.set_session(autocommit=True)
            return True
        except OperationalError as e:
            pass
            return False

    def __get_cursor(self):
        try:
            return self.connection.cursor()
        except InterfaceError as e:
            return None

    def query(self, query):
        if self.cursor is None:
            return None
        
        if self.cursor.closed:
            self.cursor = self.__get_cursor()

        if self.cursor is None:
            return None
        
        try: 
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except (UndefinedTable, InFailedSqlTransaction) as e:
            return None
        except InterfaceError as e:
            # reconnect to database
            self.cursor.close()
            return None

    def __del__(self):
        self.connection.close()


class DagsterDatabaseInterface (PostgresqlDatabaseInterface):

    # define static method for singleton pattern
    @staticmethod
    def get_instance():
        user = os.getenv("DB_DAGSTER_USER")
        password = os.getenv("DB_DAGSTER_PASSWORD")
        host = os.getenv("DB_DAGSTER_HOST")
        port = os.getenv("DB_DAGSTER_PORT")
        database = os.getenv("DB_DAGSTER_DATABASE")

        if not hasattr(DagsterDatabaseInterface, "__instance"):
            DagsterDatabaseInterface.__instance = DagsterDatabaseInterface( 
                user, password, host, port, database 
            )
        return DagsterDatabaseInterface.__instance
    
    def __init__(self, user, password, host, port, database):
        super().__init__(user, password, host, port, database)

    def get_last_run_for_each_pipeline(self):
        query = """
            SELECT
                "run_id",
                REGEXP_REPLACE( nome_pipeline::TEXT, '([.]definitions)', '' ) as pipeline,
                "status",
                start_timestamp,
                end_timestamp
            FROM (
                SELECT
                    *,
                    row_number() OVER (PARTITION BY nome_pipeline ORDER BY start_timestamp DESC) AS row_num
                FROM (
                    SELECT 
                        "run_id", 

                        "run_body"::jsonb
                        ->'external_pipeline_origin'
                        ->'external_repository_origin'
                        ->'repository_location_origin'
                        ->'location_name'::TEXT AS nome_pipeline, 

                        "status", 
                        -- "create_timestamp",
                        TO_TIMESTAMP("start_time") as start_timestamp,
                        TO_TIMESTAMP("end_time") as end_timestamp

                    FROM "runs"
                    WHERE start_time IS NOT NULL AND end_time IS NOT NULL
                ) AS runs
            ) AS runs_with_row_num
            WHERE row_num = 1
        """

        records = super().query(query)
        return records

class DWDatabaseInterface (PostgresqlDatabaseInterface):

    # define static method for singleton pattern
    @staticmethod
    def get_instance():
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        database = os.getenv("DB_NAME")

        if not hasattr(DWDatabaseInterface, "__instance"):
            DWDatabaseInterface.__instance = DWDatabaseInterface( 
                user, password, host, port, database 
            )
        return DWDatabaseInterface.__instance
    
    def __init__(self, user, password, host, port, database):
        super().__init__(user, password, host, port, database)

    def get_list_of_files_already_processed(self):
        query = """
            SELECT DISTINCT LOWER(lab_id) || '/' || file_name AS file_path
            FROM "arboviroses"."combined_01_join_labs"
        """

        records = super().query(query)
        return records
    
    def get_latest_date_of_lab_data(self):
        query = """
            SELECT lab_id, MAX(date_testing) AS last_date
            FROM "arboviroses"."combined_01_join_labs"
            GROUP BY lab_id
        """

        records = super().query(query)
        return records
    
    def get_list_of_all_labs(self):
        query = """
            SELECT DISTINCT lab_id
            FROM "arboviroses"."combined_01_join_labs"
        """

        records = super().query(query)
        return records
    
    def get_number_of_tests_per_lab_and_epiweek_in_this_year(self):
        query = """
            SELECT
                lab_id, 
                epiweek_number, 
                COUNT(*)
            FROM
                arboviroses.combined_05_location
            WHERE EXTRACT(YEAR FROM date_testing) = EXTRACT(YEAR FROM CURRENT_DATE)
            GROUP BY lab_id, epiweek_number
        """

        records = super().query(query)
        return records
    
    def get_epiweek_number_of_latest_epiweeks(self):
        query = """
            SELECT 
                unnest(
                    ARRAY[
                        week_num-7, week_num-6, week_num-5, week_num-4, 
                        week_num-3, week_num-2, week_num-1, week_num
                    ] 
                )
                AS epiweek_number_5
            FROM arboviroses.epiweeks
            WHERE 
            CURRENT_DATE<=end_date 
            AND CURRENT_DATE>=start_date
        """

        records = super().query(query)
        return records
    
    def get_number_of_tests_per_lab_in_latest_epiweeks(self):
        epiweeks = self.get_epiweek_number_of_latest_epiweeks()
        lab_counts_by_epiweek = self.get_number_of_tests_per_lab_and_epiweek_in_this_year()
        
        epiweeks = [ epiweek[0] for epiweek in epiweeks ]
        if lab_counts_by_epiweek is None:
            return None
        
        labs = self.get_list_of_all_labs()
        join_lab_and_epiweek = lambda lab, epiweek: f"{lab}-{epiweek:02d}"

        lab_counts_by_epiweek = { 
            join_lab_and_epiweek(lab, epiweek): count
            for lab, epiweek, count 
            in lab_counts_by_epiweek
            if epiweek in epiweeks
        }

        
        labs = [ lab[0] for lab in labs ]

        for lab in labs:
            for epiweek in epiweeks:
                if join_lab_and_epiweek(lab, epiweek) not in lab_counts_by_epiweek:
                    lab_counts_by_epiweek[join_lab_and_epiweek(lab, epiweek)] = 0

        return lab_counts_by_epiweek
