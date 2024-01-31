import psycopg2
import os

class DagsterDatabaseInterface:

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
        self.connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        
        self.cursor = self.connection.cursor()

    def __query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
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

        records = self.__query(query)
        return records

    def __del__(self):
        self.connection.close()


class DWDatabaseInterface:

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
        self.connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        
        self.cursor = self.connection.cursor()

    def __query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def get_list_of_files_already_processed(self):
        query = """
            SELECT DISTINCT LOWER(lab_id) || '/' || file_name AS file_path
            FROM "arboviroses"."combined_01_join_labs"
        """

        records = self.__query(query)
        return records
    
    def get_latest_date_of_lab_data(self):
        query = """
            SELECT lab_id, MAX(date_testing) AS last_date
            FROM "arboviroses"."combined_01_join_labs"
            GROUP BY lab_id
        """

        records = self.__query(query)
        return records

    def __del__(self):
        self.connection.close()