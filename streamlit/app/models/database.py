import psycopg2
import os

class DagsterDatabaseInterface:
    """
    Class to interact with the Dagster database.
    """

    @staticmethod
    def get_instance():
        """
        Singleton pattern to get the instance of the class.

        Returns:
            DagsterDatabaseInterface: The instance of the class.
        """
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
        """
        Initializes a Database object and establishes a connection to the PostgreSQL database.

        Args:
            user (str): The username for the database connection.
            password (str): The password for the database connection.
            host (str): The host address of the database server.
            port (int): The port number of the database server.
            database (str): The name of the database to connect to.
        """
        self.connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        self.connection.set_session(autocommit=True)
        
        self.cursor = self.connection.cursor()

    def __query(self, query):
        """
        Executes the given SQL query and returns the result.

        Args:
            query (str): The SQL query to execute.

        Returns:
            list: The result of the query as a list of tuples.
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def get_last_run_for_each_pipeline(self):
        """
        Retrieves the last run for each pipeline from the logs table.

        Returns:
            records (list): A list of tuples containing the details of the last run for each pipeline. Each tuple
            contains the following values:
                - "run_id" (str): The ID of the run.
                - "pipeline" (str): The name of the pipeline.
                - "status" (str): The status of the run.
                - "start_timestamp" (datetime): The start timestamp of the run.
                - "end_timestamp" (datetime): The end timestamp of the run.
        """
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
        """
        Returns the instance of the DWDatabaseInterface class.

        This method follows the singleton design pattern to ensure that only one instance of the
        DWDatabaseInterface class is created and returned.

        Returns:
            DWDatabaseInterface: The instance of the DWDatabaseInterface class.
        """
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
        """
        Initializes a Database object and establishes a connection to the PostgreSQL database.

        Args:
            user (str): The username for the database connection.
            password (str): The password for the database connection.
            host (str): The host address of the database server.
            port (int): The port number of the database server.
            database (str): The name of the database to connect to.
        """
        self.connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        self.connection.set_session(autocommit=True)
        
        self.cursor = self.connection.cursor()

    def __query(self, query):
        """
        Executes the given SQL query and returns the result.

        Args:
            query (str): The SQL query to execute.

        Returns:
            list: The result of the query as a list of tuples.
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def get_list_of_files_already_processed(self):
        """
        Retrieves the list of files that have already been processed.

        Returns:
            records (list): A list of tuples containing the details of the processed files. Each tuple
            contains the following values:
                - "file_path" (str): The path of the processed file.
        """
        query = """
            SELECT DISTINCT LOWER(lab_id) || '/' || file_name AS file_path
            FROM "arboviroses"."combined_01_join_labs"
        """

        records = self.__query(query)
        return records
    
    def get_latest_date_of_lab_data(self):
        """
        Retrieves the date of the latest record for each lab.
        Used how recent the data is for each lab.

        Returns:
            records (list): A list of tuples containing the details of the latest lab data for each lab. Each tuple
            contains the following values:
                - "lab_id" (str): The ID/name of the lab.
                - "last_date" (datetime): The latest date of lab data.
        """
        query = """
            SELECT lab_id, MAX(date_testing) AS last_date
            FROM "arboviroses"."combined_01_join_labs"
            GROUP BY lab_id
        """

        records = self.__query(query)
        return records
    
    def get_list_of_all_labs(self):
        """
        Retrieves the list of all labs.

        Returns:
            records (list): A list of tuples containing the details of the labs. Each tuple
            contains the following values:
                - "lab_id" (str): The ID/name of the lab.
        """
        query = """
            SELECT DISTINCT lab_id
            FROM "arboviroses"."combined_01_join_labs"
        """

        records = self.__query(query)
        return records
    
    def get_number_of_tests_per_lab_and_epiweek_in_this_year(self):
        """
        Retrieves the number of tests per lab and epiweek in the current year.

        Returns:
            records (list): A list of tuples containing the details of the number of tests. Each tuple
            contains the following values:
                - "lab_id" (str): The ID/name of the lab.
                - "epiweek_number" (int): The epiweek number.
                - "test_count" (int): The number of tests.
        """
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

        records = self.__query(query)
        return records
    
    def get_epiweek_number_of_latest_epiweeks(self):
        """
        Retrieves the epiweek numbers of the latest 5 epiweeks in the current year.

        Returns:
            records (list): A list of tuples containing the epiweek numbers of the latest epiweeks. Each tuple
            contains the following values:
                - "epiweek_number" (int): The epiweek number.
        """
        query = """
            SELECT 
                unnest(
                    ARRAY[week_num-4, week_num-3, week_num-2, week_num-1, week_num] 
                )
                AS epiweek_number_5
            FROM arboviroses.epiweeks
            WHERE 
            CURRENT_DATE<=end_date 
            AND CURRENT_DATE>=start_date
        """

        records = self.__query(query)
        return records
    
    def get_number_of_tests_per_lab_in_latest_epiweeks(self):
        """
        Retrieves the number of tests per lab in the latest epiweeks.

        Returns:
            records (dict): A dictionary containing the number of tests per lab in the latest epiweeks. The keys
            are strings in the format "lab-epiweek" (ex. SABIN-21) and the values are the corresponding test counts.
        """
        epiweeks = self.get_epiweek_number_of_latest_epiweeks()
        lab_counts_by_epiweek = self.get_number_of_tests_per_lab_and_epiweek_in_this_year()
        labs = self.get_list_of_all_labs()
        join_lab_and_epiweek = lambda lab, epiweek: f"{lab}-{epiweek:02d}"

        lab_counts_by_epiweek = { 
            join_lab_and_epiweek(lab, epiweek): count
            for lab, epiweek, count 
            in lab_counts_by_epiweek
        }

        epiweeks = [ epiweek[0] for epiweek in epiweeks ]
        labs = [ lab[0] for lab in labs ]

        for lab in labs:
            for epiweek in epiweeks:
                if join_lab_and_epiweek(lab, epiweek) not in lab_counts_by_epiweek:
                    lab_counts_by_epiweek[join_lab_and_epiweek(lab, epiweek)] = 0

        return lab_counts_by_epiweek

    def __del__(self):
        self.connection.close()