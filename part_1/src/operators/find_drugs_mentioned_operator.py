from pandasql import *
import pandas as pd

from hooks.local_data_connector_hook import LocalDataConnectorHook


class FindDrugsMentionedOperator():
    # In order to separate the code we will put here the buisness logic,
    # but in an Airflow project we could use an existing PythonOperator and write the function elsewhere.

    def __init__(self,
                 clinical_trials_path: str,
                 pubmed_path: str,
                 drugs_path: str,
                 output_path: str,
                 dry_run: bool = False,  # If dry_run is true, no data is writen.

    ):
        self.clinical_trials_path = clinical_trials_path
        self.pubmed_path = pubmed_path
        self.drugs_path = drugs_path
        self.output_path = output_path
        self.dry_run = dry_run
        self.local_data_connector_hook = LocalDataConnectorHook()

    def execute(self):
        #pysqldf = lambda q: sqldf(q, globals())

        clinical_trials = self.local_data_connector_hook.get_file_data_from_path(self.clinical_trials_path)
        pubmed = self.local_data_connector_hook.get_file_data_from_path(self.pubmed_path)
        drugs = self.local_data_connector_hook.get_file_data_from_path(self.drugs_path)


        join_drugs_and_clinical_trials = self.__join_source_with_drug_on_string_contains(
            drugs, 
            clinical_trials, 
            'scientific_title',
            'clinical_trials')
        
        join_drugs_and_pubmed = self.__join_source_with_drug_on_string_contains(
            drugs, 
            pubmed, 
            'title',
            'pubmed')

        union_drugs_and_pubmed = pd.concat([join_drugs_and_clinical_trials, join_drugs_and_pubmed])

        if not self.dry_run:
            self.local_data_connector_hook.write_json_file(union_drugs_and_pubmed, self.output_path)

        return union_drugs_and_pubmed




    def __join_source_with_drug_on_string_contains(self, df_drugs: pd.DataFrame, df_contains: pd.DataFrame, key_contains: str, article_type: str) -> pd.DataFrame:
        # Here we use pandasql to make the custom join request, SQL being the easiest way to read code for simple joins.
        # We will not output the drugs that are not mentioned in the result

        return sqldf(f"""
            SELECT atccode, 
                   drug, 
                   id, 
                   {key_contains} AS publication_title,
                   '{article_type}' AS article_type,
                   journal,
                   date 
            FROM df_drugs
            LEFT JOIN df_contains 
                ON UPPER({key_contains}) LIKE '%'|| UPPER(drug) ||'%'
            WHERE id IS NOT NULL;""")

