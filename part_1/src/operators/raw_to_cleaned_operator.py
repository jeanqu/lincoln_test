import pandas as pd
import os

from hooks.local_data_connector_hook import LocalDataConnectorHook


class RawToCleanedOperator():
    # Operator that take a list of files as inputs, 
    # apply a cleaning function if given as parameter,
    # force a schema if given as parameter (not implemented yet)
    # and write the result in a json file. 

    def __init__(self,
                 input_file_paths: list,
                 output_file_path: str,
                 cleaning_function = None,
                 table_schema = None,
                 dry_run: bool = False,  # If dry_run is true, no data is writen.
                 *args,
                 **kwargs):
        self.input_file_paths = input_file_paths
        self.output_file_path = output_file_path
        self.cleaning_function = cleaning_function
        self.table_schema = table_schema
        self.dry_run = dry_run
        self.local_data_connector_hook = LocalDataConnectorHook()

    def execute(self):
        df_to_clean = pd.DataFrame()
        for file_path in self.input_file_paths:
            df_to_clean = pd.concat([df_to_clean, self.local_data_connector_hook.get_file_data_from_path(file_path)])
        
        if self.cleaning_function is not None:
            df_to_clean = self.cleaning_function(df_to_clean)

        if not self.dry_run:
            self.local_data_connector_hook.write_json_file(df_to_clean, self.output_file_path)

        return df_to_clean



