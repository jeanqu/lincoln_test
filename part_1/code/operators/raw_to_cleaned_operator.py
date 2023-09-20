import pandas as pd
import os

from hooks.local_data_connector_hook import LocalDataConnectorHook


class RawToCleanedOperator():
    # Operator that take a list of files as inputs, 
    # apply a cleaning function if given as parameter,
    # force a schema if given as parameter (not implemented yet)
    # and write the result in a parquet file 

    def __init__(self,
                 input_file_paths: list,
                 output_file_path: str,
                 cleaning_function = None,
                 table_schema = None,
                 *args,
                 **kwargs):
        self.input_file_paths = input_file_paths
        self.output_file_path = output_file_path
        self.cleaning_function = cleaning_function
        self.table_schema = table_schema
        self.local_data_connector_hook = LocalDataConnectorHook()

    def execute(self):
        df = pd.DataFrame()
        for file_path in self.input_file_paths:
            df = pd.concat([df, self.local_data_connector_hook.get_file_data_from_path(file_path)])
        
        if self.cleaning_function is not None:
            df = self.cleaning_function(df)

        print(df)

        self.local_data_connector_hook.write_parquet_file(df, self.output_file_path)



