import pandas as pd
import json5


class LocalDataConnectorHook():
    # Simple hook to get and write simple files to the local database.
    # The python format it returns is a pandas dataframe, as it is simple to use and the most know format.
    # The use of a hook may be overkill (connector to simple, no credentials..), 
    # but we create it to show how a most complicate problem would be like :) 


    def __init__(self):
        pass

    def get_file_data_from_path(self, input_file_path: str) -> pd.DataFrame:
        print(f"Extracting data from {input_file_path}")
        
        if self.__get_file_format_by_suffix(input_file_path) == 'csv':
            return self.__read_csv_file(input_file_path)
        elif self.__get_file_format_by_suffix(input_file_path) == 'json':
            return self.__read_json_file(input_file_path)
        else:
            raise TypeError(f"Cannot read {input_file_path} only csv are json are allowed") 

    def __read_csv_file(self, input_file_path: str) -> pd.DataFrame:
        return pd.read_csv(self.__get_relative_data_path_from_suffix(input_file_path))

    def __read_json_file(self, input_file_path: str) -> pd.DataFrame:
        try:
            return pd.read_json(self.__get_relative_data_path_from_suffix(input_file_path))
        except ValueError:
            # We need to use a more tolerant json reader library
            with open(self.__get_relative_data_path_from_suffix(input_file_path)) as fp:
                json_data = json5.load(fp)
            return pd.DataFrame(json_data)

    def __get_file_format_by_suffix(self, input_file_path: str) -> str:
        return input_file_path.split('.')[-1]

    def __get_relative_data_path_from_suffix(self, input_file_path: str) -> str:
        return f'../data/{input_file_path}'

    def write_json_file(self, df, output_file_path: str):
        print(f'Writing file {self.__get_relative_data_path_from_suffix(output_file_path)}')
        df.to_json(self.__get_relative_data_path_from_suffix(output_file_path), orient = 'records', indent=2 )
