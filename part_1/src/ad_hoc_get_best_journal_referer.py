import pandas as pd
from pandasql import *


def get_relative_data_path_from_suffix(input_file_path: str) -> str:
        return f'../data/{input_file_path}'

def ad_hoc_get_best_journal_referer() -> str:
    input_df = pd.read_json(get_relative_data_path_from_suffix('aggregated/mentioned_drugs.json'))


    best_journal = sqldf(f"""
            SELECT journal, 
                   COUNT(DISTINCT atccode) AS distinct_drugs_nb
            FROM input_df
            GROUP BY journal
            ORDER BY distinct_drugs_nb DESC
            LIMIT 1;""")
    best_journal_name = best_journal.to_dict(orient = 'records')[0]
    
    print(f"Le nom du journal qui mentionne le plus de médicaments est {best_journal_name['journal']}, il en mentionne {best_journal_name['distinct_drugs_nb']} différents !")

    return best_journal_name['journal']

ad_hoc_get_best_journal_referer()