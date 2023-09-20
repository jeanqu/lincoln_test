import pandas as pd

from operators.raw_to_cleaned_operator import RawToCleanedOperator
from common.date_helper import standardize_date_format

def clean_clinical_trials_df(df):
    df['date'] = df['date'].apply(standardize_date_format)
    df['date'] = pd.to_datetime(df['date'])
    return df

def clean_pubmed_df(df):
    # The id does not seems to be mandatory for the moment, so we just keep it with the null values as str.
    df['id'] = df['id'].apply(lambda id: str(id))
    df['id'] = df['id'].astype('str')
    df['date'] = df['date'].apply(standardize_date_format)
    df['date'] = pd.to_datetime(df['date'])
    return df


def drug_references_dag():

    RawToCleanedOperator(
        input_file_paths = ['raw/clinical_trials.csv'],
        output_file_path = 'cleaned/clinical_trials.parquet',
        cleaning_function = clean_clinical_trials_df
    ).execute()

    RawToCleanedOperator(
        input_file_paths = ['raw/pubmed.csv', 'raw/pubmed.json'],
        output_file_path = 'cleaned/pubmed.parquet',
        cleaning_function = clean_pubmed_df
    ).execute()

drug_references_dag()
