import pandas as pd

from operators.raw_to_cleaned_operator import RawToCleanedOperator
from operators.find_drugs_mentioned_operator import FindDrugsMentionedOperator
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
    df.reset_index(drop=True, inplace=True)
    return df


def drug_references_dag():
    cleaned_clinical_trials_path = 'cleaned/clinical_trials.json'
    cleaned_pubmed_path = 'cleaned/pubmed.json'

    RawToCleanedOperator(
        input_file_paths = ['raw/clinical_trials.csv'],
        output_file_path = cleaned_clinical_trials_path,
        cleaning_function = clean_clinical_trials_df
    ).execute()

    RawToCleanedOperator(
        input_file_paths = ['raw/pubmed.csv', 'raw/pubmed.json'],
        output_file_path = cleaned_pubmed_path,
        cleaning_function = clean_pubmed_df
    ).execute()

    FindDrugsMentionedOperator(
                 clinical_trials_path = cleaned_clinical_trials_path,
                 pubmed_path = cleaned_pubmed_path,
                 drugs_path = 'raw/drugs.csv',
                 output_path = 'aggregated/mentioned_drugs.json'
    ).execute()

drug_references_dag()
