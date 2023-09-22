import pandas as pd
from pandas.testing import assert_frame_equal
from datetime import datetime
from drug_references_dag import clean_pubmed_df

def test_clean_pubmed_df_return_right_data():
    input_json_data = [{
        "id": 9,
        "title": "Gold nanoparticles synthesized from Euphorbia fischeriana root by green route method alleviates the isoprenaline hydrochloride induced myocardial infarction in rats.",
        "date": "01/01/2020",
        "journal": "Journal of photochemistry and photobiology. B, Biology"
    }]
    expected_data = [{
        "id": "9",
        "title": "Gold nanoparticles synthesized from Euphorbia fischeriana root by green route method alleviates the isoprenaline hydrochloride induced myocardial infarction in rats.",
        "date": datetime(2020, 1, 1, 0, 0),
        "journal": "Journal of photochemistry and photobiology. B, Biology"
    }]

    input_df = pd.json_normalize(input_json_data)
    expected_df = pd.json_normalize(expected_data)

    output_df = clean_pubmed_df(input_df)

    assert_frame_equal(output_df, expected_df)