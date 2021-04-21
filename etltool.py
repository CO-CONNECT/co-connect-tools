import logging
import json
import pandas as pd
import os


def read_rules():
    with open('/Users/roberto/tmp2/panther2.json', 'r') as f:
        content = f.read()

    obj = json.loads(content)

    # obj.keys: metadata, cdm
    # obj['cdm'].keys: condition_occurrence, person, observation, measurement
    print(len(obj['cdm']['person']))
    print(obj['cdm']['person'][0].keys())
    return obj


def process_person():
    """
    This method currently only process the mandatory fields: person_id,
    gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id.
    :return:
    """
    person_df = pd.DataFrame({
        'person_id': pd.Series([], dtype='int'),
        'gender_concept_id': pd.Series([], dtype='int'),
        'year_of_birth': pd.Series([], dtype='int'),
        'month_of_birth': pd.Series([], dtype='int'),
        'day_of_birth': pd.Series([], dtype='int'),
        'birth_datetime': pd.Series([]),
        'race_concept_id': pd.Series([], dtype='int'),
        'ethnicity_concept_id': pd.Series([], dtype='int'),
        'location_id': pd.Series([], dtype='int'),
        'provider_id': pd.Series([], dtype='int'),
        'care_site_id': pd.Series([], dtype='int'),
        'person_source_value': pd.Series([]),
        'gender_source_value': pd.Series([]),
        'gender_source_concept_id': pd.Series([], dtype='int'),
        'race_source_value': pd.Series([]),
        'race_source_concept_id': pd.Series([], dtype='int'),
        'ethnicity_source_value': pd.Series([]),
        'ethnicity_source_concept_id': pd.Series([], dtype='int'),
    })

    mandatory_fields = [
        'person_id',
        'gender_concept_id',
    ]

    rules = read_rules()
    person_rules = rules['cdm']['person'][0]  # TODO move to a function

    is_valid = True

    for field in mandatory_fields:
        if field not in person_rules.keys():
            is_valid = False
            logging.error('Missing mandadory field person:{}'.format(field))

    if not is_valid:
        logging.error('Stopping the process. There are missing mandadory fields in the person domain.')

    source_table = pd.read_csv(os.path.join('/Users/roberto/tmp2', person_rules['person_id']['source_table']))
    source_table.columns = [x.lower() for x in source_table.columns]

    if 'person_id' in person_rules.keys():
        person_df['person_id'] = source_table[person_rules['person_id']['source_field']]

    if 'gender_concept_id' in person_rules.keys():
        source_column_gender = person_rules['gender_concept_id']['source_field']
        gender_series_original = source_table[source_column_gender].copy()
        gender_series = gender_series_original.copy()

        for term in person_rules['gender_concept_id']['term_mapping'].keys():
            gender_series.loc[gender_series == term] = person_rules['gender_concept_id']['term_mapping'][term]

        person_df['gender_concept_id'] = gender_series
        person_df['gender_source_value'] = gender_series_original

    # print(person_df.head())
    person_df.to_csv(
        '/Users/roberto/tmp2/person__.csv',
        na_rep='',
        index=False,
    )


def process_observation():
    pass


if __name__ == "__main__":
    process_person()
    process_observation()


