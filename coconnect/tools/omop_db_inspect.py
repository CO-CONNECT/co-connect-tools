import json
import os
import sqlalchemy as sql
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path


class OMOPDetails():
    def __init__(self):
        env_path = Path('../../scripts')/'.env'
        load_dotenv(dotenv_path=env_path)
        db_name = os.environ.get('OMOP_POSTGRES_DB')
        db_user = os.environ.get('OMOP_POSTGRES_USER')
        db_password = os.environ.get('OMOP_POSTGRES_PASSWORD')
        db_host = os.environ.get('OMOP_POSTGRES_HOST')
        db_port = int(os.environ.get('OMOP_POSTGRES_PORT'))
        # need special format for azure
        # https://github.com/MicrosoftDocs/azure-docs/issues/6371#issuecomment-376997025
        con_str = f'postgresql://{db_user}%40{db_host}:{db_password}@{db_host}:{db_port}/{db_name}'
        self.ngin = sql.create_engine(con_str)
        # self.inspector = sql.inspect(self.ngin)
        # self.schema = 'public'
        # self.omop_tables = [
        #     table
        #     for table in self.inspector.get_table_names(schema=self.schema)
        # ]
        # self.omop_tables.sort()
        # print (json.dumps(self.omop_tables,indent=6))

    def get_concept_table(self, source_concept_id):
        # From OMOP db get concept relationship
        select_from_concept = r'''
        SELECT *
        FROM public.concept
        WHERE concept_id=%s
        '''
        df_concept = pd.read_sql(
            select_from_concept % (source_concept_id), self.ngin).drop(
            ["valid_start_date",
             "valid_end_date",
             "invalid_reason"], axis=1)
        return df_concept

    def get_concept_relationship_table(self, source_concept_id):
        select_from_concept_relationship = r'''
        SELECT *
        FROM public.concept_relationship
        WHERE concept_id_1=%s
        '''
        df_relationship = pd.read_sql(
            select_from_concept_relationship % (source_concept_id), self.ngin).drop(
            ["valid_start_date",
             "valid_end_date",
             "invalid_reason"], axis=1)
        return df_relationship

    def obtain_target_concept_id(self, source_concept_id):
        df_concept = self.get_concept_table(source_concept_id)
        # 1)Check if source_concept_id is Standard or Non-standard
        # 2)Get the relevant target table for the source_concept_id
        if df_concept['standard_concept'] == 'S':
            return source_concept_id
        else:
            df_relationship = self.get_concept_relationship_table(source_concept_id)
            relationships = df_relationship['relationship_id'].tolist()
            for relationship in relationships:
                # if relationship == "Mapped from":
                #     target_concept_id = df_relationship['concept_id_1'].iloc[relationships.index(relationship)]
                #     return target_concept_id
                #     source_concept_id = self.target_concept_id
                #     target_table = df_concept['domain_id'].iloc[relationships.index(relationship)]
                # NOTE: I ran a SQL command in the OMOP DB and found 340 different kinds of relationship IDs.
                # I therefore think we need to think about modifying this conditional to incorporate other possibilities
                if relationship == "Concept same_as to" or relationship == "Mapped to":
                    # Concept must be "Non-Standard"
                    target_concept_id = df_relationship['concept_id_2'].iloc[relationships.index(relationship)]
                    return target_concept_id
                    # target_table = df_concept['domain_id'].iloc[0]

    def obtain_target_table(self, source_concept_id):
        df_concept = self.get_concept_table(source_concept_id)
        # 1)Check if source_concept_id is Standard or Non-standard
        # 2)Get the relevant target table for the source_concept_id
        if df_concept['standard_concept'] == 'S':
            return df_concept['domain_id']
        else:
            df_relationship = self.get_concept_relationship_table(source_concept_id)
            relationships = df_relationship['relationship_id'].tolist()
            for relationship in relationships:
                # if relationship == "Mapped from":
                #     target_concept_id = df_relationship['concept_id_1'].iloc[relationships.index(relationship)]
                #     return target_concept_id
                #     source_concept_id = self.target_concept_id
                #     target_table = df_concept['domain_id'].iloc[relationships.index(relationship)]
                # NOTE: I ran a SQL command in the OMOP DB and found 340 different kinds of relationship IDs.
                # I therefore think we need to think about modifying this conditional to incorporate other possibilities
                if relationship == "Concept same_as to" or relationship == "Mapped to":
                    # Concept must be "Non-Standard"
                    target_concept_id = df_relationship['concept_id_2'].iloc[relationships.index(relationship)]
                    target_df_concept = self.get_concept_table(target_concept_id)
                    return target_df_concept['domain_id']

concept_id=4060225
detail1=OMOPDetails()
print(detail1.obtain_target_concept_id(concept_id))
# print(detail1.target_table)
# print(detail1.is_standard)
# exit(0)


# selection = r'''
# SELECT *
# FROM public.concept_synonym
# WHERE concept_id=%s
# '''# 8507 8532
# df = pd.read_sql(selection%('8507'),ngin)
# print (df)

# selection = r'''
# SELECT *
# FROM public.domain
# WHERE domain_id LIKE '%s'
# '''%(f'%%{domain_id}%%')
# df = pd.read_sql(selection,ngin)
# print (df)


# selection = r'''
# SELECT *
# FROM public.location_history
# '''
# df = pd.read_sql(selection,ngin)
# print (df)
# exit(0)


# selection = r'''
# SELECT *
# FROM public.concept_class
# WHERE concept_class_id LIKE '%s';
# '''%(f"%%{class_id}%%")

# df = pd.read_sql(selection,ngin)
# print (df.columns)
# print (df)
# print ()


# for table in filter(lambda tab: 'concept' in tab, omop_tables):
#     selection = r'''
#     SELECT *
#     FROM public.%s
#     LIMIT 1
#     '''%(table)
#     df = pd.read_sql(selection,ngin)
#     print ({table:df.columns})


# def get_full_df(table,schema=schema):
#     selection = r'''
#     SELECT *
#     FROM %s.%s
#     LIMIT 10
#     '''%(schema,table)
#     return pd.read_sql(selection,ngin)

# #print (get_full_df('person').columns.values)
# #print (get_full_df('source_to_concept_map'))

# dict={}
# for table in omop_tables:
#     dict[table] = list(get_full_df(table).columns.values)

# print (json.dump(dict,open('info.json','w'),indent=6))


# #omop_fields = {
# #    table: get_df(table).columns
# #    for table in omop_tables
# #}
# #print (omop_fields)
