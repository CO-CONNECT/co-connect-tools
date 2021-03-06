import os
import pandas as pd
import numpy as np
import json
import copy
import collections

from .operations import OperationTools
from coconnect.tools.logger import Logger
from .objects import Person, ConditionOccurrence, VisitOccurrence, Measurement, Observation


#lookup for name to class, e.g. "person" : Person
_classes = {
    x.name: x
    for x in [Person, ConditionOccurrence,
              VisitOccurrence, Measurement,
              Observation]
}

class NoInputFiles(Exception):
    pass


class CommonDataModelTypes(collections.OrderedDict):
    def __init__(self):
        super().__init__()
        self['INTEGER'] = lambda x : pd.to_numeric(x,errors='coerce').astype('Int64')
        self['FLOAT'] = lambda x : pd.to_numeric(x,errors='coerce').astype('Float64')
        self['VARCHAR(60)'] = lambda x : x.fillna('').astype(str).apply(lambda x: x[:60])
        self['VARCHAR(50)'] = lambda x : x.fillna('').astype(str).apply(lambda x: x[:50])
        self['VARCHAR(20)'] = lambda x : x.fillna('').astype(str).apply(lambda x: x[:20])
        self['VARCHAR(10)'] = lambda x : x.fillna('').astype(str).apply(lambda x: x[:10])
        self['VARCHAR'] = lambda x : x.fillna('').astype(str).apply(lambda x: x)
        self['STRING(50)'] = lambda x : x.fillna('').astype(str).apply(lambda x: x[:50])
        self['DATETIME'] = lambda x : pd.to_datetime(x,errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        self['DATE'] = lambda x : pd.to_datetime(x,errors='coerce').dt.date
    

class CommonDataModel:

    inputs = None
    output_folder = "output_data/"

    
    def __init__(self,**kwargs):

        name = self.__class__.__name__
        if 'name' in kwargs:
            name = kwargs['name']
            
        self.logger = Logger(self.__class__.__name__)
        self.logger.info("CommonDataModel created")

        self.dtypes = CommonDataModelTypes()

        if 'output_folder' in kwargs:
            self.output_folder = kwargs['output_folder']
        
        if 'inputs' in kwargs:
            inputs = kwargs['inputs']
            if not isinstance(inputs,dict):
                self.logger.error(inputs)
                raise NoInputFiles("setting up inputs that are not a dict!!")

            if not self.inputs is None:
                self.logger.waring("overwriting inputs")

            self.inputs = inputs

        if self.inputs == None:
            raise NoInputFiles('You need to set or specify the input files.')

            
        #register opereation tools
        self.tools = OperationTools()

        #self.__dict__.update(self.__class__.__dict__)

        self.person_id_masker = None
        self.omop = {}


        
    def set_indexing(self,index_map,strict_check=False):
        if self.inputs == None:
            raise NoInputFiles('Trying to indexing before any inputs have been setup')
        
        for key,index in index_map.items():
            if key not in self.inputs:
                self.logger.warning(f"trying to set index '{index}' for '{key}' but this has not been loaded as an inputs!")
                continue

            if index not in self.inputs[key].columns:
                self.logger.error(f"trying to set index '{index}' on dataset '{key}', but this index is not in the columns! something really wrong!")
                continue
                
            self.inputs[key].index = self.inputs[key][index].rename('index') 


    def get_cdm_class(self,class_type):
        if class_type in _classes:
            return _classes[class_type]()
    
    def get_objs(self,class_type):
        self.logger.debug(f"looking for {class_type}")
        return  [
            getattr(self,x)
            for x in dir(self)
            if isinstance(getattr(self,x),class_type)
        ]
    
    def run_cdm(self,class_type):
        objects = self.get_objs(class_type)
        nobjects = len(objects)
        extra = ""
        if nobjects>1:
            extra="s"
        self.logger.info(f"for {class_type.name}: found {nobjects} object{extra}")
        
        if len(objects) == 0:
            return
        
        #execute them all
        dfs = []
        self.logger.info(f"working on {class_type}")
        for i,obj in enumerate(objects):
            obj.execute(self)
            df = obj.get_df()
            self.logger.info(f"finished {obj.name} "
                             f"... {i}/{len(objects)}, {len(df)} rows") 
            if len(df) == 0:
                self.logger.warning(f".. {i}/{len(objects)}  no outputs were found ")
                continue

            dfs.append(df)

        #merge together
        self.logger.info(f'Merging {len(dfs)} objects for {class_type}')
        df_destination = pd.concat(dfs,ignore_index=True)

        self.logger.info(f'Masking the person_id for {class_type}')
        df_destination = self.mask_person_id(df_destination)
        
        self.logger.info(f'Finalising {class_type}')
        df_destination = objects[0].finalise(df_destination)
        
        self.logger.info(f'Formating the output for {class_type}')
        df_destination = objects[0].format(df_destination,raise_error=False)

        return df_destination


    def mask_person_id(self,df):
        if 'person_id' in df.columns:
            #if masker has not been defined, define it
            if self.person_id_masker is None:
                self.person_id_masker = {
                    x:i+1
                    for i,x in enumerate(df['person_id'].unique())
                }
            #apply the masking
            df['person_id'] = df['person_id'].map(self.person_id_masker)
            self.logger.info(f"Just masked person_id")
        return df
        
    def process(self,output_folder='output_data/'):
        
        if not self.output_folder is None:
            output_folder = self.output_folder
        
        self._df_map = {}
        self._df_map[Person.name] = self.run_cdm(Person)
        self.logger.info(f'finalised {Person.name}')

        self._df_map[ConditionOccurrence.name] = self.run_cdm(ConditionOccurrence)
        self.logger.info(f'finalised {ConditionOccurrence.name}')

        self._df_map[VisitOccurrence.name] = self.run_cdm(VisitOccurrence)
        self.logger.info(f'finalised {VisitOccurrence.name}')

        self._df_map[Measurement.name] = self.run_cdm(Measurement)
        self.logger.info(f'finalised {Measurement.name}')

        self._df_map[Observation.name] = self.run_cdm(Observation)
        self.logger.info(f'finalised {Observation.name}')

        self.save_to_file(self._df_map,output_folder)

        #register output
        self.omop = self._df_map
        
        
    def save_to_file(self,df_map,f_out):
        for name,df in df_map.items():
            if df is None:
                continue
            fname = f'{f_out}/{name}.csv'
            if not os.path.exists(f'{f_out}'):
                self.logger.info(f'making output folder {f_out}')
                os.mkdir(f'{f_out}')
            self.logger.info(f'saving {name} to {fname}')
            df.set_index(df.columns[0],inplace=True)
            df.to_csv(fname,index=True)
            self.logger.info(df.dropna(axis=1,how='all'))
        

