from configparser import SafeConfigParser
import os as __os
from os.path import split as __split, join as __join, abspath as __abspath, \
    isfile as __isfile

class postgresql_settings():
    def __init__(self,filename_I=None):
        self.database_settings = {};
        self.datadir_settings = {};
        if filename_I:
            try:
                config = self.read_settings(filename_I);
                self.database_settings.update(self.get_database_variables(config));
                self.datadir_settings.update(self.get_datadir_variables(config));
            except Exception as e:
                print(e);

    def read_settings(self,filename_I):
        '''read settings from file'''
        config = None;
        ##test if file exists
        #import os.path
        #os.path.isfile(filename_I)
        try:
            config = SafeConfigParser();
            config.read(filename_I);
        except FileNotFoundError as e:
            print(e);
        except Exception as e:
            print(e);
        return config;


    def set_database_config(self,host_I,database_I,password_I,schema_I,user_I):
        '''set the database settings

        '''
        config = SafeConfigParser();
        # set the default settings for the database
        config.add_section("DATABASE")

        # user defined settings for the database
        config.set("DATABASE", "host", host_I)
        config.set("DATABASE", "database", database_I)
        config.set("DATABASE", "password", password_I)
        config.set("DATABASE", "schema", schema_I)
        config.set("DATABASE", "user", user_I)

        return config;

    def set_datadir_config(self,name_location_dict_I):
        '''set the data directories for the project
        INPUT:
        name_location_dict_I = {name:location}

        '''
        config = self.add_section_config("DATA_DIR",name_location_dict_I);
        return config;

    def add_sections_config(self,sections_I):
        '''add multiple sections and settings
        INPUT:
        sections_I = {section_name:{key:value,...}}'''
        config = SafeConfigParser();
        for section,kv in sections_I.items():
            config.add_section(section);
            for k,v in kv.items():
                config.set(section,k,v);
        return config;

    def add_section_config(self,section_I,key_value_I):
        '''function to add a new section and settings
        INPUT:
        section_I = string, section name
        key_value_I = string dict, {setting:attribute}
        '''
        config = SafeConfigParser();
        config.add_section(section_I);
        for k,v in key_value_I.items():
            config.set(section_I,k,v);
        return config;

    def get_variables_config(self,config_I,section_I,variables_I=[]):
        '''function to return variables from a section
        INPUT:
        config_I = configuration object
        section_I = name of the section
        variables_I = name of variables to extraction'''
        variables_O = {};
        for v in variables_I:
            variables_O[v] = config_I.get(section_I,v);
        return variables_O;

    def get_database_variables(self,config_I,variables_I=["host",'database','password','schema','user']):
        '''get variables for the database'''
        db_variables = {};
        try:
            db_variables = self.get_variables_config(config_I,"DATABASE",variables_I);
        except Exception as e:
            print(e);
        return db_variables;

    def get_datadir_variables(self,config_I,variables_I=["sbaas",'workspace','workspace_data','visualization_data','visualization_resources','drive','github']):
        '''get variables for the data directories'''
        datadir_variables = {};
        try:
            datadir_variables = self.get_variables_config(config_I,"DATA_DIR",variables_I);
        except Exception as e:
            print(e);
        return datadir_variables;

    def write_settings(self,filename_O):
        '''write settings to file
        INPUT:
        filename_O = name of output file (.ini)'''
        settings = {};
        settings['DATABASE'] = self.database_settings;
        settings['DATA_DIR'] = self.datadir_settings;
        config = self.add_sections_config(settings);
        with open(filename_O,'w') as file:
            config.write(file);

    def write_defaultSettings(self,filename_O):
        '''write default settings to file'''

        config = SafeConfigParser();
        # set the default settings for the database
        config.add_section("DATABASE")
        config.set("DATABASE", "host", "localhost:5432")
        config.set("DATABASE", "database", "metabolomics")
        config.set("DATABASE", "password", "dmccloskey")
        config.set("DATABASE", "schema", "public")
        config.set("DATABASE", "user", "dmccloskey")
        # set the default settings for the data directories
        config.add_section("DATA_DIR")
        config.set("DATA_DIR", "sbaas", "C:/Users/dmccloskey-sbrg/Documents/GitHub/sbaas")
        config.set("DATA_DIR", "workspace", "C:/Users/dmccloskey-sbrg/Documents/GitHub/sbaas_workspace/sbaas_workspace/workspace")
        config.set("DATA_DIR", "workspace_data", "C:/Users/dmccloskey-sbrg/Documents/GitHub/sbaas_workspace/sbaas_workspace/workspace_data")
        config.set("DATA_DIR", "visualization_data", "C:/Users/dmccloskey-sbrg/Documents/GitHub/sbaas_workspace/sbaas_workspace/visualization_data")
        config.set("DATA_DIR", "visualization_resources", "C:/Users/dmccloskey-sbrg/Documents/GitHub/sbaas_workspace/sbaas_workspace/visualization_resources")
        
        with open(filename_O,'w') as file:
            config.write(file);       

    def clear_settings(self):
        '''clear existing settings'''
        self.database_settings = {};
        self.datadir_settings = {};