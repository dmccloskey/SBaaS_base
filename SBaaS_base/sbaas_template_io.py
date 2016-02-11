from .sbaas_base import sbaas_base
from .sbaas_base_i import sbaas_base_i
from .sbaas_base_o import sbaas_base_o
# Resources
from io_utilities.base_importData import base_importData
from io_utilities.base_exportData import base_exportData
# Import other resources for svgs here...

class sbaas_template_io(sbaas_base):
    '''Template class for io methods
    DESCRIPTION: Abstract class to be used as a mix-in to allow for the inherited object access to templated io methods
    NOTES: Inherit from sbaas_base and call individual i/o class in order to scope access to query methods'''
    def export_rows_tables_csv(self,
        tables_I,
        query_I,
        filename_O):
        '''export rows of table_I to filename_I
        INPUT:
        tables_I = string, table names
        query_I = query dictionary (see documentation in query_select)
        OUTPUT:
        filename_O = string, .csv file name/location
        '''
        sbaasbaseo = sbaas_base_o(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
        table_model = self.convert_tableStringList2SqlalchemyModelDict(tables_I);
        sbaasbaseo.export_rows_sqlalchemyModel_csv(
            table_model_I = table_model,
            query_I=query_I,
            filename_O=filename_O
            );
    
    def import_rows_table_add_csv(self,table_I,filename_I):
        '''Add rows to table_I from filename_I
        INPUT:
        table_I = string, table name
        filename_I = .csv file name/location
        '''
        sbaasbasei = sbaas_base_i(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
        model = self.convert_tableString2SqlalchemyModel(table_I);
        sbaasbasei.import_rows_sqlalchemyModel_add_csv(model_I=model,filename_I=filename_I);

    def import_rows_table_update_csv(self,table_I,filename_I):
        '''update rows of table_I from filename_I
        INPUT:
        table_I = string, table name
        filename_I = .csv file name/location
        '''
        sbaasbasei = sbaas_base_i(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
        model = self.convert_tableString2SqlalchemyModel(table_I);
        sbaasbasei.import_rows_sqlalchemyModel_update_csv(model_I=model,filename_I=filename_I);

    def import_rows_table_reset_csv(self,table_I,filename_I):
        '''reset rows of table_I from filename_I
        INPUT:
        table_I = string, table name
        filename_I = .csv file name/location
        '''
        sbaasbasei = sbaas_base_i(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
        model = self.convert_tableString2SqlalchemyModel(table_I);
        sbaasbasei.import_rows_sqlalchemyModel_reset_csv(model_I=model,filename_I=filename_I);


    def export_rows_tables_table_js(self,
                tables_I,
                query_I,
                data_dir_I,
                tabletype='responsivetable_01',
                data1_keys=None,
                data1_nestkeys=None,
                data1_keymap=None,
                tabletileheader=None,
                tablefilters=None,
                tableheaders=None
                ):
        '''export rows of table_I for query_I to data_dir_I
        INPUT:
        tables_I = sqlalchemy model object
        query_I = dictionary of query parameters
        data_dir_I = .js file name/location
            if  data_dir_I == 'tmp'
                    the .js file will be written the 'visualization/tmp' direction specified in the settings
                data_dir_I == 'data_json'
                    the .js string will be returned
                all other cases will be written to the .js file name/location specified
        OPTIONAL INPUT to ddt_python:

        '''
        sbaasbaseo = sbaas_base_o(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
        table_model = self.convert_tableStringList2SqlalchemyModelDict(tables_I);
        if data_dir_I=='data_json':
            data_json = sbaasbaseo.export_rows_sqlalchemyModel_table_js(
                    table_model_I = table_model,
                    query_I=query_I,
                    data_dir_I=data_dir_I,
                    tabletype='responsivetable_01',
                    data1_keys=data1_keys,
                    data1_nestkeys=data1_nestkeys,
                    data1_keymap=data1_keymap,
                    tabletileheader=tabletileheader,
                    tablefilters=tablefilters,
                    tableheaders=tableheaders
                    );
            return data_json;
        else:
            sbaasbaseo.export_rows_sqlalchemyModel_table_js(
                    table_model_I = table_model,
                    query_I=query_I,
                    data_dir_I=data_dir_I,
                    tabletype='responsivetable_01',
                    data1_keys=data1_keys,
                    data1_nestkeys=data1_nestkeys,
                    data1_keymap=data1_keymap,
                    tabletileheader=tabletileheader,
                    tablefilters=tablefilters,
                    tableheaders=tableheaders
                    );

    def export_rows_table_svg_js(self,
                tables_I,
                query_I,
                data_dir_I,
                ):
        '''export rows of tables_I for query_I to data_dir_I
        INPUT:
        tables_I = [] string, tablenames
        query_I = dictionary of query parameters
        data_dir_I = .js file name/location
            if  data_dir_I == 'tmp'
                    the .js file will be written the 'visualization/tmp' direction specified in the settings
                data_dir_I == 'data_json'
                    the .js string will be returned
                all other cases will be written to the .js file name/location specified
        OPTIONAL INPUT to ddt_python:

        '''
        #Your code here...
        pass;
    def export_rows_table_queryForm_js(self,
                tables_I,
                query_I,
                data_dir_I,
                tabletype='responsivetable_01',
                data1_keys=None,
                data1_nestkeys=None,
                data1_keymap=None,
                tabletileheader=None,
                tablefilters=None,
                tableheaders=None
                ):
        '''export rows of table_I for query_I to data_dir_I
        INPUT:
        tables_I = sqlalchemy model object
        query_I = dictionary of query parameters
        data_dir_I = .js file name/location
            if  data_dir_I == 'tmp'
                    the .js file will be written the 'visualization/tmp' direction specified in the settings
                data_dir_I == 'data_json'
                    the .js string will be returned
                all other cases will be written to the .js file name/location specified
        OPTIONAL INPUT to ddt_python:

        '''
        sbaasbaseo = sbaas_base_o(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
        table_model = self.convert_tableStringList2SqlalchemyModelDict(tables_I);
        if data_dir_I=='data_json':
            data_json = sbaasbaseo.export_rows_sqlalchemyModel_queryForm_js(
                    table_model_I = table_model,
                    query_I=query_I,
                    data_dir_I=data_dir_I,
                    tabletype='responsivetable_01',
                    data1_keys=data1_keys,
                    data1_nestkeys=data1_nestkeys,
                    data1_keymap=data1_keymap,
                    tabletileheader=tabletileheader,
                    tablefilters=tablefilters,
                    tableheaders=tableheaders
                    );
            return data_json;
        else:
            sbaasbaseo.export_rows_sqlalchemyModel_queryForm_js(
                    table_model_I = table_model,
                    query_I=query_I,
                    data_dir_I=data_dir_I,
                    tabletype='responsivetable_01',
                    data1_keys=data1_keys,
                    data1_nestkeys=data1_nestkeys,
                    data1_keymap=data1_keymap,
                    tabletileheader=tabletileheader,
                    tablefilters=tablefilters,
                    tableheaders=tableheaders
                    );

