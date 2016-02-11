from .sbaas_base_o import sbaas_base_o
# resources
from io_utilities.base_exportData import base_exportData
from ddt_python.ddt_container_table import ddt_container_table

class sbaas_analysis_o(sbaas_base_o
    ):

    def export_rows_analysisID_sqlalchemyModel_csv(self,model_I,
                analysis_id_I,
                filename_O,
                used__I=True,
                ):
        '''export rows of model_I to filename_O
        INPUT:
        model_I = sqlalchemy model object
        analysis_id_I = string,
        OUTPUT:
        filename_O = .csv file name/location
        '''

        data = [];
        queryselect = sbaas_base_query_select(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
        data = queryselect.get_rows_analysisID_sqlalchemyModel(analysis_id_I,used__I);
        #data = self.get_rows_analysisID_sqlalchemyModel(analysis_id_I,used__I);
        if data:
            # write data to file
            export = base_exportData(data);
            export.write_dict2csv(filename_O);
        else:
            print('rows not found.');

    def export_rows_analysisID_sqlalchemyModel_table_js(self,
        model_I,
        analysis_id_I,
        data1_keys,
        data1_nestkeys,
        data1_keymap,
        used__I=True,
        tabletype_I='responsivecrosstable_01',
        data_dir_I='tmp'):
        '''Export a tabular representation of the data
        INPUT:
        model_I = sqlalchemy model object
        analysis_id_I = string,
        '''

        #get the data for the analysis
        data_O = [];
        queryselect = sbaas_base_query_select(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
        data_O = queryselect.get_rows_analysisID_sqlalchemyModel(analysis_id_I,used__I);
        #data_O = self.get_rows_analysisID_sqlalchemyModel(analysis_id_I,used__I);

        ddttable = ddt_container_table()
        ddttable.make_container_table(data_O,data1_keys,data1_nestkeys,data1_keymap,tabletype=tabletype_I);

        if data_dir_I=='tmp':
            filename_str = self.settings['visualization_data'] + '/tmp/ddt_data.js'
        elif data_dir_I=='data_json':
            data_json_O = ddtutilities.get_allObjects_js();
            return data_json_O;
        with open(filename_str,'w') as file:
            file.write(ddttable.get_allObjects());
