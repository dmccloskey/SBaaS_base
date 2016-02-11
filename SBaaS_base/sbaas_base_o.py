from .sbaas_base import sbaas_base
from .sbaas_base_query_select import sbaas_base_query_select
# resources
from io_utilities.base_exportData import base_exportData
from ddt_python.ddt_container_table import ddt_container_table
from ddt_python.ddt_container_SQL import ddt_container_SQL

class sbaas_base_o(sbaas_base
    ):

    def export_rows_sqlalchemyModel_csv(self,
                table_model_I,
                query_I,
                filename_O,
                ):
        '''export rows of model_I to filename_O
        INPUT:
        table_model_I = {tablename:sqlalchemy model object,...}
        query_I = dictionary of query parameters
        OUTPUT:
        filename_O = .csv file name/location
        '''

        data = [];
        queryselect = sbaas_base_query_select(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
        query = queryselect.make_queryFromString(table_model_I,query_I);
        data = queryselect.get_rows_sqlalchemyModel(
            query_I=query,
            output_O='listDict',
            dictColumn_I=None,
            verbose_I = False);
        if data:
            # write data to file
            export = base_exportData(data);
            export.write_dict2csv(filename_O);
        else:
            print('rows not found.');

    def export_rows_sqlalchemyModel_table_js(self,
                table_model_I,
                query_I,
                data_dir_I,
                tabletype='responsivecrosstable_01',
                data1_keys=None,
                data1_nestkeys=None,
                data1_keymap=None,
                tabletileheader=None,
                tablefilters=None,
                tableheaders=None
                ):
        '''export rows of model_I to filename_O
        INPUT:
        table_model_I = {tablename:sqlalchemy model object,...}
        query_dict_I = dictionary of query parameters
        data_dir_I = .js file name/location
            if  data_dir_I == 'tmp'
                    the .js file will be written the 'visualization/tmp' direction specified in the settings
                data_dir_I == 'data_json'
                    the .js string will be returned
                all other cases will be written to the .js file name/location specified
        OPTIONAL INPUT to ddt_python:

        '''
        
        # query the data
        data = [];
        queryselect = sbaas_base_query_select(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
        query = queryselect.make_queryFromString(table_model_I,query_I);
        data = queryselect.get_rows_sqlalchemyModel(
            query_I=query,
            output_O='listDict',
            dictColumn_I=None,
            verbose_I = False);

        # dump chart parameters to a js files
        if data1_keys is None or not data1_keys:
            data1_keys=[];
            for model in table_model_I.values():
                data1_keys.extend(queryselect.get_columns_sqlalchemyModel(model));
        if data1_nestkeys is None or not data1_nestkeys:
            data1_nestkeys = data1_keys[0];
        if data1_keymap is None or not data1_keymap:
            data1_keymap = {};
        if tabletileheader is None or not tabletileheader:
            tabletileheader = '';
            for model in table_model_I.values():
                tabletileheader += queryselect.get_tableName_sqlalchemyModel(model);
                tabletileheader += ' and ';
            tabletileheader = tabletileheader[:-5];

        ddttable = ddt_container_table()
        ddttable.make_container_table(
            data,
            data1_keys,
            data1_nestkeys,
            data1_keymap,
            tabletype=tabletype,
            tabletileheader=tabletileheader,
            tableheaders=tableheaders,
            tablefilters=tablefilters,
            );
        
        if data_dir_I=='tmp':
            filename_str = self.settings['visualization_data'] + '/tmp/ddt_data.js';
        elif data_dir_I=='data_json':
            data_json_O = ddttable.get_allObjects_js();
            return data_json_O;
        else:
            filename_str = data_dir_I;
        with open(filename_str,'w') as file:
            file.write(ddttable.get_allObjects());

    def export_rows_sqlalchemyModel_queryForm_js(self,
                table_model_I,
                query_I,
                data_dir_I,
                tabletype='responsivecrosstable_01',
                data1_keys=None,
                data1_nestkeys=None,
                data1_keymap=None,
                tabletileheader=None,
                tablefilters=None,
                tableheaders=None
                ):
        '''export rows of model_I to filename_O
        INPUT:
        table_model_I = {tablename:sqlalchemy model object,...}
        query_dict_I = dictionary of query parameters
        data_dir_I = .js file name/location
            if  data_dir_I == 'tmp'
                    the .js file will be written the 'visualization/tmp' direction specified in the settings
                data_dir_I == 'data_json'
                    the .js string will be returned
                all other cases will be written to the .js file name/location specified
        OPTIONAL INPUT to ddt_python:

        '''
        
        # query the data
        data = [];
        queryselect = sbaas_base_query_select(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
        query = queryselect.make_queryFromString(table_model_I,query_I);
        try:
            data = queryselect.get_rows_sqlalchemyModel(
                query_I=query,
                output_O='listDict',
                dictColumn_I=None,
                verbose_I = False,
                raise_I = True);
            alert=None;
        except Exception as e:
            alert=str(e);

        # dump chart parameters to a js files
        if data1_keys is None or not data1_keys:
            data1_keys=list(data[0].keys());
        if data1_nestkeys is None or not data1_nestkeys:
            data1_nestkeys = [list(data[0].keys())[0]];
        if data1_keymap is None or not data1_keymap:
            data1_keymap = {};
        if tabletileheader is None or not tabletileheader:
            tabletileheader = '';
            for model in table_model_I.values():
                tabletileheader += queryselect.get_tableName_sqlalchemyModel(model);
                tabletileheader += ' and ';
            tabletileheader = tabletileheader[:-5];
        #TODO:
        #refactor into a form menu
        #---
            from_cmd = '';
            for model in table_model_I.values():
                from_cmd += '"' + queryselect.get_tableName_sqlalchemyModel(model) + '"';
                from_cmd += ", ";
            from_cmd = from_cmd[:-2];
        #---

        querytable = ddt_container_SQL();
        #TODO:
        #refactor into a form menu
        #---
        select_cmd,where_cmd,group_by_cmd,having_cmd,order_by_cmd,limit_cmd,offset_cmd = queryselect.make_querySelectClauses(query_I);
        select_str,from_str,where_str,group_by_str,having_str,order_by_str,limit_str,offset_str = queryselect.convert_querySelectClauses2str(select_cmd,where_cmd,group_by_cmd,having_cmd,order_by_cmd,limit_cmd,offset_cmd);
        data_select_I = [
                {
                #'SELECT':data1_keys,
                'SELECT':select_str,
                #'FROM':from_cmd,
                'FROM':from_str,
                'WHERE':where_str,
                'GROUP_BY':group_by_str,
                'HAVING':having_str,
                'ORDER_BY':order_by_str,
                'LIMIT':limit_str,
                'OFFSET':offset_str,
                 }];
        querytable.make_container_querySelectForm(
            data_1=data_select_I,
            rowcnt=1,colcnt=1,datacnt=0,
            htmlalert=alert,
            formurl='pipeline',
            );
        #---
        querytable.make_container_queryInsertUpdateDeleteForm(
                tablename = from_str,
                data_2=data,
                data2_keys=data1_keys,
                data2_nestkeys=data1_nestkeys,
                data2_keymap=data1_keymap,
                rowcnt=1,colcnt=2,datacnt=1,
                formurl='pipeline',
                );
        querytable.make_container_table(
            data,
            data1_keys,
            data1_nestkeys,
            data1_keymap,
            tabletype=tabletype,
            tabletileheader=tabletileheader,
            tableheaders=tableheaders,
            tablefilters=tablefilters,
            rowcnt=2,colcnt=1,
            datacnt=2);
        return querytable.get_allObjects_js();
        
        if data_dir_I=='tmp':
            filename_str = self.settings['visualization_data'] + '/tmp/ddt_data.js';
        elif data_dir_I=='data_json':
            data_json_O = ddttable.get_allObjects_js();
            return data_json_O;
        else:
            filename_str = data_dir_I;
        with open(filename_str,'w') as file:
            file.write(ddttable.get_allObjects());
