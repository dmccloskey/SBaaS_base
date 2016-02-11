
# SBaaS
from SBaaS_base.sbaas_base import sbaas_base
from SBaaS_base.sbaas_base_query_update import sbaas_base_query_update
from SBaaS_base.sbaas_base_query_drop import sbaas_base_query_drop
from SBaaS_base.sbaas_base_query_initialize import sbaas_base_query_initialize
from SBaaS_base.sbaas_base_query_insert import sbaas_base_query_insert
from SBaaS_base.sbaas_base_query_select import sbaas_base_query_select
from SBaaS_base.sbaas_base_query_delete import sbaas_base_query_delete

class sbaas_template_query(sbaas_base):
    #Query rows
    def get_rows_tables(self,
                tables_I,
                query_I,
                output_O,
                dictColumn_I=None,
                verbose_I = False,
                raise_I = False):
        """get rows from tables
        tables_I = [] of strings, table names
        query_I = query dictionary (see documentation in query_select)
        output_O = string, output format (see documentation in query_select)
        dictColumn_I = string, (see document in query_select)
        OUTPUT:
        data_O = listDict, dictList, dictColumn as specified by output_O and dictColumn_I
        """
        data_O = [];
        try:
            table_model = self.convert_tableStringList2SqlalchemyModelDict(tables_I);
            queryselect = sbaas_base_query_select(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
            query = queryselect.make_queryFromString(table_model,query_I);
            data_O = queryselect.get_rows_sqlalchemyModel(
                query_I=query,
                output_O=output_O,
                dictColumn_I=dictColumn_I,
                verbose_I=verbose_I,
                raise_I=raise_I);
        except Exception as e:
            if raise_I: raise;
            else: print(e);
        return data_O;
    def add_rows_table(self,table_I,data_I,
            verbose_I = False,
            raise_I = False):
        '''add rows to table
        INPUT:
        table_I = string, table name
        data_I = add listDict'''
        if data_I:
            try:
                model_I = self.convert_tableString2SqlalchemyModel(table_I);
                queryinsert = sbaas_base_query_insert(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
                queryinsert.add_rows_sqlalchemyModel(model_I,data_I,raise_I=raise_I);
            except Exception as e:
                if raise_I: raise;
                else: print(e);
    def update_rows_table(self,table_I,data_I,
            verbose_I = False,
            raise_I = False):
        '''update rows of table
        INPUT:
        table_I = string, table name
        data_I = update listDict
        NOTES:
        WHERE clause is currently based on the tables primary key(s)
        '''
        if data_I:
            try:
                model_I = self.convert_tableString2SqlalchemyModel(table_I);
                queryupdate = sbaas_base_query_update(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
                queryupdate.update_rows_sqlalchemyModel_primaryKeys(model_I,data_I,verbose_I=verbose_I,raise_I=raise_I);
            except Exception as e:
                if raise_I: raise;
                else: print(e);
    def reset_rows_table(self,table_I,data_I,
            warn_I = False,
            raise_I = False):
        '''delete rows of table
        INPUT:
        table_I = string, table name
        data_I = delete listDict
        NOTES:
        WHERE clause is currently based on the tables primary key(s)
        '''
        if data_I:
            try:
                model_I = self.convert_tableString2SqlalchemyModel(table_I);
                querydelete = sbaas_base_query_delete(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
                querydelete.reset_rows_sqlalchemyModel_primaryKeys(model_I,data_I,warn_I=warn_I,raise_I=raise_I);
            except Exception as e:
                if raise_I: raise;
                else: print(e);
    def initialize_tables(self,
            tables_I = [],):
        try:
            if not tables_I:
                tables_I = list(self.get_supportedTables().keys());
            queryinitialize = sbaas_base_query_initialize(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
            for table in tables_I:
                model_I = self.convert_tableString2SqlalchemyModel(table);
                queryinitialize.initialize_table_sqlalchemyModel(model_I);
        except Exception as e:
            print(e);
    def drop_tables(self,
            tables_I = [],
            warn_I = True):
        try:
            if not tables_I:
                tables_I = list(self.get_supportedTables().keys());
            querydrop = sbaas_base_query_drop(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
            for table in tables_I:
                model_I = self.convert_tableString2SqlalchemyModel(table);
                querydrop.drop_table_sqlalchemyModel(model_I,warn_I);
        except Exception as e:
            print(e);
    def reset_tables(self,
            tables_I = [],
            query_I = {},
            warn_I=True):
        try:
            if not tables_I:
                tables_I = list(self.get_supportedTables().keys());
            querydelete = sbaas_base_query_delete(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
            for table in tables_I:
                table_model = self.convert_tableStringList2SqlalchemyModelDict([table]);
                query = querydelete.make_queryFromString(table_model,query_I);
                querydelete.reset_table_sqlalchemyModel(query_I=query,warn_I=warn_I);
        except Exception as e:
            print(e);
