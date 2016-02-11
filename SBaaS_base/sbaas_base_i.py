from .sbaas_base import sbaas_base
from .sbaas_base_query_insert import sbaas_base_query_insert
from .sbaas_base_query_update import sbaas_base_query_update
from .sbaas_base_query_delete import sbaas_base_query_delete
# resources
from io_utilities.base_importData import base_importData

class sbaas_base_i(sbaas_base
    ):
    
    def import_rows_sqlalchemyModel_add_csv(self,model_I,filename_I):
        '''Add rows to model_I from filename_I
        INPUT:
        model_I = sqlalchemy model object
        filename_I = .csv file name/location
        '''
        data = base_importData();
        data.read_csv(filename_I);
        data.format_data();
        queryinsert = sbaas_base_query_insert(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
        queryinsert.add_rows_sqlalchemyModel(model_I,data.data);
        data.clear_data();

    def import_rows_sqlalchemyModel_update_csv(self,model_I,filename_I):
        '''update rows of model_I from filename_I
        INPUT:
        model_I = sqlalchemy model object
        filename_I = .csv file name/location
        '''
        data = base_importData();
        data.read_csv(filename_I);
        data.format_data();
        queryupdate = sbaas_base_query_update(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
        queryupdate.update_rows_sqlalchemyModel_primaryKeys(model_I,data.data);
        data.clear_data();

    def import_rows_sqlalchemyModel_reset_csv(self,model_I,filename_I):
        '''reset rows of model_I from filename_I
        INPUT:
        model_I = sqlalchemy model object
        filename_I = .csv file name/location
        '''
        data = base_importData();
        data.read_csv(filename_I);
        data.format_data();
        querydelete = sbaas_base_query_delete(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
        querydelete.reset_rows_sqlalchemyModel_primaryKeys(model_I,data.data);
        data.clear_data();
    
    def import_rows_sqlalchemyModel_add_listDict(self,model_I,listDict_I):
        '''Add rows to model_I from listDict_I
        INPUT:
        model_I = sqlalchemy model object
        listDict_I = [{}]
        '''
        queryinsert = sbaas_base_query_insert(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
        queryinsert.add_rows_sqlalchemyModel(model_I,listDict_I);

    def import_rows_sqlalchemyModel_update_listDict(self,model_I,filename_I):
        '''update rows of model_I from listDict_I
        INPUT:
        model_I = sqlalchemy model object
        listDict_I = [{}]
        '''
        queryupdate = sbaas_base_query_update(session_I=self.session,engine_I=self.engine,settings_I=self.settings,data_I=self.data);
        queryupdate.update_rows_sqlalchemyModel_primaryKeys(model_I,listDict_I);
