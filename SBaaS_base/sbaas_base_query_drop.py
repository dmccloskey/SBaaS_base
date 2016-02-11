from .sbaas_base_query_select import sbaas_base_query_select
from sqlalchemy.exc import SQLAlchemyError

class sbaas_base_query_drop(sbaas_base_query_select):
    def drop_table_sqlalchemyModel(self, model_I, warn_I=True):
        '''drop table
        INPUT:
        model_I = sqlalchemy model
        warn_I = if True, a warning message will be displayed
        '''
        try:
            if warn_I:
                input_str = 'drop table ' + self.get_tableName_sqlalchemyModel(model_I) + '? [y/n]: ';
                yorno = input(input_str);
                if yorno == 'y':
                    model_I.__table__.drop(self.engine,True);  
            else:
                model_I.__table__.drop(self.engine,True);                            
        except SQLAlchemyError as e:
            print(e);