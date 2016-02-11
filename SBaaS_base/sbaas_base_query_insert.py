from .sbaas_base_query_select import sbaas_base_query_select
from sqlalchemy.exc import SQLAlchemyError

class sbaas_base_query_insert(sbaas_base_query_select):

    def add_rows_sqlalchemyModel(self,model_I,data_I,raise_I=False):
        '''add rows to model_I
        INPUT:
        model_I = sqlalchemy model object
        data_I = listDict of table rows to add
        raise_I = boolean, raise error
        '''
        #get the model columns (in order)
        model_columns = self.get_columns_sqlalchemyModel(
            model_I,
            #exclude_I=['id','rid','wid']
            );
        #add in the data
        if data_I:
            #add in the data row by row
            for d in data_I:
                try:
                    #convert data row in update dict
                    input_dict = self.convert_dict2UpdateDict(d,model_columns);
                    # add data to the model
                    data_add = model_I(input_dict);
                    self.session.add(data_add);
                except SQLAlchemyError as e:
                    #self.session.rollback();
                    if raise_I: raise;
                    else: print(e);
                except Exception as e:
                    if raise_I: raise;
                    else: print(e);
            #commit the added data:
            try:
                self.session.commit();
            except SQLAlchemyError as e:
                self.session.rollback();
                if raise_I: raise;
                else: print(e);
            except Exception as e:
                self.session.rollback();
                if raise_I: raise;
                else: print(e);

