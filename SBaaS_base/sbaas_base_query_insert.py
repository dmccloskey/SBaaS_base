from .sbaas_base_query_select import sbaas_base_query_select
from sqlalchemy.exc import SQLAlchemyError

class sbaas_base_query_insert(sbaas_base_query_select):

    def add_rows_sqlalchemyModel(
        self,model_I,data_I,
        raise_I=False,safeInsert_I=False):
        '''add rows to model_I
        INPUT:
        model_I = sqlalchemy model object
        data_I = listDict of table rows to add
        raise_I = boolean, raise error
        safeInsert_I = boolean, if True, each row is committed one-by-one
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
                    input_dict = self.convert_dict2InputDict(d,model_columns);
                    # add data to the model
                    data_add = model_I(input_dict);
                    self.session.add(data_add);
                    if safeInsert_I:
                        try:
                            self.session.commit();
                        except SQLAlchemyError as e:
                            self.session.rollback();
                            if raise_I: raise;
                            #else: print(e);
                except SQLAlchemyError as e:
                    self.session.rollback();
                    if raise_I: raise;
                    else: print(e);
                except Exception as e:
                    self.session.rollback();
                    if raise_I: raise;
                    else: print(e);
            #commit the added data:
            if not safeInsert_I:
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

    def execute_insert(self,query_I,raise_I=False):
        '''execute a raw sql query
        INPUT:
        query_I = string or sqlalchemy text or sqlalchemy select
        raise_I = boolean, raise error

        '''
        try:
            ans = self.session.execute(query_I);
            self.session.commit();
        except SQLAlchemyError as e:
            self.session.rollback();
            if raise_I: raise;
            else: print(e);
        except Exception as e:
            self.session.rollback();
            if raise_I: raise;
            else: print(e);

