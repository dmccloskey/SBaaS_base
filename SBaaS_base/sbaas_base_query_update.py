from .sbaas_base_query_select import sbaas_base_query_select
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy import update
from sqlalchemy import text

class sbaas_base_query_update(sbaas_base_query_select):

    def update_rows_sqlalchemyModel_id(self, model_I,data_I):
        '''update rows of model_I using the row index
        REQUIREMENTS:
        table must contain a column of type sequence "id" that is unique
        INPUT:
        model_I = sqlalchemy model object
        data_I = listDict of table rows to update
        '''
        #get the model columns (in order)
        model_columns = self.get_columns_sqlalchemyModel(
            model_I,exclude_I=['id']
            );
        #update the data
        if data_I:
            for d in data_I:
                try:
                    if not 'id' in d.keys():
                        print('column "id" not found');
                        return;
                    #convert data row in update dict
                    input_dict = self.convert_dict2UpdateDict(d,model_columns);
                    #update the table
                    data_update = self.session.query(model_I).filter(
                            model_I.id==d['id']).update(
                            input_dict,
                            synchronize_session=False);
                    if data_update == 0:
                        print('row not found.')
                        print(d)
                except IntegrityError as e:
                    print(e);
                except SQLAlchemyError as e:
                    print(e);
                except Exception as e:
                    print(e);
            self.session.commit();

    def update_row_sqlalchemyModel(self,query_I,verbose_I=False,raise_I=False):
        '''update row of a table
        INPUT:
        query_I = {}, query dictionary
        update: [{'model':sqlalchemy model object}]
        where: [{'table_name':sqlalchemy table_name object,'column_name':'','value':'','operator':'','connector':''},...]
            where value = string, float, boolean, etc., or [] or nested 'where':[]
            where operator is of '<', 'LIKE', '=', 'IN' etc.,
            where connector is of 'AND' or 'OR'
        set: [{'model':sqlalchemy model object, 'values_dict':{column_name:value}}]
        
        raise_I = boolean, raise error
        '''
        #update the data
        try:
            update_cmd = None;
            where_cmd = None;
            set_cmd = None;
            # generate the query clauses
            if 'update' in query_I.keys():
                update_cmd = self.make_update(query_I['update']);
            if 'where' in query_I.keys():
                where_cmd = self.make_where(query_I['where']);
            if 'set' in query_I.keys():
                set_cmd = self.make_set(query_I['set']);
            # make the query statement
            query_cmd = None;
            query_cmd = update(update_cmd).where(where_cmd).values(set_cmd);
            if verbose_I:
                print(self.convert_sqlalchemyQuery2PostgresqlString(query_cmd));
            # execute the update
            self.execute_update(query_cmd,raise_I=raise_I);
        except IntegrityError as e:
            if raise_I: raise;
            else: print(e);
        except SQLAlchemyError as e:
            if raise_I: raise;
            else: print(e);
        except Exception as e:
            if raise_I: raise;
            else: print(e);

    def make_set(self,set_I):
        '''make the set clause
        INPUT:
        set_I = {};
        OUTPUT:
        set_O = sqlalchemy text
        '''
        set_str = '';
        set_O = {};
        if len(set_I)>1:
            print('only single table updates are supported.');
            return set_O;
        for row in set_I:
            set_O=row['values_dict'];
        #    tablename = self.get_tableName_sqlalchemyModel(row['model']);
        #    for column,value in row['values_dict'].items():
        #        columnname = self.make_tableColumnStr(tablename,column);
        #        clause = ("%s = %s, " %(columnname,value));
        #        set_str += clause;
        #set_str = set_str[:-2]; #remove trailing ', '
        #set_O = text(set_str);
        return set_O;
    def make_update(self,update_I):
        '''make the update table
        INPUT:
        update_I = []
        OUTPUT:
        update_O = sqlalchemy table object;
        '''
        update_O = None;
        try:
            if len(update_I)>1:
                print('multiple table updates is not supported.');
                return None;
            for row in update_I:
                update_O=row['model'];
        except Exception as e:
            print(e);
        return update_O

    def update_rows_sqlalchemyModel_primaryKeys(self,model_I,data_I,verbose_I=False,raise_I=False):
        '''update rows of model_I using the row primary keys
        INPUT:
        model_I = sqlalchemy model object
        data_I = listDict of table rows to update
        raise_I = boolean, raise error

        '''
        #get the model columns (in order)
        primary_keys = self.get_primaryKeys_sqlalchemyModel(model_I)
        model_columns = self.get_columns_sqlalchemyModel(
            model_I,exclude_I=primary_keys
            );
        #update the data
        if data_I:
            for d in data_I:
                try:
                    query = {};
                    query['update']=[{'model':model_I}];
                    query['where'] = self.make_whereFromPrimaryKeys(model_I,primary_keys,d);
                    query['set']=[{'model':model_I,'values_dict':d}];
                    self.update_row_sqlalchemyModel(query,verbose_I=False,raise_I=raise_I);
                except IntegrityError as e:
                    if raise_I: raise;
                    else: print(e);
                except SQLAlchemyError as e:
                    if raise_I: raise;
                    else: print(e);
                except Exception as e:
                    if raise_I: raise;
                    else: print(e);
            self.session.commit();

    def execute_update(self,query_I,raise_I=False):
        '''execute a raw sql query
        INPUT:
        query_I = string or sqlalchemy text or sqlalchemy select
        raise_I = boolean, raise error

        '''
        try:
            ans = self.session.execute(query_I);
            if ans == 0:
                if raise_I:
                    raise Exception('row not found.');
                else:
                    print('row not found.');
                    print(d);
            else:
                self.session.commit();
                updated_O = True;
        except SQLAlchemyError as e:
            self.session.rollback();
            if raise_I: raise;
            else: print(e);
        except Exception as e:
            self.session.rollback();
            if raise_I: raise;
            else: print(e);
