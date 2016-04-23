from .sbaas_base_query_select import sbaas_base_query_select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import select

class sbaas_base_query_delete(sbaas_base_query_select):
    def reset_table_sqlalchemyModel(self,query_I,warn_I=True,raise_I=False):
        '''delete rows
        INPUT:
        query_I {}, query dictionary
            delete_from: sqlalchemy model object
            where: {sqlalchemy model object:{column_name:{value:,operator:,}}
                where operator is of '<', 'LIKE', '==', etc.,
        conn.execute(users.delete().where(users.c.name > 'm'))
        '''
        try:
            delete_cmd = None;
            where_cmd = None;
            # generate the query clauses
            if 'delete_from' in query_I.keys():
                delete_cmd = self.make_deleteFrom(query_I['delete_from']);
            if 'where' in query_I.keys():
                where_cmd = self.make_where(query_I['where']);
            if not delete_cmd is None and not where_cmd is None:
                #query_cmd = select(delete_cmd).where(where_cmd);
                self.execute_deleteCmd(delete_cmd,where_cmd,warn_I=warn_I,raise_I=raise_I);
                #reset = delete_cmd.delete().where(where_cmd);
        except SQLAlchemyError as e:
            if raise_I: raise;
            else: print(e);

    def make_deleteFrom(self,delete_from_I):
        '''make the delete_from clause
        INPUT:
        delete_from_I = [{'model':sqlalchemymodel}];
        OUTPUT:
        delete_from_O = sqlalchemy test
        '''
        delete_from_str = '';
        delete_from_O = delete_from_I[0]['model'];
        return delete_from_O

    def execute_deleteCmd(self,delete_cmd,where_cmd,warn_I=True,raise_I=False):
        '''execute a raw sql query
        INPUT:
        delete_cmd
        where_cmd
        warn_I
        raise_I
        OUTPUT:
        data_O = keyed tuple sqlalchemy object
        '''
        data_O = None;
        try:
            reset = self.session.query(delete_cmd).filter(where_cmd).delete(synchronize_session=False);
            if warn_I:
                # warn the user
                print(str(reset) + ' deleted from ' + self.get_tableName_sqlalchemyModel(delete_cmd));
                yorno = input("commit delete? [y/n]: ");
                if yorno == 'y':
                    self.session.commit();
                else:
                    self.session.rollback();
            else:
                self.session.commit();
        except SQLAlchemyError as e:
            self.session.rollback;
            if raise_I: raise;
            else: print(e);

    def reset_rows_sqlalchemyModel_primaryKeys(self,model_I,data_I,warn_I=False,raise_I=False):
        '''update rows of model_I using the row primary keys
        INPUT:
        model_I = sqlalchemy model object
        data_I = listDict of table rows to be deleted
        '''
        #get the model columns (in order)
        primary_keys = self.get_primaryKeys_sqlalchemyModel(model_I);
        #update the data
        if data_I:
            for d in data_I:
                try:
                    query = {};
                    query['delete_from']=[{'model':model_I}];
                    query['where'] = self.make_whereFromPrimaryKeys(model_I,primary_keys,d);
                    self.reset_table_sqlalchemyModel(query,warn_I=warn_I,raise_I=raise_I);
                except Exception as e:
                    if raise_I: raise;
                    else: print(e);

    def execute_delete(self,query_I,warn_I=False,raise_I=False):
        '''execute a raw sql query
        INPUT:
        query_I = string or sqlalchemy text or sqlalchemy select
        raise_I = boolean, raise error
        OUTPUT:
        data_O = keyed tuple sqlalchemy object
        '''
        try:
            reset = self.session.execute(query_I);            
            if warn_I:
                # warn the user
                print(str(reset) + ' deleted');
                yorno = input("commit delete? [y/n]: ");
                if yorno == 'y':
                    self.session.commit();
                else:
                    self.session.rollback();
            else:
                self.session.commit();
        except SQLAlchemyError as e:
            self.session.rollback();
            if raise_I: raise;
            else: print(e);