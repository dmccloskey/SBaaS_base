'''Base class for sbaas'''
from sqlalchemy.dialects import postgresql

class sbaas_base():
    def __init__(self,session_I=None,engine_I=None,settings_I={},data_I=None,tableString2SqlalchemyModel_I={}):
        # base properties
        if session_I: self.session = session_I;
        else: self.session = None;
        if engine_I: self.engine = engine_I;
        else: self.engine = None;
        if settings_I: self.settings = settings_I;
        else: self.settings = {};
        if data_I: self.data = data_I;
        else: self.data = [];

        #defined for each class based on the dependent database tables
        if tableString2SqlalchemyModel_I: self.tableString2SqlalchemyModel = tableString2SqlalchemyModel_I;
        else: self.tableString2SqlalchemyModel = {};

    def set_session(self,session_I):
        '''set the session object'''
        self.session = session_I;
    def set_engine(self,engine_I):
        '''set the engine object'''
        self.engine = engine_I;
    def set_settings(self,settings_I):
        '''set the settings object'''
        self.settings = settings_I;
    def get_session(self,session_I):
        '''get the session object'''
        self.session = session_I;
    def get_engine(self,engine_I):
        '''get the engine object'''
        self.engine = engine_I;
    def get_settings(self,settings_I):
        '''get the settings object'''
        self.settings = settings_I;

    def clear_data(self):
        '''clear the data catch'''
        del self.data[:];
    def add_data(self,data_I):
        '''add data'''
        self.data = data_I;

    def convert_datetime2string(self,datetime_I):
        '''convert datetime to string date time 
        e.g. time.strftime('%Y/%m/%d %H:%M:%S') = '2014-04-15 15:51:01' '''

        from time import mktime,strftime

        time_str = datetime_I.strftime('%Y-%m-%d %H:%M:%S')
        
        return time_str
    def convert_string2datetime_mdYHM(self,datetime_I):
        '''convert string date time to datetime
        e.g. time.strptime('4/15/2014 15:51','%m/%d/%Y %H:%M')'''

        from time import mktime,strptime
        from datetime import datetime

        time_struct = strptime(datetime_I,'%m/%d/%Y %H:%M')
        dt_O = datetime.fromtimestamp(mktime(time_struct))
        
        return dt_O
    def convert_string2datetime(self,datetime_I):
        '''convert string date time to datetime
        e.g. time.strptime('2014-04-15 15:51:01','%Y/%m/%d %H:%M:%S')'''

        from time import mktime,strptime
        from datetime import datetime

        time_struct = strptime(datetime_I,'%Y-%m-%d %H:%M:%S')
        dt_O = datetime.fromtimestamp(mktime(time_struct))
        
        return dt_O

    def convert_dict2OrderedTuple(self,dict_I,order_I):
        '''Convert a dictionary into an ordered tuple
        INPUT:
        dict_I = dictionary
        order_I = list of keys in dict_I
        OUTPUT:
        tuple_O = tuple of dict_I values in order specified by order_I
        '''
        tuple_O = None;
        try:
            input_list = [];
            for k in order_I:
                value = None;
                if k in dict_I.keys():
                    value = dict_I[k];
                input_list.append(value);
            tuple_O = tuple(input_list);
            return tuple_O;
        except Exception as e:
            print(e);

    def convert_dict2UpdateDict(self,dict_I,order_I):
        '''Convert a dictionary into an ordered tuple
        INPUT:
        dict_I = dictionary
        order_I = list of keys in dict_I
        OUTPUT:
        dict_O = dictionary of keys specified in order_I and values specified in dict_I
        '''
        dict_O = {};
        try:
            for k in order_I:
                value = None;
                if k in dict_I.keys():
                    value = dict_I[k];
                dict_O[k] = value;
            return dict_O;
        except Exception as e:
            print(e);

    def convert_string2StringString(self,string_I):
        '''Convert a string to a string within a string
        INPUT:
        string_I
        OUTPUT:
        string_O
        '''
        string_O = ("'%s'" %(string_I));
        return string_O;

    def convert_usedBoolean2String(self,used__I):
        '''convert used_ boolean to used_ string'''

        if used__I:
            used_='true';
        else:
            used_='false';
        return used_;

    def convert_sqlalchemyQuery2PostgresqlString(self,query_I):
        '''convert sqlalchemy query object to postgresql compliant string'''
        query_O = None;
        try:
            query_O = str(query_I.compile(dialect=postgresql.dialect()));
        except AttributeError as e:
            try:
                query_O = str(query_I.statement.compile(dialect=postgresql.dialect()));
            except AttributeError as ae:
                print(e);
                print(ae);
        except Exception as ex:
            print(ex);
        return query_O;

    def set_supportedTables(self,table_model_I):
        '''Set the dictionary of supported tables
        INPUT:
        tables_O = {} tablename:sqlalchemy object
        '''
        self.tableString2SqlalchemyModel = table_model_I;

    def get_supportedTables(self):
        '''return a dictionary of supported tables
        OUTPUT:
        tables_O = {} tablename:sqlalchemy object
        '''
        tables_O = self.tableString2SqlalchemyModel;
        return tables_O;

    def convert_tableString2SqlalchemyModel(self,table_I):
        '''convert table name to sqlalchemy model
        INPUT:
        table_I = string name of the sqlalchemyModel object
        OUTPUT
        model_O = sqlalchemy model object
        '''
        
        tables = self.get_supportedTables();
        model_O = None;
        try:
            if table_I in tables.keys():
                model_O = tables[table_I];
            else:
                print("table " + table_I + " not recognized.");
        except Exception as e:
            print(e);
        return model_O;

    def convert_tableStringList2SqlalchemyModelDict(self,tables_I):
        '''convert table name to sqlalchemy model
        INPUT:
        tables_I = [], string name of the sqlalchemyModel object
        OUTPUT
        table_model_O = {table_name:model}
        '''
        
        tables = self.get_supportedTables();
        table_model_O = {};
        try:
            for table in tables_I:
                if table in tables.keys():
                    model_O = tables[table];
                    table_model_O[table]=model_O;
                else:
                    print("table " + table + " not recognized.");
        except Exception as e:
            print(e);
        return table_model_O;