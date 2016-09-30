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
    def set_data(self,data_I):
        '''set data'''
        self.data = data_I;
    def add_data(self,data_I):
        '''add data'''
        self.data.extend(data_I);
    def get_data(self,data_I):
        '''get the data catch'''
        return self.data;

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

    def convert_dict2InputDict(self,dict_I,order_I):
        '''Convert a dictionary into an ordered dictionary filling None for empty row columns
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

    def convert_dict2UpdateDict(self,dict_I,order_I):
        '''Convert a dictionary into an ordered dictionary NOT filling None for empty row columns
        INPUT:
        dict_I = dictionary
        order_I = list of keys in dict_I
        OUTPUT:
        dict_O = dictionary of keys specified in order_I and values specified in dict_I
        '''
        dict_O = {};
        try:
            for k in order_I:
                if k in dict_I.keys():
                    dict_O[k] = dict_I[k];
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

    def convert_list2string(self,list_I,deliminator_I=','):
        '''convert a list 2 a string
        INPUT:
        list_I = list
        deliminator_I = string
        OUTPUT:
        string_O = string
        '''
        string_O = '';
        if type(list_I)==type([]):
            string_O = deliminator_I.join(list_I);
        elif type(list_I)==type(''):
            string_O = list_I;
        else:
            print('type of list_I is not supported.')
        return string_O;

    def hashJoin(self,table1, index1, table2, index2):
        '''Implement the hashJoin algorithm across two lists of tuples
        https://rosettacode.org/wiki/Hash_join#Python
        INPUT:
        table1 = list of tuples, dictionaries, lists, etc.
        index1 = integer (tuple/list index),
                 string/integer (dict key),
                 function (composite key)
        table2 = same type as table1
        index2 = same type as index1
        OUTPUT:
        ...

        EXAMPLE1:
        table1 = [(27, "Jonah"),
                  (18, "Alan"),
                  (28, "Glory"),
                  (18, "Popeye"),
                  (28, "Alan")]
        table2 = [("Jonah", "Whales"),
                  ("Jonah", "Spiders"),
                  ("Alan", "Ghosts"),
                  ("Alan", "Zombies"),
                  ("Glory", "Buffy")]
 
        for row in hashJoin(table1, 1, table2, 0):
            print(row)

        EXAMPLE1 OUTPUT:
        (27, 'Jonah', 'Jonah', 'Whales')
        (27, 'Jonah', 'Jonah', 'Spiders's)
        (18, 'Alan', 'Alan', 'Ghosts')
        (28, 'Alan', 'Alan', 'Ghosts')
        (18, 'Alan', 'Alan', 'Zombies')
        (28, 'Alan', 'Alan', 'Zombies')
        (28, 'Glory', 'Glory', 'Buffy')

        EXAMPLE2;
        table1 = [{'age':27, 'person':"Jonah"},
                  {'age':18, 'person':"Alan"},
                  {'age':28, 'person':"Glory"},
                  {'age':18, 'person':"Popeye"},
                  {'age':28, 'person':"Alan"}]
        table2 = [{'person':"Jonah", 'book':"Whales"},
                  {'person':"Jonah", 'book':"Spiders"},
                  {'person':"Alan", 'book':"Ghosts"},
                  {'person':"Alan", 'book':"Zombies"},
                  {'person':"Glory", 'book':"Buffy"}]
        for row in hashJoin(table1, 'person', table2, 'person'):
            print(row)

        '''
        from collections import defaultdict
        from copy import copy

        table_O = [];
        if not table1 or len(table1)<1 or not table2 or len(table2)<1:
            return table_O;

        h = defaultdict(list)
        # hash phase
        for s in table1:
            h[s[index1]].append(s)
        # join phase
        if str(type(table1[0]))=="<class 'dict'>":
            for r in table2:
                for s in h[r[index2]]:
                    t = copy(s)
                    t.update(r)
                    table_O.append(t);
            #table_O = [{**s, **r} for r in table2 for s in h[r[index2]]]; #python 3.5+
        elif str(type(table1[0]))=="<class 'list'>" or \
            str(type(table1[0]))=="<class 'tuple'>":
            table_O = [s+r for r in table2 for s in h[r[index2]]];
        else:
            print('type not supported.');
        return table_O;
