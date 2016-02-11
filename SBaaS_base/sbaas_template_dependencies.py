#import sqlalchemy classes here...

class sbaas_template_dependencies():
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
