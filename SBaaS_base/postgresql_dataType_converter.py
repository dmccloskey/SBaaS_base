class postgresql_dataType_converter():
    def __init__(self,data_I=None):
        if data_I:
            self.data = data_I;
        else: self.data = None

    def convert_torf2Boolean(self,data_I):
        '''Convert t/f to boolean
        INPUT:
        data_I = string representation of data
        OUTPUT
        data_O = boolean representation of data (if data_I = t/f)'''
        data_O = data_I;
        if data_I is None:
            return data_O;
        elif data_I == 'f':
            data_O = 'FALSE';
        elif data_I == 't':
            data_O = 'TRUE';
        return data_O;

    def convert_text2PostgresqlDataType(self,data_I):
        '''Convert text to boolean
        INPUT:
        data_I = string representation of data
        OUTPUT
        data_O = boolean representation of data (if data_I = t/f)'''
        if data_I is None:
            return data_I;
        data_O = '';
        for d in data_I:
            data_O += self.convert_torf2Boolean(d);
        return data_O; 

    def convert_text2List(self,data_I):
        '''Convert text to a list
        INPUT:
        data_I = '{a,b,c,...}'
        Output:
        data_O = list, [a,b,c,...]
        '''
    
        data_O = None;
        if data_I is None:
            return data_O;
        elif data_I[0] != '{':
            data_O = None;
            data_O = self.convert_text2PostgresqlDataType(data_I);
            return data_O;
        elif data_I[0] == '{' and data_I[:-1] == '}':
            data_tmp = data_I[:];
            data_tmp = data_tmp.replace('{','[');
            data_tmp = data_tmp.replace('}',']');
            data_tmp = self.convert_text2PostgresqlDataType(data_tmp);
            data_O = eval(data_tmp);
            return data_O;
        else:
            return data_O;