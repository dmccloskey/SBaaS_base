from io_utilities.base_importData import base_importData
from io_utilities.base_exportData import base_exportData

class postgresql_models(object):

    def convert_listDict2DictColumn(self,rows_I,column_I):
        '''convert a [{},...] to {:[{},...],...}
        INPUT:
        rows_I = list of dictionaries
        column_I = name of the column
        OUTPUT:
        rows_O = dict of lists of dictionaries
        '''
        rows_O = {};
        try:
            if rows_I: 
                for d in rows_I:
                    if d[column_I] in rows_O.keys():
                        rows_O[d[column_I]].append(d);
                    else:
                        rows_O[d[column_I]] = [];
                        rows_O[d[column_I]].append(d);
            else:
                print("no rows found");
        except Exception as e:
            print(e);
        return rows_O;

    def make_module_py_csv(self,
        filename_O,
        attributes_filename_I,
        constraints_filename_I=None,
        imports_I = ['from SBaaS_base.postgresql_orm_base import *']
        ):
        '''generate the .py text postgresql_model file
        INPUT:
        filename_O = .txt file to write the script to
        constraints_filename_I = .csv file with keys=parameters of make_postgresql_modelConstraints_py
        attributes_filename_I = .csv file with keys=parameters of make_postgresql_modelAttributes_py
        imports_I = additional list of import statements
        '''
        #parse the input data
        io = base_importData();
        attributes = None;
        constraints = None;
        if not attributes_filename_I is None:
            io.read_csv(attributes_filename_I);
            attributes = self.convert_listDict2DictColumn(io.data,'table_name_I');
            io.clear_data();
        constraint_keys = ['foreignKeyConstraints_I','uniqueConstraints_I','primaryKeyConstraint_I','checkConstraint_I']
        if not constraints_filename_I is None:
            io.read_csv(constraints_filename_I);
            constraints = {};
            for d in io.data:
                if not d['table_name_I'] in constraints.keys():constraints[d['table_name_I']] = {};
                if not d['constraint_name_I'] in constraints[d['table_name_I']].keys():constraints[d['table_name_I']][d['constraint_name_I']] = [];
                if d['constraint_name_I'] in ['primaryKeyConstraint_I']:
                    constraints[d['table_name_I']][d['constraint_name_I']]=d['constraint_I']
                else:
                    constraints[d['table_name_I']][d['constraint_name_I']].append(d['constraint_I'])
            io.clear_data();

        #make the classes
        classes = [];
        for k in attributes.keys():
            if constraints is None:
                class_py = self.make_postgresql_modelClass_py(k,attributes[k],constraints_I=None);
            else:
                class_py = self.make_postgresql_modelClass_py(k,attributes[k],constraints_I=constraints[k]);
            classes.append(class_py);

        #make the module
        module_py = self.make_headerAndClasses_py(classes=classes,imports=imports_I);

        #write the module to file
        with open(filename_O, 'w') as f:
            #f.write("{}".format(module_py)) #python 2.7+
            print("{}".format(module_py), file=f) #python 3+

    def make_headerAndClasses_py(self,
        classes,
        imports = ['from SBaaS_base.postgresql_orm_base import *']
        ):
        '''generate the .py text postgresql_model file
        INPUT: 
        classes = list of class strings
        imports = additional list of import statements
        OUTPUT
        file_py = string
        '''
        file_py = ''
        for imp in imports:
            file_py += '%s\n' %imp
        for model in classes:
            file_py += model;
        return file_py

    def make_postgresql_modelConstraints_py(self,
        foreignKeyConstraints_I=None,
        uniqueConstraints_I=None,
        primaryKeyConstraint_I=None,
        checkConstraint_I=None,
        ):
        '''
        generate the .py text for an sqlalchemy model constraint
        INPUT:
        foreignKeyConstraints_I = list of tuples of form ([table_columns],[foreign_table_columns])
        uniqueConstraints_I = list of strings of column names where each list is a different comma seperated string of column names
        primaryKeyConstraint_I = string of comma seperated column names
        checkConstraint_I = list of strings specifying the check
        OUTPUT:
        '''
        table_args_O = "    __table_args__ = (\n"
        if foreignKeyConstraints_I and not foreignKeyConstraints_I is None:
            #TODO:...
            for constraint in foreignKeyConstraints_I:
                table_args_O += "        ForeignKeyConstraint(%s,%s),\n"%(constraint[0],constraint[1])
        if uniqueConstraints_I and not uniqueConstraints_I is None:
            for constraint in uniqueConstraints_I:
                table_args_O += "        UniqueConstraint("
                for c in constraint.split(','):
                    table_args_O += "'%s',"%(c)
                table_args_O = table_args_O[:-1];
                table_args_O += "),\n"
        if primaryKeyConstraint_I and not primaryKeyConstraint_I is None:
            table_args_O += "        PrimaryKeyConstraint("
            for constraint in primaryKeyConstraint_I.split(','):
                table_args_O += "'%s',"%(constraint)
            table_args_O = table_args_O[:-1];
            table_args_O += "),\n"
        if checkConstraint_I and not checkConstraint_I is None:
            for constraint in checkConstraint_I:
                table_args_O += "        CheckConstraint(%s),\n"%(constraint)
        table_args_O += "    )\n"

        return table_args_O

    def make_postgresql_modelAttributes_py(self,
        name_I,
        type_I='Text',
        default_I=None,
        doc_I=None,
        nullable_I=False
        ):
        '''
        generate the .py text for an sqlalchemy model Column
        '''
        attribute_O = "    %s = Column(%s)\n" %(name_I,type_I);
        return attribute_O;

    def make_postgresql_modelClass_py(self,table_name_I,attributes_I,constraints_I):
        '''generate the .py text for an sqlalchemy model class
        INPUT: 
        table_name_I = string
        attributes_I = list of dicts with keys=parameters of make_postgresql_modelAttributes_py
        constraints_I = dict with keys=parameters of make_postgresql_modelConstraints_py
        OUTPUT
        text_py = string
        '''

        from .sbaas_base import sbaas_base
        sbase = sbaas_base();

        class_name = 'class %s(Base):\n' %table_name_I
    
        tablename = "    __tablename__ = '%s'\n" %table_name_I
    
        #attributes = "    id = Column(Integer, Sequence('%s_id_seq'), primary_key=True)\n" %table_name_I
        attributes = "    id = Column(Integer, Sequence('%s_id_seq'))\n" %table_name_I
        for column in attributes_I:
            attributes += self.make_postgresql_modelAttributes_py(**sbase.check_parameters(column,self.make_postgresql_modelAttributes_py));
        attributes += '    used_ = Column(Boolean)\n    comment_ = Column(Text)\n'
        
        if constraints_I:
            table_args = self.make_postgresql_modelConstraints_py(**sbase.check_parameters(constraints_I,self.make_postgresql_modelConstraints_py));
        else:
            table_args = "    #__table_args__ = (UniqueConstraint('',),)\n"
    
        init = '    def __init__(self,row_dict_I,):\n'
        for column in attributes_I:
            init += ("        self.%s = row_dict_I['%s']\n" %(column['name_I'],column['name_I']))
        init += "        self.used_=row_dict_I['used_']\n        self.comment_=row_dict_I['comment_']\n"

        set_row = '    def __set__row__(self, '
        for column in attributes_I:
            set_row += "%s_I,"%column['name_I']
        set_row += 'used__I,comment__I):\n'
        for column in attributes_I:
            set_row += ("        self.%s = %s_I\n" %(column['name_I'],column['name_I']))
        set_row += "        self.used_ = used__I\n        self.comment_ = comment__I\n"
    
        repr_dict = '    def __repr__dict__(self):\n        return {\n'
        for column in attributes_I:
            repr_dict += ("        '%s':self.%s,\n" %(column['name_I'],column['name_I']))
        repr_dict += "        'id':self.id,\n        'used_':self.used_,\n        'comment_':self.comment_,\n"
        repr_dict += '        }\n'
    
        repr_json = "    def __repr__json__(self):\n        return json.dumps(self.__repr__dict__())\n"
    
        text_py = ("%s%s%s%s%s%s%s%s" %(class_name,tablename,attributes,table_args,init,set_row,repr_dict,repr_json))
        return text_py

    def make_postgresql_modelClasses_py(table_name,columns):
        '''generate the .py text for an sqlalchemy model class
        INPUT: 
        table_name = string
        columns = string
        OUTPUT
        text_py = string
        '''
        class_name = 'class %s(Base):\n' %table_name
    
        tablename = "    __tablename__ = '%s'\n" %table_name
    
        attributes = "    id = Column(Integer, Sequence('%s_id_seq'), primary_key=True)\n" %table_name
        for column in columns:
            attributes += "    %s = Column(Text)\n" %column
        attributes += '    used_ = Column(Boolean)\n    comment_ = Column(Text)\n'
    
        table_args = "    #__table_args__ = (UniqueConstraint('',),)\n"
    
        init = '    def __init__(self,row_dict_I,):\n'
        for column in columns:
            init += ("        self.%s = row_dict_I['%s']\n" %(column,column))
        init += "        self.used_=row_dict_I['used_']\n        self.comment_=row_dict_I['comment_']\n"

        set_row = '    def __set__row__(self,'
        for column in columns:
            set_row += "%s_I,"%column
        set_row += 'used__I,comment__I):\n'
        for column in columns:
            set_row += ("        self.%s = %s_I\n" %(column,column))
        set_row += "        self.used_ = used__I\n        self.comment_ = comment__I\n"
    
        repr_dict = '    def __repr__dict__(self):\n        return {\n'
        for column in columns:
            repr_dict += ("        '%s':self.%s,\n" %(column,column))
        repr_dict += "        'id':self.id,\n        'used_':self.used_,\n        'comment_':self.comment_,\n"
        repr_dict += '        }\n'
    
        repr_json = "    def __repr__json__(self):\n        return json.dumps(self.__repr__dict__())\n"
    
        text_py = ("%s%s%s%s%s%s%s%s" %(class_name,tablename,attributes,table_args,init,set_row,repr_dict,repr_json))
        return text_py




