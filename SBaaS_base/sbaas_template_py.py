
class sbaas_template_py():
    def make_headerAndClasses_py(self,
        classes,
        imports = ['from SBaaS_base.postgresql_orm_base import *']):
        '''generate the .py text postgresql_model file
        INPUT: 
        classes = list of class strings
        columns = additional list of import statements
        OUTPUT
        file_py = string
        '''
        file_py = ''
        for imp in imports:
            file_py += '%s\n' %imp
        for model in classes:
            file_py += model;
        return file_py

    def make_postgresql_modelClasses_py(
        self,
        table_name,columns):
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

    def make_queryClasses_py(
        self,
        class_name,table_names):
        '''generate the .py text for an sqlalchemy model class
        INPUT: 
        table_name = string
        columns = string
        OUTPUT
        text_py = string
        '''
        class_name = 'class %s(sbaas_template_query):\n' %class_name

        init = '    def initialize_supportedTables(self):\n'
        init += "        tables_supported = {\n"
        for table_name in table_names:
            init += ("        '%s':%s,\n" %(table_name,table_name))
        init += "        }\n"
        init += "        self.set_supportedTables(tables_supported)\n"

        text_py = ("%s%s" %(class_name,init))
        return text_py

"""
sqlalchemy_models = []
for k,v in table_columns.items():
    table_name = 'models_biocyc_%s'%k
    columns = v.split(',');
    columns.append('database')
    sqlalchemy_models.append(make_postgresql_modelClasses_py(table_name,columns))

imports = ['from SBaaS_base.postgresql_orm_base import *']
print(make_headerAndClasses_py(sqlalchemy_models,imports))

query_classes = []
table_names = ['models_biocyc_%s'%table for table in table_columns.keys()]
query_classes.append(make_queryClasses_py('models_BioCyc_query',table_names))
 
imports = [
'from SBaaS_base.sbaas_base_query_delete import sbaas_base_query_delete',
'from SBaaS_base.sbaas_template_query import sbaas_template_query',
'from .models_BioCyc_postgresql_models import *',]
print(make_headerAndClasses_py(query_classes,imports))
 """