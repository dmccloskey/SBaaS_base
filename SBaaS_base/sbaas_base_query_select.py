from .sbaas_base import sbaas_base
from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from sqlalchemy.sql import select
from sqlalchemy import func

class sbaas_base_query_select(sbaas_base):
    def get_constraint_sqlalchemyModel(self,model_I,constraint_I):
        '''return a dictionary of sql constraints
        INPUT:
        model_I = sqlalchemy class object
        constraint_I = string, constraint type
            foreign_keys
            unique_constraints
            pk_constraint
        OUTPUT:
        data_O = [] or {} depending on the constraint
        '''
        data_O = None;
        if constraint_I == 'foreign_keys':
            data_O = insp.get_foreign_keys(self.get_tableName_sqlalchemyModel(model_I));
            #[]
        elif constraint_I == 'unique_constraints':
            data_O = insp.get_unique_constraints(self.get_tableName_sqlalchemyModel(model_I));
            #[{'name': 'visualization_project_project_id_analysis_id_data_export_id_key', 'column_names': ['project_id', 'analysis_id', 'data_export_id', 'container_export_id', 'pipeline_id']}]
        elif constraint_I == 'pk_constraint':
            data_O = insp.get_pk_constraint(self.get_tableName_sqlalchemyModel(model_I));
            #{'constrained_columns': ['id'], 'name': 'visualization_project_pkey'}
        else:
            print('constraint not recognized');
        return data_O;

    def get_primaryKeys_sqlalchemyModel(self,
                                       model_I):
        '''Query primary key constraint for a given sqlalchemy model
        INPUT:
        model_I = sqlalchemy class object
        model_I
        OUTPUT:
        primary_keys_O = list of sqlalchemy columns in order

        result = dict([(c.name, any([c.primary_key, c.unique])) for c in User.__table__.columns])
        '''
        inst = inspect(model_I);
        primary_keys_O = [key.name for key in inst.primary_key]
        return primary_keys_O;

    def get_columns_sqlalchemyModel(self,
        model_I,exclude_I=[]):
        '''Query column names for a given sqlalchemyModel
        INPUT:
        model_I = sqlalchemy class object
        exclude_I = [] of strings, column names to exclude
        OUTPUT:
        columns_O = list of sqlalchemy columns in order
        '''
        inst = inspect(model_I);
        columns_O = [c_attr.key for c_attr in inst.mapper.column_attrs if not c_attr.key in exclude_I];
        #columns = [m.key for m in model_I.__table__.columns];
        return columns_O;

    def get_tableName_sqlalchemyModel(self,
        model_I):
        '''Query column names for a given sqlalchemyModel
        INPUT:
        model_I = sqlalchemy class object
        OUTPUT:
        tablename_O = string, name of the model table
        '''
        tablename_O = model_I.__tablename__;
        return tablename_O;

    def get_columnAttributeDataType_sqlalchemyModel(self,
        model_I,column_I):
        '''Query column names for a given sqlalchemyModel
        INPUT:
        model_I = sqlalchemy class object
        column_I = string
        OUTPUT:
        columntype_O = data type of column
        '''
        try:
            columnattr = self.get_columnAttribute_sqlalchemyModel(model_I,column_I);
            columntype_O = columnattr.prop.columns[0].type
            return columntype_O;
        except SQLAlchemyError as e:
            print(e);

    def get_columnAttribute_sqlalchemyModel(self,
        model_I,column_I):
        '''Query column names for a given sqlalchemyModel
        INPUT:
        model_I = sqlalchemy class object
        column_I = string
        OUTPUT:
        column_O = name of the model table
        '''
        try:
            column_O = model_I.__dict__[column_I];
            return column_O;
        except SQLAlchemyError as e:
            print(e);

    def get_rows_sqlalchemyModel(self,
        query_I,
        output_O='listDict',
        dictColumn_I=None,
        verbose_I = False,
        raise_I = False):
        '''
        query rows from model_I based on query_I

        INPUT:
        model_I = sqlalchemy model object
        query_I = {}, query dictionary
            select: [{'model':sqlalchemy model object,'column_name':'',
                    'aggregate_function':'','label':''},...]
                where aggregate_function = string, 'count', 'ave', 'max', 'min'
                where label = string, equivalent to "AS"
            where: [{'model':sqlalchemy model object,'column_name':'','value':'','operator':'','connector':''},...]
                where value = string, float, boolean, etc., or [] or nested 'where':[]
                where operator is of '<', 'LIKE', '=', 'IN' etc.,
                where connector is of 'AND' or 'OR'
            group_by: [{'model':sqlalchemy model object,'column_name':''},...]
            having: [{'model':sqlalchemy model object,'column_name':'','value':'','operator':'','connector':'','aggregate_function':''},...]
                where value = string, float, boolean, etc., or [] or nested 'where':[]
                where operator is of '<', 'LIKE', '=', 'IN' etc.,
                where connector is of 'AND' or 'OR'
                where aggregate_function = string, 'count', 'ave', 'max', 'min'
            order_by: [{'model':sqlalchemy model object,'column_name':'','order':'',},...]
                where order is of 'ASC' or 'DESC'
            limit: integer
            offset: integer
        output_O = 'dictList' = {column:[value,...],...}
                 = 'listDict' = [{column:value,...},...]
                 = 'dictColumn' = {unique_column_value:listDict,...}
                 = 'scalar' = integer
        dictColumn_I = string, name of the column to use in the dictColumn

        OUTPUT:
        rows_O = listDict of queried rows

        DEBUG:
        >>> from sqlalchemy.dialects import postgresql
        >>> query_cmd.compile(dialect=postgresql.dialect())
        >>> print(query_cmd.compile(dialect=postgresql.dialect()))
        '''
        
        try:
            # generate the query clauses
            select_cmd,where_cmd,group_by_cmd,having_cmd,order_by_cmd,limit_cmd,offset_cmd = self.make_querySelectClauses(query_I);
            # make the query statement
            query_cmd = None;
            query_cmd = self.make_queryStatement(select_cmd,where_cmd,group_by_cmd,having_cmd,order_by_cmd,limit_cmd,offset_cmd);
            if verbose_I:
                print(self.convert_sqlalchemyQuery2PostgresqlString(query_cmd));
            data = self.execute_select(query_cmd);
            # extract out the data
            if output_O == 'listDict':
                data_O = self.convert_listKeyedTuple2ListDict(data);
            elif output_O == 'dictList':
                data_O = self.convert_listKeyedTuple2DictList(data);
            elif output_O == 'dictColumn' and dictColumn_I:
                data_O = self.convert_listKeyedTuple2DictColumn(data,dictColumn_I);
            elif output_O == 'scalar':
                data_O = data[0][0];
            else:
                data_O = data;
            return data_O;
        except Exception as e:
            if raise_I: raise;
            else: print(e);

    def make_querySelectClauses(self,query_I):
        '''
        make the query clauses
        INPUT:
        query_I = 
        OUTPUT:
        select_cmd = 
        where_cmd = 
        group_by_cmd = 
        having_cmd = 
        order_by_cmd = 
        limit_cmd = 
        offset_cmd = 

        '''
        select_cmd = None;
        where_cmd = text('1=1'); #always true
        group_by_cmd = None;
        having_cmd = None;
        order_by_cmd = None;
        limit_cmd = None;
        offset_cmd = None;
        if 'select' in query_I.keys():
            select_cmd = self.make_select(query_I['select']);
        if 'where' in query_I.keys():
            where_cmd = self.make_where(query_I['where']);
        if 'group_by' in query_I.keys():
            group_by_cmd = self.make_groupBy(query_I['group_by']);
        if 'having' in query_I.keys():
            having_cmd = self.make_where(query_I['having']);
        if 'order_by' in query_I.keys():
            order_by_cmd = self.make_orderBy(query_I['order_by']);
        if 'limit' in query_I.keys():
            limit_cmd = self.make_limit(query_I['limit']);
        if 'offset' in query_I.keys():
            offset_cmd = self.make_limit(query_I['offset']);
        return select_cmd,where_cmd,group_by_cmd,having_cmd,order_by_cmd,limit_cmd,offset_cmd;

    def convert_querySelectClauses2str(self,select_cmd,where_cmd,group_by_cmd,having_cmd,order_by_cmd,limit_cmd,offset_cmd):
        '''
        convert the query clauses from SQLalchemy Text to strings
        INPUT:select_cmd,where_cmd,group_by_cmd,having_cmd,order_by_cmd,limit_cmd,offset_cmd
        OUTPUT:select_str,from_str,where_str,group_by_str,having_str,order_by_str,limit_str,offset_str
        '''
        if select_cmd is None:
            select_str = '';
            from_str = '';
        else:
            select_str = '';
            from_str = '';
            for model in select_cmd:
                table_name = self.get_tableName_sqlalchemyModel(model);
                from_str += '"' + table_name + '"';
                from_str += ", ";
                columns = self.get_columns_sqlalchemyModel(model);
                for column_name in columns:
                    select_str += ('"%s"."%s"' %(table_name,column_name));
                    select_str += ", ";
            from_str = from_str[:-2];
            select_str = select_str[:-2];
        if where_cmd is None:
            where_str = '';
        else:
            where_str = str(where_cmd);
        if group_by_cmd is None:
            group_by_str = '';
        else:
            group_by_str = str(group_by_cmd);
        if having_cmd is None:
            having_str = '';
        else:
            having_str = str(having_cmd);
        if order_by_cmd is None:
            order_by_str = '';
        else:
            order_by_str = str(order_by_cmd);
        if limit_cmd is None:
            limit_str = '';
        else:
            limit_str = str(limit_cmd);
        if offset_cmd is None:
            offset_str = '';
        else:
            offset_str = str(offset_cmd);
        return select_str,from_str,where_str,group_by_str,having_str,order_by_str,limit_str,offset_str;

    def make_queryStatement(self,select_cmd,where_cmd,group_by_cmd,having_cmd,order_by_cmd,limit_cmd,offset_cmd):
        '''
        make the query statement command
        INPUT:
        select_cmd
        where_cmd

        OPTIONAL INPUT:
        group_by_cmd
        having_cmd
        order_by_cmd
        limit_cmd
        offset_cmd

        OUTPUT:
        query_cmd = sqlalchemy query statement
        '''
        query_cmd = None;
        # add in the select and where statements
        if select_cmd is None or where_cmd is None:
            print('select and where must be provided.');
        query_cmd = select(select_cmd).where(where_cmd);
        # add in all other statements in order
        if not group_by_cmd is None:
            query_cmd = query_cmd.group_by(group_by_cmd);
        if not having_cmd is None:
            query_cmd = query_cmd.having(having_cmd);
        if not order_by_cmd is None:
            query_cmd = query_cmd.order_by(order_by_cmd);
        if not limit_cmd is None:
            query_cmd = query_cmd.limit(limit_cmd);
        if not offset_cmd is None:
            query_cmd = query_cmd.offset(offset_cmd);
        return query_cmd;

    def convert_keyedTuple2Dict(self,row_I):
        '''converted keyed tuple to dictionary
        
        TODO: why not use ._asdict()?
        '''
        row_O = {};
        try:
            for column in row_I.__table__.columns:
                row_O[column.name] = getattr(row_I, column.name);
        except Exception as e:
            for key in row_I.keys():
                row_O[key] = getattr(row_I, key);
        except SQLAlchemyError as e:
            print(e);
        return row_O

    def convert_listKeyedTuple2DictColumn(self,rows_I,column_I):
        '''convert a keyed tuple list to a list of dictionaries
        INPUT:
        rows_I = list of keyed tuples
        column_I = name of the column
        OUTPUT:
        rows_O =
        '''
        rows_O = {};
        try:
            if rows_I: 
                for d in rows_I:
                    tmp = self.convert_keyedTuple2Dict(d);
                    if tmp[column_I] in rows_O.keys():
                        rows_O[tmp[column_I]].append(tmp);
                    else:
                        rows_O[tmp[column_I]] = [];
                        rows_O[tmp[column_I]].append(tmp);
            else:
                print("no rows retreived");
        except Exception as e:
            print(e);
        return rows_O;

    def convert_listKeyedTuple2DictList(self,rows_I):
        '''convert a keyed tuple list to a list of dictionaries
        INPUT:
        rows_I = list of keyed tuples
        OUTPUT:
        rows_O =
        '''
        rows_O = {};
        try:
            if rows_I: 
                for d in rows_I:
                    tmp = self.convert_keyedTuple2Dict(d);
                    for key in tmp.keys():
                        #if tmp[key] in rows_O.keys(): BUG?
                        if key in rows_O.keys():
                            rows_O[key].append(tmp[key]);
                        else:
                            rows_O[key] = [];
                            rows_O[key].append(tmp[key]);
            else:
                print("no rows retreived");
        except Exception as e:
            print(e);
        return rows_O;

    def convert_listKeyedTuple2ListDict(self,rows_I):
        '''convert a keyed tuple list to a list of dictionaries
        INPUT:
        rows_I = list of keyed tuples
        OUTPUT:
        rows_O = list of dictionaries
        '''
        rows_O = [];
        try:
            if rows_I: 
                for d in rows_I:
                    tmp = self.convert_keyedTuple2Dict(d);
                    rows_O.append(tmp);
            else:
                print("no rows retreived");
        except Exception as e:
            print(e);
        return rows_O;

    def check_allColumns(self,column_I):
        '''check for a return of all columns'''
        all = False;
        if column_I is None or column_I == '*':
            all = True;
        return all;

    def execute_select(self,query_I,raise_I=False):
        '''execute a raw sql query
        INPUT:
        query_I = string or sqlalchemy text or sqlalchemy select
        raise_I = boolean, raise error
        OUTPUT:
        data_O = keyed tuple sqlalchemy object
        '''
        data_O = None;
        try:
            ans = self.session.execute(query_I);
            data_O = ans.fetchall(); #TODO: export direction to listDict object
        except SQLAlchemyError as e:
            self.session.rollback();
            if raise_I: raise;
            else: print(e);
        return data_O

    def make_select(self,select_I):
        '''make the select list
        INPUT:
        select_I = []
        OUTPUT:
        select_O = [];
        '''
        select_O = [];
        try:
            for row in select_I:
                if 'column_name' in row.keys() and \
                    ('aggregate_function' in row.keys() or 'label' in row.keys()):
                    column = self.get_columnAttribute_sqlalchemyModel(row['model'],row['column_name']);
                    if 'aggregate_function' in row.keys() and 'label' in row.keys():
                        aggregate_function_name = self.check_aggregateFunction(row['aggregate_function']);
                        select_obj = aggregate_function_name(column).label(row['label']);
                    elif 'aggregate_function' in row.keys():
                        aggregate_function_name = self.check_aggregateFunction(row['aggregate_function']);
                        select_obj = aggregate_function_name(column);
                    elif 'label' in row.keys():
                        select_obj = column.label(row['label']);
                    #table_name = self.get_tableName_sqlalchemyModel(row['model']);
                    #column_name = row['column_name']; #need to validate the column_name
                    #if 'aggregate_function' in row.keys():
                    #    aggregate_function_name = self.check_aggregateFunction(row['aggregate_function']);
                    #    select_str = ('%s("%s"."%s")' %(aggregate_function_name,table_name,column_name));
                    #else:
                    #    select_str = ('"%s"."%s")' %(table_name,column_name));
                    #if 'label' in row.keys():
                    #    select_str += (' AS "%s"' %(row['label']));
                    select_O.append(select_obj);
                elif 'column_name' in row.keys():
                    column = self.get_columnAttribute_sqlalchemyModel(row['model'],row['column_name']);
                    select_O.append(column);
                else:
                    select_O.append(row['model']);
        except Exception as e:
            print(e);
        return select_O

    def check_aggregateFunction(self,aggregate_function_I):
        '''
        check the aggregate function
        INPUT:
        aggregate_function_I = string
        OUTPUT:
        aggregate_function_O = string
        '''
        supported_operators = [
            "count", "avg", "min", "max", "sum", "ave"
			];
        sqlalchemy_function_dict = {
            "count":func.count,
            "avg":func.avg,
            "ave":func.ave,
            "min":func.min,
            "max":func.max,
            "sum":func.sum,
			};
        #if aggregate_function_I in supported_aggregate_functions:
        #    aggregate_function_O = aggregate_function_I;
        if aggregate_function_I in sqlalchemy_function_dict.keys():
            aggregate_function_O = sqlalchemy_function_dict[aggregate_function_I];
        else:
            aggregate_function_O = None;
        return aggregate_function_O;

    def check_whereOperator(self,operator_I):
        '''
        check the where comparison operator
        INPUT:
        operator_I = string
        OUTPUT:
        operator_O = string
        '''
        supported_operators = [
            "<", ">", "<=", ">=" , "=", "!=",
            "BETWEEN", "NOT BETWEEN",
            "LIKE", "NOT LIKE",
            "=ANY","!=ANY",
            "ILIKE", "NOT ILIKE",
            "IS", "IS NOT",
            "IS DISTINCT FROM", "IS NOT DISTINCT FROM",
            "IN", "NOT IN",
            "@>","<@","<>","&&"
			];
        if operator_I in supported_operators:
            operator_O = operator_I;
        else:
            operator_O = None;
        return operator_O;

    def check_whereConnector(self,connector_I):
        '''
        check the where connector
        INPUT:
        connector_I = string
        OUTPUT:
        connector_O = string
        '''
        supported_connectors = ["AND","OR"
			];
        if connector_I in supported_connectors:
            connector_O = connector_I;
        else:
            connector_O = None;
        return connector_O;

    def check_whereValue(self,model_I,column_name_I,value_I):
        '''
        check the where value
        INPUT:
        model_I = sqlalchemy model object
        column_name_I = string, name of the column
        value_I = string
        operator_I = string
        OUTPUT:
        value_O = string, in the correct format for the value
        '''
        #TODO: validate the value (DATE, JSON, ARRAY, etc.,)
        columntype = self.get_columnAttributeDataType_sqlalchemyModel(model_I,column_name_I);
        if '::text[]' in str(value_I) or '::int[]' in str(value_I) or\
            '::character varying[]' in str(value_I):
            #array comparator
            value_O = value_I;
        elif 'VARCHAR' in str(columntype) or 'TEXT' in str(columntype):
            #need to add in double string
            value_O = self.convert_string2StringString(value_I);
        else:
            value_O = value_I;
        return value_O;

    def make_where(self,where_I):
        '''make the where clause
        INPUT:
        where_I = {};
        OUTPUT:
        where_O = sqlalchemy text
        '''
        where_str = '';
        where_O = None;
        for row in where_I:
            tablename = self.get_tableName_sqlalchemyModel(row['model']);
            columnname = self.make_tableColumnStr(tablename,row['column_name']);
            #validate the operator
            operator = self.check_whereOperator(row['operator']);
            #validate the value
            value = self.check_whereValue(row['model'],row['column_name'],row['value']);
            #validate the connector
            connector = self.check_whereConnector(row['connector']);
            clause = ("%s %s %s %s " %(columnname,operator,value,connector));
            where_str += clause;
        if where_I[-1]['connector']=='AND':
            where_str = where_str[:-5]; #remove trailing 'AND'
        elif where_I[-1]['connector']=='OR':
            where_str = where_str[:-4]; #remove trailing 'OR'
        where_O = text(where_str);
        return where_O;

    def make_limit(self,limit_I):
        '''make the limit clause
        INPUT:
        limit_I = integer (or float which will be coerced to an integer)
        OUTPUT:
        limit_O = integer
        '''
        where_str = '';
        limit_O = None;
        if type(limit_I)==type(1):
            limit_str = str(limit_I);
        elif type(limit_I)==type(1.0):
            limit_str = str(int(limit_I));
        else:
            print('only int or float types are supported for limit/offset.');
        limit_O = text(limit_str);
        return limit_O;

    def make_tableColumnStr(self,table_I,column_I):
        '''make the table column string for raw SQL expressions'''
        #columnname_O = ('%s.%s' %(table_I,column_I));
        columnname_O = ('"%s".%s' %(table_I,column_I));
        return columnname_O;

    def make_groupBy(self,group_by_I):
        '''make the group_by clause
        INPUT:
        group_by_I = [];
        OUTPUT:
        group_by_O = sqlalchemy test
        '''
        group_by_str = '';
        group_by_O = None;
        for row in group_by_I:
            tablename = self.get_tableName_sqlalchemyModel(row['model']);
            columnname = self.make_tableColumnStr(tablename,row['column_name']);
            clause = ('%s, ' %(columnname));
            group_by_str += clause;
        group_by_str = group_by_str[:-2]; #remove trailing comma
        group_by_O = text(group_by_str);
        return group_by_O;

    def make_orderBy(self,order_by_I):
        '''make the order_by clause
        INPUT:
        order_by_I = [];
        OUTPUT:
        order_by_O = sqlalchemy test
        '''
        order_by_str = '';
        order_by_O = None;
        for row in order_by_I:
            if 'model' in row.keys():
                tablename = self.get_tableName_sqlalchemyModel(row['model']);
                columnname = self.make_tableColumnStr(tablename,row['column_name']);
                #TODO: validate order
                clause = ('%s %s, ' %(columnname,row['order']));
            elif 'label' in row.keys():
                clause = ('%s %s, ' %(row['label'],row['order']));
            order_by_str += clause;
        order_by_str = order_by_str[:-2]; #remove trailing comma
        order_by_O = text(order_by_str);
        return order_by_O;

    def get_values_sqlalchemyModel(self,
        model_I,column_I,order_I,group_I=False):
        '''query all values of column_I from model_I in order_I
        INPUT:
        model_I = sqlalchemy model ojbect
        column_I = column name
        order_I = 'asc' or 'desc'
        group_I = boolean, False = return all values
                           True = return only unique values
        OUTPUT:
        rows_O = values
        '''
        pass
    def make_queryFromString(self,table_model_I,query_I):
        '''replace tablenames with modelobjects in query attribute
        INPUT:
        table_model_I = {table_name:model}
        query_I = {} query dictionary
            select: [{'table_name':sqlalchemy table_name object,'column_name':''},...]
            where: [{'table_name':sqlalchemy table_name object,'column_name':'','value':'','operator':'','connector':''},...]
                where value = string, float, boolean, etc., or [] or nested 'where':[]
                where operator is of '<', 'LIKE', '=', 'IN' etc.,
                where connector is of 'AND' or 'OR'
            group_by: [{'table_name':sqlalchemy table_name object,'column_name':''},...]
            having: [{'table_name':sqlalchemy table_name object,'column_name':'','value':'','operator':'','connector':''},...]
                where value = string, float, boolean, etc., or [] or nested 'where':[]
                where operator is of '<', 'LIKE', '=', 'IN' etc.,
                where connector is of 'AND' or 'OR'
            order_by: [{'table_name':sqlalchemy table_name object,'column_name':'','order':'',},...]
                where order is of 'ASC' or 'DESC'
            limit: integer
            offset: integer
        OUTPUT:
        query_O = {} query dictionary
            select: [{'model':sqlalchemy model object,'column_name':''},...]
            where: [{'model':sqlalchemy model object,'column_name':'','value':'','operator':'','connector':''},...]
                where value = string, float, boolean, etc., or [] or {'model':'column_name'}
                where operator is of '<', 'LIKE', '=', 'IN' etc.,
                where connector is of 'AND' or 'OR'
            group_by: [{'model':sqlalchemy model object,'column_name':''},...]
            having: [{'model':sqlalchemy model object,'column_name':'','value':'','operator':'','connector':''},...]
                where value = string, float, boolean, etc., or []
                where operator is of '<', 'LIKE', '=', 'IN' etc.,
                where connector is of 'AND' or 'OR'
            order_by: [{'model':sqlalchemy model object,'column_name':'','order':'',},...]
                where order is of 'ASC' or 'DESC'
            limit: integer
            offset: integer
        '''
        query_O = {};
        for k,clause in query_I.items():
            query_O[k]=[];
            if k in ['select','where','group_by','having','order_by']:
                for row in clause:
                    if 'table_name' in row.keys():
                        row['model']=table_model_I[row['table_name']];
                        query_O[k].append(row);
                    elif 'label' in row.keys():
                        query_O[k].append(row);
            elif k in ['limit','offset']:
                query_O[k] = clause;
            elif k in ['delete_from']:
                for row in clause:
                    row['model']=table_model_I[row['table_name']];
                    query_O[k].append(row);
                #query_O[k] = table_model_I[clause];

        return query_O;

    def make_whereFromPrimaryKeys(self,model_I,primary_keys_I,data_I):
        '''
        INPUT:
        model_I = sqlalchemy model
        primary_keys_I = list of column primary keys
        data_I = dictionary of column_name:value
        OUTPUT
        where: [{'table_name':sqlalchemy table_name object,'column_name':'','value':'','operator':'','connector':''},...]
            where value = string, float, boolean, etc., or [] or nested 'where':[]
            where operator is of '<', 'LIKE', '=', 'IN' etc.,
            where connector is of 'AND' or 'OR'
        where_O = {column_name:{value:,operator:,}}
                where operator is of '=',
        '''
        where_O = [];
        for k,v in data_I.items():
            if k in primary_keys_I:
                row = {'model':model_I,'column_name':k,'value':v,'operator':'=','connector':'AND'};
                where_O.append(row);
        return where_O;

