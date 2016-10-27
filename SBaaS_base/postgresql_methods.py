from os import system
from .postgresql_orm_base import *
from .postgresql_orm import postgresql_orm

class postgresql_methods(postgresql_orm):

    def create_tablePartition(self,conn,
        table_name_I='',
        partition_id_I='',
        constraint_column_I='analysis_id',
        constraint_clause_I='',
        constraint_comparator_I='=',
        constraint_id_I='01',
        verbose_I=True,
        ):
        '''create a table partition
        
        '''
        #example:
        #CREATE TABLE child_table_name
        #  (LIKE measurement INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
        table_name_partition = '"%s_%s"'%(table_name_I,partition_id_I)
        self.create_table(conn,
                    table_I=table_name_partition,
                    schema_I='public',
                    initialize_pkey_I = False,
                    table_constraints_I = [],
                    table_constraints_options_I = [],
                    like_sourceTable_schema_I = 'public',
                    like_sourceTable_I = table_name_I,
                    like_options_I = 'INCLUDING ALL',            
                    verbose_I = verbose_I,
                    )

        #example:		
        #ALTER TABLE child_table_name ADD CONSTRAINT y2008m02
        #   CHECK ( logdate >= DATE '2008-02-01' AND logdate < DATE '2008-03-01' );   
        partition_constraint = '"%s_%s_%s"'%(table_name_I,partition_id_I,constraint_id_I)
        constraint_clause = '''"%s" %s '%s' '''%(constraint_column_I,
                                constraint_comparator_I,constraint_clause_I)
        self.alter_table_addConstraint(self,conn,
                    constraint_name_I=partition_constraint,
                    constraint_type_I='CHECK',
                    constraint_columns_I=[],
                    constraint_clause_I=constraint_clause,
                    table_I=table_name_partition,
                    schema_I='public',
                    verbose_I = verbose_I,
			        )	

        #Example:
        #ALTER TABLE child_table_name INHERIT measurement;
        action_options = 'public."%s"'%(table_name_I);
        self.alter_table_action(conn,
                    action_I='INHERIT',
                    action_options_I='action_options',
                    tables_I=[table_name_partition],
                    schema_I='public',
                    verbose_I = verbose_I,
			        )	
    def create_tablePartitionTriggerFunction(
        self,conn,
        user_I,
        schema_I='',
        table_name_I='',
        partition_schema_I='',
        partition_table_name_I='',
        list_range_I = 'LIST',
        column_name_I = 'analysis_id',
        constraint_column_I='analysis_id',
        constraint_comparator_I='=',
        constraint_id_I='01',
        verbose_I=True,
        ):
        '''create a table partition

        EXAMPLE:

        
        '''

        #declare variables
        function_body_declare = 'DECLARE \n_tablename text; \n_partitionid int;  \n_result record; \n';
        function_body_begin = 'BEGIN \n';

        #lookup the partition id
        function_body_selectPartitionID='_partitionid := EXECUTE \n';
        function_body_selectPartitionID+="'";
        function_body_selectPartitionID+='SELECT partition_id FROM "%s"."%s" WHERE "%s"."%s".partition_column = NEW."%s"'%(
            partition_schema_I,partition_table_name_I,
            partition_schema_I,partition_table_name_I,
            column_name_I);
        function_body_selectPartitionID+="'; \n";
        function_body_begin+=function_body_selectPartitionID;
        
        function_body_begin+='IF _partitionid IS NULL \n';

        #if the partition id does not exist, make one
        function_body_insertPartitionValue='EXECUTE \n';
        function_body_insertPartitionValue+="'";
        function_body_insertPartitionValue+='INSERT INTO "%s"."%s" (partition_column,partition_value,used_,comment_) \n'%(
            partition_schema_I,partition_table_name_I);
        function_body_insertPartitionValue+="VALUES ('%s',"%(
            constraint_column_I);
        function_body_insertPartitionValue+='NEW."%s",'%(constraint_column_I);
        function_body_insertPartitionValue+="%s,%s)"%('true','null');
        function_body_insertPartitionValue+="'; \n";
        function_body_begin+=function_body_insertPartitionValue;

        #lookup the partition id
        function_body_begin+=function_body_selectPartitionID;    

        #define the new tablename
        function_body_newTable='_tablename := "%s"_||_partitionid; \n'%(
            table_name_I);
        function_body_begin+=function_body_newTable;
        #function_body_newTable='_tablename := "%s"."%s"_||_partitionid;'%(
        #    schema_I,table_name_I);    

        #make a new partition table (if it does not exist)
        create_partitionTable = self.create_table(conn,
                    table_I='_tablename',
                    #table_I=table_name_partition,
                    schema_I=schema_I,
                    initialize_pkey_I = False,
                    table_constraints_I = [],
                    table_constraints_options_I = [],
                    like_sourceTable_schema_I = schema_I,
                    like_sourceTable_I = table_name_I,
                    like_options_I = 'INCLUDING ALL',            
                    verbose_I = verbose_I,
                    execute_I = False,
                    commit_I=False,
                    return_response_I=False,
                    return_cmd_I=True,
                    )
        function_body_begin+='EXECUTE \n';
        function_body_begin+="'";
        function_body_begin+=create_partitionTable; 
        function_body_begin=function_body_begin[:-1];
        function_body_begin+="'; \n";
        
        #create the partition constraints
        partition_constraint = '''%s_||%s||_%s'''%(table_name_I,'_partitionid',constraint_id_I)
        constraint_clause = '''"%s" %s NEW."%s" '''%(constraint_column_I,
                                constraint_comparator_I,constraint_column_I)
        #constraint_clause = '''"%s" %s '%s' '''%(constraint_column_I,
        #                        constraint_comparator_I,constraint_clause_I)
        add_partitionTableConstraints = self.alter_table_addConstraint(conn,
                    constraint_name_I=partition_constraint,
                    constraint_type_I='CHECK',
                    constraint_columns_I=[],
                    constraint_clause_I=constraint_clause,
                    table_I='_tablename',
                    schema_I=schema_I,          
                    verbose_I = verbose_I,
                    execute_I = False,
                    commit_I=False,
                    return_response_I=False,
                    return_cmd_I=True,
                    )
        function_body_begin+=' \n';
        function_body_begin+='EXECUTE \n';
        function_body_begin+="'";
        function_body_begin+=add_partitionTableConstraints; 
        function_body_begin=function_body_begin[:-1];
        function_body_begin+="'; \n";

        #assign inheritance        
        action_options = '"%s"."%s"'%(
            schema_I,table_name_I); 
        add_partitionTableInheritance = self.alter_table_action(conn,
            action_I='INHERIT',
            action_options_I=action_options,
            tables_I=['_tablename'],
            schema_I=schema_I,
            verbose_I = verbose_I,
            execute_I = False,
            commit_I=False,
            return_response_I=False,
            return_cmd_I=True,
            )
        function_body_begin+=' \n';
        function_body_begin+='EXECUTE \n';
        function_body_begin+="'";
        function_body_begin+=add_partitionTableInheritance[0];
        function_body_begin=function_body_begin[:-1];
        function_body_begin+="'; \n";

        #assign privileges (if they do not exist) 
        add_partitionTableOwner = self.alter_table_action(conn,
            action_I='OWNER TO',
            action_options_I=user_I,
            tables_I=['_tablename'],
            schema_I=schema_I,
            verbose_I = verbose_I,
            execute_I = False,
            commit_I=False,
            return_response_I=False,
            return_cmd_I=True,
            )
        function_body_begin+=' \n';
        function_body_begin+='EXECUTE \n';
        function_body_begin+="'";
        function_body_begin+=add_partitionTableOwner[0]; 
        function_body_begin=function_body_begin[:-1];
        function_body_begin+="'; \n";
        add_partitionTablePrivileges = self.grant_privileges(
            conn,user_I=user_I,
            privileges_I=['ALL'],
            tables_I=['_tablename'],
            schema_I=schema_I,
            verbose_I = verbose_I,
            execute_I = False,
            commit_I=False,
            return_response_I=False,
            return_cmd_I=True,
            )
        function_body_begin+=' \n';
        function_body_begin+='EXECUTE \n';
        function_body_begin+="'";
        function_body_begin+=add_partitionTablePrivileges;  
        function_body_begin=function_body_begin[:-1];
        function_body_begin+="'; \n";

        function_body_begin+='END IF; \n';

        #define the new tablename
        function_body_newTable='_tablename := "%s"_||_partitionid; \n'%(
            table_name_I);
        function_body_begin+=function_body_newTable;

        #insert data into the partition table  		
        function_body_insert = "EXECUTE 'INSERT INTO ";
        function_body_insert += '"%s". ' %(schema_I);
        function_body_insert += "|| quote_ident(_tablename) || ' VALUES ($1.*)' USING NEW; \n"	
        function_body_begin+=function_body_insert; 

        function_body_begin+='RETURN NULL; \nEND; \n'

        function = function_body_declare+function_body_begin;
        self.create_function(conn,
                    function_I=function,
                    argmode_I='',
                    argname_I='',
                    argtype_I='',
                    default_expr_I='',
                    returns_rettype_I ='TRIGGER',
                    returns_table_I ='',
                    returns_table_column_name_I =[],
                    returns_table_column_type_I =[],
                    language_I = 'plpgsql',
                    as_I = as_I,
                    with_attributes_I=[],
                    verbose_I = verbose_I,
                    )	

    def create_tablePartitionTrigger(
        self,conn,
        table_name_I='',
        table_name_partitions_I = [],
        verbose_I=True
        ):
        #example:			
        #CREATE OR REPLACE FUNCTION measurement_insert_trigger()
        #RETURNS TRIGGER AS $$
        #BEGIN
        #    INSERT INTO measurement_y2008m01 VALUES (NEW.*);
        #    RETURN NULL;
        #END;
        #$$
        #LANGUAGE plpgsql;
        cmd_I = "INSERT"
        function = '"%s_%s_triggerFunction"'%(table_name_I,cmd_I)	
        lines = [];
        for table_name_partition in table_name_partitions_I:
            line = 'INSERT INTO "%s" VALUES (NEW.*)'%table_name_partition;
            lines.append(line);
        lines.append('RETURN NULL');
        as_I = self.make_definition(
            definition_I = self.make_script(
                lines)
            );
        self.create_function(conn,
                    function_I=function,
                    argmode_I='',
                    argname_I='',
                    argtype_I='',
                    default_expr_I='',
                    returns_rettype_I ='TRIGGER',
                    returns_table_I ='',
                    returns_table_column_name_I =[],
                    returns_table_column_type_I =[],
                    language_I = 'plpgsql',
                    as_I = as_I,
                    with_attributes_I=[],
                    verbose_I = verbose_I,
                    )	

        #Example:
        #CREATE TRIGGER insert_measurement_trigger
        #    BEFORE INSERT ON measurement
        #    FOR EACH ROW EXECUTE PROCEDURE measurement_insert_trigger();	
        trigger = '"%s_%s_trigger"'%(table_name_I,cmd_I)
        self.create_trigger(self,conn,
                    trigger_I=trigger,
                    constraint_I='',
                    before_after_insteadOf_I='BEFORE',
                    event_I ='',
                    referenced_table_schema_I='public',
                    referenced_table_name_I=table_name_I,
                    deferrable_clause_I ='',
                    for_clause_I ='FOR EACH ROW',
                    when_conditions_I = [],
                    function_name_I = '',
                    function_arguments_I = '',
                    verbose_I = verbose_I,
                    )

    def drop_tablePartitionTrigger(self,conn,
        table_name_I='',
        verbose_I=True,
        ):
        
        cmd_I = "INSERT"
        function = '"%s_%s_triggerFunction"'%(table_name_I,cmd_I)	
        self.drop_function(conn,
            function_I=function,
            argmode_I='',
            argname_I='',
            argtype_I='',
            cascade_restrict_I='',
            verbose_I = verbose_I,
            )	
        		
        trigger = '"%s_%s_trigger"'%(table_name_I,cmd_I)
        self.drop_trigger(conn,
            trigger_I=trigger,
            referenced_table_schema_I='public',
            referenced_table_name_I=table_name_I,
            cascade_restrict_I='',
            verbose_I = verbose_I,
            )