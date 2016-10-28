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
        partition_lookup_schema_I='',
        partition_lookup_table_name_I='',
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
        function_body_declare = 'DECLARE \n_tablename text; \n_partitionid int;  \n_partitionidstr text;  \n_result record; \n';
        function_body_begin = 'BEGIN \n';

        #lookup the partition id
        function_body_selectPartitionID='_partitionid := EXECUTE \n';
        function_body_selectPartitionID+="'";
        function_body_selectPartitionID+='SELECT partition_id FROM "%s"."%s" WHERE "%s"."%s"."%s" = NEW."%s"'%(
            partition_lookup_schema_I,partition_lookup_table_name_I,
            partition_lookup_schema_I,partition_lookup_table_name_I,
            constraint_column_I,column_name_I);
        function_body_selectPartitionID+="'; \n";
        function_body_begin+=function_body_selectPartitionID;
        
        function_body_begin+='IF _partitionid IS NULL \n';

        #if the partition id does not exist, make one
        function_body_insertPartitionValue='EXECUTE \n quote_literal(';
        function_body_insertPartitionValue+='INSERT INTO "%s"."%s" (partition_column,partition_value,used_,comment_) \n'%(
            partition_lookup_schema_I,partition_lookup_table_name_I);
        function_body_insertPartitionValue+="VALUES ('%s',"%(
            constraint_column_I);
        function_body_insertPartitionValue+='NEW."%s",'%(constraint_column_I);
        function_body_insertPartitionValue+="%s,%s)"%('true','null');
        function_body_insertPartitionValue+="); \n";
        function_body_begin+=function_body_insertPartitionValue;

        #lookup the partition id
        function_body_begin+=function_body_selectPartitionID;    

        #create the partition constraints
        ##TODO make function to calculate unique partition id based on schema,table,user,partition_column,partition_value
        add_partitionIDString = ''' _partitionidstr := to_char(_partitionid, '999'); \n'''
        function_body_begin+=add_partitionIDString;

        #define the new tablename
        function_body_newTable=''' _tablename := quote_ident(%s)||'_'||_partitionidstr; \n '''%(
            table_name_I);
        function_body_begin+=function_body_newTable;   

        #make a new partition table (if it does not exist)
        create_partitionTable = '''CREATE TABLE IF EXISTS '"%s".'||quote_ident(_tablename) (LIKE "%s"."%s" INCLUDING ALL )
WITH (OIDS=FALSE);''' %(partition_schema_I,schema_I,table_name_I)
        #create_partitionTable = self.create_table(conn,
        #            table_I='_tablename',
        #            #table_I=table_name_partition,
        #            schema_I=partition_schema_I,
        #            initialize_pkey_I = False,
        #            table_constraints_I = [],
        #            table_constraints_options_I = [],
        #            like_sourceTable_schema_I = schema_I,
        #            like_sourceTable_I = table_name_I,
        #            like_options_I = 'INCLUDING ALL',            
        #            verbose_I = verbose_I,
        #            execute_I = False,
        #            commit_I=False,
        #            return_response_I=False,
        #            return_cmd_I=True,
        #            )
        function_body_begin+='EXECUTE \n quote_literal(';
        function_body_begin+=create_partitionTable; 
        function_body_begin=function_body_begin[:-1];
        function_body_begin+="); \n";
        
        #create the partition constraints
        add_partitionTableConstraints = '''ALTER TABLE IF EXISTS '"%s".'||quote_ident(_tablename) ADD CONSTRAINT quote_ident(_tablename)||'_check%s' CHECK  ("%s" %s NEW."%s");'''%(
            partition_schema_I,constraint_id_I, constraint_column_I,constraint_comparator_I,constraint_column_I);
        #partition_constraint = ''' quote_ident(_tablename)||'_'||_partitionid||'_%s' '''%(table_name_I,'_partitionid',constraint_id_I)
        #constraint_clause = '''"%s" %s NEW."%s" '''%(constraint_column_I,
        #                        constraint_comparator_I,constraint_column_I)
        #add_partitionTableConstraints = self.alter_table_addConstraint(conn,
        #            constraint_name_I=partition_constraint,
        #            constraint_type_I='CHECK',
        #            constraint_columns_I=[],
        #            constraint_clause_I=constraint_clause,
        #            table_I='_tablename',
        #            schema_I=partition_schema_I,          
        #            verbose_I = verbose_I,
        #            execute_I = False,
        #            commit_I=False,
        #            return_response_I=False,
        #            return_cmd_I=True,
        #            )
        function_body_begin+=' \n';
        function_body_begin+='EXECUTE \n quote_literal(';
        function_body_begin+=add_partitionTableConstraints; 
        function_body_begin=function_body_begin[:-1];
        function_body_begin+="); \n";

        #assign inheritance        
        action_options = '"%s"."%s"'%(
            schema_I,table_name_I); 
        add_partitionTableInheritance = '''ALTER TABLE IF EXISTS '"%s".'||quote_ident(_tablename) INHERIT %s;'''%(
            partition_schema_I,action_options)
        #add_partitionTableInheritance = self.alter_table_action(conn,
        #    action_I='INHERIT',
        #    action_options_I=action_options,
        #    tables_I=['_tablename'],
        #    schema_I=partition_schema_I,
        #    verbose_I = verbose_I,
        #    execute_I = False,
        #    commit_I=False,
        #    return_response_I=False,
        #    return_cmd_I=True,
        #    )
        function_body_begin+=' \n';
        function_body_begin+='EXECUTE \n quote_literal(';
        #function_body_begin+=add_partitionTableInheritance[0];
        function_body_begin+=add_partitionTableInheritance;
        function_body_begin=function_body_begin[:-1];
        function_body_begin+="); \n";

        #assign privileges (if they do not exist) 
        add_partitionTableOwner = '''ALTER TABLE IF EXISTS '"%s".'||quote_ident(_tablename) OWNER TO "%s";'''%(
            partition_schema_I,user_I)
        #add_partitionTableOwner = self.alter_table_action(conn,
        #    action_I='OWNER TO',
        #    action_options_I=user_I,
        #    tables_I=['_tablename'],
        #    schema_I=schema_I,
        #    verbose_I = verbose_I,
        #    execute_I = False,
        #    commit_I=False,
        #    return_response_I=False,
        #    return_cmd_I=True,
        #    )
        function_body_begin+=' \n';
        function_body_begin+='EXECUTE \n quote_literal(';
        function_body_begin+=add_partitionTableOwner; 
        #function_body_begin+=add_partitionTableOwner[0]; 
        function_body_begin=function_body_begin[:-1];
        function_body_begin+="); \n";
        add_partitionTablePrivileges = '''GRANT ALL ON '"%s".'||quote_ident(_tablename) TO "%s" '''%(
            partition_schema_I,user_I)
        #add_partitionTablePrivileges = self.grant_privileges(
        #    conn,user_I=user_I,
        #    privileges_I=['ALL'],
        #    tables_I=['_tablename'],
        #    schema_I=partition_schema_I,
        #    verbose_I = verbose_I,
        #    execute_I = False,
        #    commit_I=False,
        #    return_response_I=False,
        #    return_cmd_I=True,
        #    )
        function_body_begin+=' \n';
        function_body_begin+='EXECUTE \n quote_literal(';
        function_body_begin+=add_partitionTablePrivileges;  
        function_body_begin=function_body_begin[:-1];
        function_body_begin+="); \n";

        function_body_begin+='END IF; \n';

        #define the new tablename
        function_body_begin+=function_body_newTable;

        #insert data into the partition table  		
        function_body_insert = '''EXECUTE 'INSERT INTO "%s".'|| quote_ident(_tablename) || ' VALUES ($1.*)' USING NEW; \n'''%(
            partition_schema_I);
        function_body_begin+=function_body_insert; 

        function_body_begin+='RETURN NULL; \nEND; \n'

        function = "$BODY% \n"+function_body_declare+function_body_begin+"$BODY% \n";
        function_name = '%s_partitionTrigFunc'%(table_name_I)
        #function_script = '''CREATE OR REPLACE FUNCTION "%s"."%s" () \nRETURNS TRIGGER \n%s plpgsql;'''%(
        #    schema_I,function_name,function_body_begin);
        function_script = self.create_function(conn,
            schema_I=schema_I,
            function_I=function_name,
            argmode_I='',
            argname_I='',
            argtype_I='',
            default_expr_I='',
            returns_rettype_I ='TRIGGER',
            returns_table_I ='',
            returns_table_column_name_I =[],
            returns_table_column_type_I =[],
            language_I = 'plpgsql',
            as_I = function,
            with_attributes_I=[],
            verbose_I = verbose_I,
            execute_I = True,
            commit_I=True,
            return_response_I=False,
            return_cmd_I=False,
            #execute_I = False,
            #commit_I=False,
            #return_response_I=False,
            #return_cmd_I=True,
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


    def create_databaseShardFunction(self,
        schema_I,
        function_name_I,
        ):
        '''
        Create a database sharding function using postgresql schemas
        INPUT:
        schema_I
        function_name_I
        
        BASED ON:
        sharding based on tablename:
        https://www.tumblr.com/login_required/instagram-engineering/10853187575
        http://rob.conery.io/2014/05/29/a-better-id-generator-for-postgresql/

        '''
        #create schema shard_1;
        #create sequence shard_1.global_id_sequence;

        '''CREATE OR REPLACE FUNCTION "%s"."%s"(OUT result bigint) AS $$
        DECLARE
            our_epoch bigint := 1314220021721;
            seq_id bigint;
            now_millis bigint;
            -- the id of this DB shard, must be set for each
            -- schema shard you have - you could pass this as a parameter too
            shard_id int := 1;
        BEGIN
            SELECT nextval('shard_1.global_id_sequence') % 1024 INTO seq_id;

            SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) INTO now_millis;
            result := (now_millis - our_epoch) << 23;
            result := result | (shard_id << 10);
            result := result | (seq_id);
        END;
        $$ LANGUAGE PLPGSQL;'''%(
            schema_I,function_name_I)

        #select shard_1.id_generator();
        pass;