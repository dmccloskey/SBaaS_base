from os import system
from .postgresql_orm_base import *
from .postgresql_orm import postgresql_orm
from .postgresql_orm import execute_query

class postgresql_methods(postgresql_orm):

    def make_tablePartitionTriggerFunctionName(
        self,
        table_name_I
        ):
        '''
        Return the function name for the partition trigger function
        INPUT:
        table_name_I = string
        OUTPUT:
        function_name_O = string
        '''
        function_name_O = '%s_partitionTrigFunc'%(table_name_I);
        return function_name_O;
    def make_tablePartitionTriggerName(
        self,
        table_name_I
        ):
        '''
        Return the name for the partition trigger
        INPUT:
        table_name_I = string
        OUTPUT:
        trigger_name_O = string
        '''
        trigger_name_O = '%s_partTrig'%(table_name_I);
        return trigger_name_O;
    def make_tablePartitionSequenceName(
        self,
        table_name_I
        ):
        '''
        Return the name for the partition sequencing
        INPUT:
        table_name_I = string
        OUTPUT:
        sequence_name_O = string
        '''
        sequence_name_O = '%s_partSeq'%(table_name_I);
        return sequence_name_O;
    def make_tablePartitionSequenceGeneratorFunctionName(
        self,
        table_name_I
        ):
        '''
        Return the name for the partition sequencing generator function
        INPUT:
        table_name_I = string
        OUTPUT:
        function_name_O = string
        '''
        function_name_O = '%s_partSeqGenFunc'%(table_name_I);
        return function_name_O;
    def make_tableColumnSequenceName(
        self,
        column_name_I,
        table_name_I
        ):
        '''
        Return the name for the partition sequencing generator function
        INPUT:
        table_name_I = string
        OUTPUT:
        sequence_name_O = string
        '''
        sequence_name_O = '%s_%s_seq'%(table_name_I,column_name_I);
        return sequence_name_O;

    def drop_tableMasterAndPartitions(self,conn,
        table_I='',
        schema_I='',
        like_sourceTable_schema_I = '',
        like_sourceTable_I = '',
        like_options_I = '',
        verbose_I=True,
        ):
        '''create a master table and child partitions
        
        '''
        
        '''CREATE OR REPLACE FUNCTION footgun(IN _schema TEXT, IN _parttionbase TEXT) 
        RETURNS void 
        LANGUAGE plpgsql
        AS
        $$
        DECLARE
            row     record;
        BEGIN
            FOR row IN 
                SELECT
                    table_schema,
                    table_name
                FROM
                    information_schema.tables
                WHERE
                    table_type = 'BASE TABLE'
                AND
                    table_schema = _schema
                AND
                    table_name ILIKE (_parttionbase || '%')
            LOOP
                EXECUTE 'DROP TABLE ' || quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
                RAISE INFO 'Dropped table: %', quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
            END LOOP;
        END;
        $$;

        SELECT footgun('public', 'tablename');
        '''
        pass;

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
        verbose_I=True,
        ):
        '''create a table partition trigger function
        1. Makes a unique partition_id
        2. Creates the partition table
            creates the check constraint
            creates the inheritance link
            assigns table ownership
            grants user privileges
        3. Inserts rows into the partition table
        INPUT:
        conn = connection or session,
        user_I = string,
        schema_I=string, schema of the master table
        table_name_I=string, master table name
        partition_schema_I=string, schema of the partition table
        partition_lookup_schema_I=string, chema of the partition id lookup table
        partition_lookup_table_name_I=string, name of the parition id lookup table
        list_range_I = 'LIST',
        column_name_I = string, 
        constraint_column_I=string,
        constraint_comparator_I=string, e.g. '='
        constraint_id_I=string,

        EXAMPLE:

        
        '''

        #declare variables
        function_body_declare = 'DECLARE \n_tablename text; \n_partitionid int;  \n_partitionidstr text; \n_partitioncolstr text;  \n_partitionlookupid  int;  \n_partitionlookupstr text;  \n_result record; \n';
        function_body_begin = 'BEGIN \n';
        
        #convert the partition id to a string
        add_partitionColString = '''_partitioncolstr := '%s'; \n'''%(column_name_I)
        function_body_begin+=add_partitionColString;

        #lookup the partition id
        function_body_selectPartitionID='''SELECT partition_id FROM "%s"."%s" WHERE "%s"."%s"."partition_column" = '%s' AND "%s"."%s"."partition_value" = NEW."%s" INTO _partitionid; \n '''%(
            partition_lookup_schema_I,partition_lookup_table_name_I,
            partition_lookup_schema_I,partition_lookup_table_name_I,column_name_I,
            partition_lookup_schema_I,partition_lookup_table_name_I,
            constraint_column_I);
        function_body_begin+=function_body_selectPartitionID;
        
        function_body_begin+='IF _partitionid IS NULL THEN \n';

        #if the partition id does not exist, make one
        sequence_generator_name = self.make_tablePartitionSequenceGeneratorFunctionName(partition_lookup_table_name_I);
        function_body_selectPartitionID='''SELECT "%s"."%s"() INTO _partitionid;  \n '''%(
            partition_lookup_schema_I,sequence_generator_name);
        function_body_begin+=function_body_selectPartitionID;

        #convert the partition id to a string
        add_partitionIDString = ''' _partitionidstr := trim(both ' ' from to_char(_partitionid, '999999999999')); \n'''
        function_body_begin+=add_partitionIDString;

        ####
        lookup_sequence_name = self.make_tableColumnSequenceName('id',partition_lookup_table_name_I);
        function_body_selectPartitionLookupID='''SELECT nextval('"%s"."%s"') INTO _partitionlookupid;  \n '''%(
            partition_lookup_schema_I,lookup_sequence_name);
        function_body_begin+=function_body_selectPartitionLookupID;
        add_partitionLookupIDString = ''' _partitionlookupstr := trim(both ' ' from to_char(_partitionlookupid, '999999999999')); \n'''
        function_body_begin+=add_partitionLookupIDString;

        #insert the new partition id into the partition table  VALUES ($1.*)' USING NEW;
        function_body_insertPartitionValue='''EXECUTE \n 'INSERT INTO "%s"."%s" (id,partition_column,partition_value,partition_id,used_,comment_) \n '''%(
            partition_lookup_schema_I,partition_lookup_table_name_I);
        #function_body_insertPartitionValue+='''VALUES (' ||quote_literal(%s) || ',NEW."%s",_partitionidstr,true,null)'; \n '''%(
        #    column_name_I,constraint_column_I);
        function_body_insertPartitionValue+='''VALUES (' || _partitionlookupstr || ',' ||quote_literal(_partitioncolstr) || ',$1."%s",' || _partitionidstr || ',true,null)' USING NEW; \n '''%(
            constraint_column_I);
        function_body_begin+=function_body_insertPartitionValue;

        ##lookup the partition id
        #function_body_begin+=function_body_selectPartitionID;    

        #define the new tablename
        function_body_newTable=''' _tablename := '%s_'||_partitionidstr; \n '''%(
            table_name_I);
        function_body_begin+=function_body_newTable;   

        #make a new partition table (if it does not exist)
        create_partitionTable = '''CREATE TABLE IF NOT EXISTS "%s".'||quote_ident(_tablename)||' (LIKE "%s"."%s" INCLUDING ALL) WITH (OIDS=FALSE) ''' %(
            partition_schema_I,schema_I,table_name_I)
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
        function_body_begin+="EXECUTE \n '";
        function_body_begin+=create_partitionTable; 
        function_body_begin=function_body_begin[:-1];
        function_body_begin+="'; \n";
        
        #create the partition constraints
        add_partitionTableConstraints = '''EXECUTE \n 'ALTER TABLE IF EXISTS "%s".'||quote_ident(_tablename)||' ADD CONSTRAINT '|| quote_ident(_tablename||'_partCheck') ||' CHECK  ("%s" %s $1."%s")' USING NEW; \n '''%(
            partition_schema_I, constraint_column_I,constraint_comparator_I,constraint_column_I);
        function_body_begin+=add_partitionTableConstraints; 
        function_body_begin=function_body_begin[:-1];

        #assign inheritance        
        action_options = '"%s"."%s"'%(
            schema_I,table_name_I); 
        add_partitionTableInheritance = '''ALTER TABLE IF EXISTS "%s".'||quote_ident(_tablename)||' INHERIT %s;'''%(
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
        #function_body_begin+=' \n';
        function_body_begin+="EXECUTE \n '";
        #function_body_begin+=add_partitionTableInheritance[0];
        function_body_begin+=add_partitionTableInheritance;
        function_body_begin=function_body_begin[:-1];
        function_body_begin+="'; \n";

        #assign privileges (if they do not exist) 
        add_partitionTableOwner = '''ALTER TABLE IF EXISTS "%s".'||quote_ident(_tablename)||' OWNER TO "%s" '''%(
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
        #function_body_begin+=' \n';
        function_body_begin+="EXECUTE \n '";
        function_body_begin+=add_partitionTableOwner; 
        #function_body_begin+=add_partitionTableOwner[0]; 
        function_body_begin=function_body_begin[:-1];
        function_body_begin+="'; \n";
        add_partitionTablePrivileges = '''GRANT ALL ON "%s".'||quote_ident(_tablename)||' TO "%s" '''%(
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
        #function_body_begin+=' \n';
        function_body_begin+="EXECUTE \n '";
        function_body_begin+=add_partitionTablePrivileges;  
        function_body_begin=function_body_begin[:-1];
        function_body_begin+="'; \n";

        function_body_begin+='END IF; \n';        

        #convert the partition id to a string
        function_body_begin+=add_partitionIDString;

        #define the new tablename
        function_body_begin+=function_body_newTable;

        #insert data into the partition table  		
        function_body_insert = '''EXECUTE 'INSERT INTO "%s".'|| quote_ident(_tablename) || ' VALUES ($1.*)' USING NEW; \n'''%(
            partition_schema_I);
        function_body_begin+=function_body_insert; 

        function_body_begin+='RETURN NULL; \nEND; \n'

        function = "$BODY$ \n"+function_body_declare+function_body_begin+"$BODY$ \n";
        function_name = self.make_tablePartitionTriggerFunctionName(table_name_I)
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
        schema_I='',
        table_name_I='',
        verbose_I=True
        ):
        '''
        Create a table partition trigger
        INPUT:
        conn = connection or session
        schema_I = string
        table_name_I = string
        '''

        #Example:
        #CREATE TRIGGER insert_measurement_trigger
        #    BEFORE INSERT ON measurement
        #    FOR EACH ROW EXECUTE PROCEDURE measurement_insert_trigger();	
        function_name = self.make_tablePartitionTriggerFunctionName(table_name_I)
        trigger = self.make_tablePartitionTriggerName(table_name_I)
        self.create_trigger(conn,
            trigger_I=trigger,
            constraint_I='',
            before_after_insteadOf_I='BEFORE',
            event_I ='INSERT',
            schema_I='public',
            table_name_I = table_name_I,
            referenced_table_schema_I='',
            referenced_table_name_I='',
            deferrable_clause_I ='',
            for_clause_I ='FOR EACH ROW',
            when_conditions_I = [],
            function_name_I = function_name,
            function_arguments_I = '',
            verbose_I = verbose_I,
            )
    def drop_tablePartitionTriggerFunction(self,conn,
        schema_I='public',
        table_name_I='',
        verbose_I=True,
        ):
        
        function_name = self.make_tablePartitionTriggerFunctionName(table_name_I)	
        self.drop_function(conn,
            schema_I=schema_I,
            function_I=function_name,
            argmode_I='',
            argname_I='',
            argtype_I='',
            cascade_restrict_I='',
            verbose_I = verbose_I,
            )	
    def drop_tablePartitionTrigger(self,conn,
        schema_I='public',
        table_name_I='',
        verbose_I=True,
        ):
        		
        trigger = self.make_tablePartitionTriggerName(table_name_I)
        self.drop_trigger(conn,
            trigger_I=trigger,
            referenced_table_schema_I='public',
            referenced_table_name_I=table_name_I,
            cascade_restrict_I='',
            verbose_I = verbose_I,
            )

    def create_databaseShardSequenceGenerator(self,
        schema_I,
        function_name_I,
        ):
        '''
        Create a database sharding sequence generating function using postgresql schemas
        INPUT:
        schema_I
        function_name_I
        
        BASED ON:
        sharding based on schema:
        https://www.tumblr.com/login_required/instagram-engineering/10853187575
        http://rob.conery.io/2014/05/29/a-better-id-generator-for-postgresql/

        TODO...
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
    def create_tablePartitionSequenceGenerator(
        self,
        conn,
        schema_I,
        table_name_I,
        verbose_I=True
        ):
        '''
        Create a table partitioning sequencing generating function
        INPUT:
        schema_I
        function_name_I
        
        BASED ON:
        partitioning based on tablename:
        
        TODO:
        ensure unique partition_ids

        '''

        #create sequence
        sequence_name = self.make_tablePartitionSequenceName(table_name_I);
        self.create_sequence(conn,
            schema_I=schema_I,
            sequence_I=sequence_name,
            verbose_I = verbose_I,
            execute_I = True,
            commit_I=True,
            return_response_I=False,
            return_cmd_I=False,)

        #create the sequence generator function
        function_name = self.make_tablePartitionSequenceGeneratorFunctionName(table_name_I);
        sequence_generator = '''CREATE OR REPLACE FUNCTION "%s"."%s"(OUT result bigint) AS $$
        DECLARE
            our_epoch bigint := 1314220021721;
            seq_id bigint;
        BEGIN
            SELECT nextval('"%s"."%s"') INTO seq_id;
            result := seq_id;
        END;
        $$ LANGUAGE PLPGSQL;'''%(
            schema_I,function_name,schema_I,sequence_name)
        execute_query(conn,sequence_generator,
            verbose_I = verbose_I,
            execute_I = True,
            commit_I=True,
            return_response_I=False,
            return_cmd_I=False,);

    def drop_tablePartitionSequenceGenerator(
        self,
        conn,
        schema_I,
        table_name_I,
        verbose_I=True
        ):
        '''
        drop a table partitioning sequencing generating function
        INPUT:
        schema_I
        function_name_I

        '''

        #drop sequence
        sequence_name = self.make_tablePartitionSequenceName(table_name_I);
        self.drop_sequence(conn,
            schema_I=schema_I,
            sequence_I=sequence_name,
            verbose_I = verbose_I,
            );
        #drop the function
        function_name = self.make_tablePartitionSequenceGeneratorFunctionName(table_name_I);	
        self.drop_function(conn,
            schema_I=schema_I,
            function_I=function_name,
            argmode_I='',
            argname_I='',
            argtype_I='',
            cascade_restrict_I='',
            verbose_I = verbose_I,
            );
