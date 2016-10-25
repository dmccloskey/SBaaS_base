from os import system
from .postgresql_orm_base import *
from .postgresql_orm import postgresql_orm

class postgres_methods(postgresql_orm):

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