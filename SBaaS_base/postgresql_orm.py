from os import system
from .postgresql_orm_base import *
from .postgresql_utilities import _Session

def return_postgresqlQueryFromSQLAlchemyQuery(query_I):
    '''return a string representation of the actual postgresql query
    generated by SQLAlchemy
    INPUT:
    query_I = SQLAlchemy query object
    OUTPUT;
    query_O = string query
    '''
    query_O = str(query_I.statement.compile(dialect=postgresql.dialect()));
    #print(str(query_I.statement.compile(dialect=postgresql.dialect())));
    return query_O;

class postgresql_orm():
    def __init__(self):
        self.engine = None;
        self.Session = None;

    def make_engineFromSettings(self,settings_I={}):
        '''make the database engine
        INPUT:
        settings_I = string dict, {settings name:settings value}
        '''
        engine = create_engine("postgresql://%s:%s@%s/%s" %
            (settings_I['user'], settings_I['password'], settings_I['host'], settings_I['database']))
        self.engine = engine;
        return engine;

    def make_engine(self,database_I='sbaas01',user_I='guest',password_I='guest',host_I="localhost:5432"):
        '''make the database engine'''
        engine = create_engine("postgresql://%s:%s@%s/%s" %
            (user_I, password_I, host_I, database_I))
        self.engine = engine;
        return engine;

    def make_defaultEngine(self):
        '''return default database engine'''
        engine = create_engine("postgres://postgres@/postgres");
        self.engine = engine;
        return engine;

    def make_connectionFromSettings(self,
            settings_I={}):
        '''return connection to the database
        INPUT (option1):
        settings_I
        '''
        try:
            engine = self.make_engineFromSettings(settings_I);
            conn = engine.connect();
            conn.execute("commit");
            return conn;
        except SQLAlchemyError as e:
            print(e);
            exit(-1);

    def make_connection(self,
            database_I='postgres',user_I='postgres',password_I='postgres',host_I="localhost:5432"):
        '''return connection to the database
        INPUT (option2):
        database_I
        user_I
        password_I
        host_I
        '''
        try:
            engine = self.make_engine(database_I=database_I,user_I=user_I,password_I=password_I,host_I=host_I);
            conn = engine.connect();
            conn.execute("commit");
            return conn;
        except SQLAlchemyError as e:
            print(e);
            exit(-1);

    def create_newDatabaseAndUser(self,
            database_I='postgres',user_I='postgres',password_I='postgres',host_I="localhost:5432",
            database_O='sbaas01',user_O='guest',password_O='guest',privileges_O=['ALL PRIVILEGES'],tables_O=['ALL TABLES'],schema_O='public'):
        '''create a new database and user
        INPUT: settings to connect to an exiting database
        database_I
        user_I
        password_I
        host_I
        OUTPUT: settings for the new database and user
        database_O
        user_O
        password_O
        privileges_O
        tables_O
        schema_O'''
        try:
            conn = self.make_connection(database_I=database_I,user_I=user_I,password_I=password_I,host_I=host_I);
            self.create_database(conn,database_O);
            self.create_user(conn,user_O,password_O);
            self.grant_privileges(conn,user_O,privileges_O,tables_O,schema_O);
            conn.close();
        except SQLAlchemyError as e:
            print(e);
            exit(-1);
    
    def create_database(self,conn,database_I='sbaas',
            verbose_I=False):
        """create a new database"""
        cmd = 'CREATE DATABASE "%s"' %(database_I);
        if verbose_I:
            print(cmd);
        try:
            conn.execute(cmd);
            conn.commit();
        except SQLAlchemyError as e:
            print(e);
            #conn.rollback();

    def drop_database(self,conn,database_I='sbaas',
            verbose_I=False):
        """drop a database"""
        cmd = 'DROP DATABASE IF EXISTS "%s"' %(database_I);
        if verbose_I:
            print(cmd);
        try:
            conn.execute(cmd);
            conn.commit();
        except SQLAlchemyError as e:
            print(e);

    def drop_user(self,conn,user_I,
            verbose_I=False):
        """drop a user"""
        cmd = 'DROP USER IF EXISTS "%s"' %(user_I);
        if verbose_I:
            print(cmd);
        try:
            conn.execute(cmd);
            conn.commit();
        except SQLAlchemyError as e:
            print(e);

    def create_user(self,conn,user_I='guest',password_I='guest',
            verbose_I=False):
        '''create a new role with a password
        INPUT:
        user_I = username
        password_I = password'''

        try:
            cmd = 'CREATE USER "%s" ' %(user_I);
            cmd += "WITH PASSWORD '%s'" %(password_I);
            if verbose_I:
                print(cmd);
            conn.execute(cmd);
            #conn.execute("commit");
            conn.commit();
        except SQLAlchemyError as e:
            print(e);
            #conn.rollback();

    def grant_privileges(self,conn,user_I='guest',
            privileges_I=['SELECT'],
            tables_I=['ALL TABLES'],
            schema_I='public',
            verbose_I=False):
        '''grant privileges to user/role
        INPUT:
        user_I = username
        privileges_I = list of priveleges (e.g., ['SELECT','UPDATE','DELETE','INSERT']
        tables_I = list of tables
        schema_I = schema name
        '''

        try:
            privileges = ', '.join(privileges_I);
            tables = '';
            for table in tables_I:
                tables += ('"%s", '%(table));
            tables = tables[:-2];
            #cmd = 'GRANT %s ON %s IN SCHEMA %s TO "%s"' %(privileges,tables,schema_I,user_I);
            cmd = 'GRANT %s ON %s TO "%s"' %(privileges,tables,user_I);
            if verbose_I:
                print(cmd);
            conn.execute(cmd);
            conn.commit();
        except SQLAlchemyError as e:
            print(e);
            #conn.rollback();

    def revoke_privileges(self,conn,user_I='guest',
            privileges_I=['SELECT'],
            tables_I=['ALL TABLES'],
            schema_I='public',
            verbose_I=False):
        '''grant privileges to user/role
        INPUT:
        user_I = username
        privileges_I = list of priveleges (e.g., ['SELECT','UPDATE','DELETE','INSERT']
        tables_I = list of tables
        schema_I = schema name,
        '''

        try:
            privileges = ', '.join(privileges_I);
            tables = ', '.join(tables_I);
            for table in tables_I:
                for privilege in privileges_I:
                    #cmd = 'REVOKE %s ON %s IN SCHEMA %s FROM "%s"' %(privilege,table,schema_I,user_I);
                    cmd = 'REVOKE %s ON "%s" FROM "%s"' %(privilege,table,user_I);
                    if verbose_I:
                        print(cmd);
                    conn.execute(cmd);
                    conn.commit();
        except SQLAlchemyError as e:
            print(e);
            #conn.rollback();

    def set_sessionFromSettings(self,settings_I):
        '''set a session object ,database_I='sbaas01',user_I='guest',password_I='guest',host_I="localhost:5432"'''
        engine = self.make_engineFromSettings(settings_I);
        self.Session = sessionmaker(bind=engine, class_=_Session);

    def set_session(self,database_I='sbaas01',user_I='guest',password_I='guest',host_I="localhost:5432"):
        '''set a session object '''
        engine = self.make_engine(database_I=database_I,user_I=user_I,password_I=password_I,host_I=host_I);
        self.Session = sessionmaker(bind=engine, class_=_Session);

    def get_session(self):
        '''return new session object'''
        session = self.Session();
        return session; 

    def get_engine(self):
        '''return engine'''
        return self.engine; 

    def create_newDatabaseAndUserFromSettings(self,
            database_I='postgres',user_I='postgres',password_I='postgres',host_I="localhost:5432",
            settings_I = {},
            privileges_O=['ALL PRIVILEGES'],tables_O=['ALL TABLES'],schema_O='public'):
        '''create a new database and user
        INPUT: settings to connect to an exiting database
        database_I
        user_I
        password_I
        host_I
        OUTPUT: settings for the new database and user
        database_O (in settings_I)
        user_O (in settings_I)
        password_O (in settings_I)
        privileges_O
        tables_O
        schema_O'''
        try:
            conn = self.make_connection(database_I=database_I,user_I=user_I,password_I=password_I,host_I=host_I);
            self.create_database(conn,settings_I['database']);
            self.create_user(conn,settings_I['user'],settings_I['password'],privileges_O,tables_O,schema_O);
            self.grant_privileges(conn,settings_I['user'],privileges_O,tables_O,schema_O);
            conn.close();
        except SQLAlchemyError as e:
            print(e);
            exit(-1);

    def copy_databaseFromSettings(self,
            database_I='postgres',user_I='postgres',password_I='postgres',host_I="localhost:5432",
            settings_I = {},
            database_O = 'postgres_copy'):
        '''copy a database'''
        return;

    def dump_databaseFromSettings(self,
            settings_I = {},
            database_dump_options_I = {},
            filename_O='database_dump'):
        '''backup a database'''
        return;

    def restore_databaseFromSettings(self,
            settings_I = {},
            database_dump_options_I = {},
            filename_O='database_dump'):
        '''restore a database'''
        return;

    def create_policy(self,conn,
            user_I='guest',
            privileges_I=['SELECT'],
            tables_I=['ALL TABLES'],
            schema_I='public',
            using_I="",
            with_check_I="",
            verbose_I = False
            ):
        '''create table level policy
        INPUT:
        user_I = username
        privileges_I = list of priveleges (e.g., ['SELECT','UPDATE','DELETE','INSERT']
        tables_I = list of tables
        schema_I = string
        using_I = constraint for SELECT privilege
        with_check_ I = constraint for INSERT, UPDATE, DELETE privileges
        '''

        try:
            privileges = ' '.join(privileges_I);
            for table in tables_I:
                for privilege in privileges_I:
                    cmd = 'CREATE POLICY "%s_%s" ' %(user_I,privilege);
                    cmd += 'ON "%s"."%s" ' %(schema_I,table);
                    cmd += 'FOR %s TO "%s" ' %(privilege,user_I);
                    if privilege in ['UPDATE','SELECT','DELETE','ALL']:
                        cmd += "USING (%s) " %(using_I);
                    if privilege in ['UPDATE','INSERT','ALL']:
                        cmd += "WITH CHECK (%s) " %(using_I);
                    cmd += ';';
                    if verbose_I:
                        print(cmd);
                    try:
                        conn.execute(cmd);
                        conn.commit();
                    except SQLAlchemyError as e:
                        print(e);
                        conn.rollback();
        except SQLAlchemyError as e:
            print(e);
            #conn.rollback();

    def drop_policy(self,conn,
            user_I='guest',
            privileges_I=['SELECT'],
            tables_I=['ALL TABLES'],
            schema_I='public',
            verbose_I = False
            ):
        '''create table level policy
        INPUT:
        user_I = username
        privileges_I = list of priveleges (e.g., ['SELECT','UPDATE','DELETE','INSERT']
        tables_I = list of tables
        schema_I = string
        '''

        try:
            privileges = ' '.join(privileges_I);
            for table in tables_I:
                for privilege in privileges_I:
                    cmd = 'DROP POLICY IF EXISTS "%s_%s" ' %(user_I,privilege);
                    cmd += 'ON "%s"."%s" ' %(schema_I,table);
                    cmd += ';';
                    if verbose_I:
                        print(cmd);
                    try:
                        conn.execute(cmd);
                        conn.commit();
                    except SQLAlchemyError as e:
                        print(e);
                        conn.rollback();
        except SQLAlchemyError as e:
            print(e);
            #conn.rollback();

    def alter_table_action(self,conn,
            action_I='ENABLE ROW LEVEL SECURITY',
            tables_I=['ALL TABLES'],
            schema_I='public',
            verbose_I = False,
            ):
        '''alter tables using the ALTER TABLE action format
        INPUT:
        action_I = string
        tables_I = list of tables
        schema_I = string
        '''

        try:
            for table in tables_I:
                cmd = 'ALTER TABLE IF EXISTS "%s"."%s" ' %(schema_I,table);
                cmd += '%s ' %(action_I);
                cmd += ';';
                if verbose_I:
                    print(cmd);
                try:
                    conn.execute(cmd);
                    conn.commit();
                except SQLAlchemyError as e:
                    print(e);
                    conn.rollback();
        except SQLAlchemyError as e:
            print(e);
            #conn.rollback();
            
    
    def create_sequence(self,conn,sequence_I='_id_seq',
            verbose_I=False):
        """create a new sequence"""
        cmd = 'CREATE SEQUENCE "%s"' %(sequence_I);
        if verbose_I:
            print(cmd);
        try:
            conn.execute(cmd);
            conn.commit();
        except SQLAlchemyError as e:
            print(e);
            conn.rollback();

    def drop_sequence(self,conn,sequence_I='_id_seq',
            verbose_I=False):
        """drop a sequence"""
        cmd = 'DROP SEQUENCE IF EXISTS "%s"' %(sequence_I);
        if verbose_I:
            print(cmd);
        try:
            conn.execute(cmd);
            conn.commit();
        except SQLAlchemyError as e:
            print(e);
            conn.rollback();

    def alter_table_addConstraint(self,conn,
            constraint_name_I='',
            constraint_type_I='',
            constraint_columns_I=[],
            tables_I='',
            schema_I='public',
            verbose_I = False,
            ):
        '''alter tables using the ADD CONSTRAINT format
        INPUT:
        constraint_name_I = string
        constraint_type_I = string, e.g., UNIQUE, PRIMARY KEY, etc,.
        constraint_columns_I = list of column names to apply the constraint
        tables_I = string
        schema_I = string
        '''

        try:
            cmd = 'ALTER TABLE IF EXISTS "%s"."%s" ' %(schema_I,tables_I);
            cmd += 'ADD CONSTRAINT %s "%s"' %(constraint_type_I,constraint_name_I);
            columns_str = '(';
            for column in constraint_columns_I:
                columns_str += ('"%s",'%(column));
            columns_str += columns_str[:-1];
            columns_str += ')';
            cmd += columns_str;
            cmd += ';';
            if verbose_I:
                print(cmd);
            try:
                conn.execute(cmd);
                conn.commit();
            except SQLAlchemyError as e:
                print(e);
                conn.rollback();
        except SQLAlchemyError as e:
            print(e);
            #conn.rollback();

    def alter_table_drop(self,conn,
            attribute_I='CONSTRAINT',
            attribute_name_I='',
            tables_I='',
            schema_I='public',
            verbose_I = False,
            ):
        '''alter tables using the DROP format

        INPUT:
        attribute_I = string, attribute to drop
        attribute_name_I = name of the attribute
        tables_I = string
        schema_I = string
        '''

        try:
            cmd = 'ALTER TABLE IF EXISTS "%s"."%s" ' %(schema_I,tables_I);
            cmd += 'DROP %s "%s"' %(attribute_I,attribute_name_I);
            cmd += ';';
            if verbose_I:
                print(cmd);
            try:
                conn.execute(cmd);
                conn.commit();
            except SQLAlchemyError as e:
                print(e);
                conn.rollback();
        except SQLAlchemyError as e:
            print(e);
            #conn.rollback();
