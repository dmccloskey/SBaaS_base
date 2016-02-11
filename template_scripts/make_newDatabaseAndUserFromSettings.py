import sys
sys.path.append('C:/Users/dmccloskey-sbrg/Google Drive/SBaaS_base')
from SBaaS_base.postgresql_settings import postgresql_settings
from SBaaS_base.postgresql_orm import postgresql_orm

# read in the settings file
filename = 'C:/Users/dmccloskey-sbrg/Google Drive/SBaaS_base/settings.ini';
pg_settings = postgresql_settings(filename);

# make a new database and user from the settings file
pg_orm = postgresql_orm();
pg_orm.create_newDatabaseAndUserFromSettings(
            # default login made when installing postgresql
            database_I='postgres',user_I='postgres',password_I='18dglass',host_I="localhost:5432",
            # new settings
            settings_I=pg_settings.database_settings,
            # privileges for the new user
            privileges_O=['ALL PRIVILEGES'],tables_O=['ALL TABLES'],schema_O='public');