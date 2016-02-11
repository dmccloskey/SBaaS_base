import sys
sys.path.append('C:/Users/dmccloskey-sbrg/Google Drive/SBaaS_base')
from SBaaS_base.postgresql_settings import postgresql_settings
from SBaaS_base.postgresql_orm import postgresql_orm

# make a new database and user
pg_orm = postgresql_orm();
conn = pg_orm.make_connection();
pg_orm.drop_database(conn,'sbaas01');
pg_orm.drop_user(conn,'test');
conn.close();
pg_orm.create_newDatabaseAndUser(user_O='test',password_O='test');