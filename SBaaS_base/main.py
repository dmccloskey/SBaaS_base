import sys
sys.path.append('C:/Users/dmccloskey-sbrg/Documents/GitHub/SBaaS_base')
from SBaaS_base.postgresql_settings import postgresql_settings
from SBaaS_base.postgresql_orm import postgresql_orm

# read in the settings file
filename = 'C:/Users/dmccloskey-sbrg/Google Drive/SBaaS_settings/settings_metabolomics.ini';
pg_settings = postgresql_settings(filename);

# connect to the database from the settings file
pg_orm = postgresql_orm();
pg_orm.set_sessionFromSettings(pg_settings.database_settings);
session = pg_orm.get_session();
engine = pg_orm.get_engine();

# your app...

users = ['edith_hernandez', 'wayne_yang', 'nathan_mihn', 'julia_xu'];
privileges_modify = ['SELECT','UPDATE','DELETE','INSERT'];
tables_modify = ['data_stage01_quantification_checkCV_dilutions',
    'data_stage01_quantification_checkCV_QCs',
    'data_stage01_quantification_checkCVAndExtracellular_averages',
    'data_stage01_quantification_checkISMatch',
    'data_stage01_quantification_checkLLOQAndULOQ',
    'data_stage01_quantification_averages',
    'data_stage01_quantification_averagesmi',
    'data_stage01_quantification_averagesmigeo',
    'data_stage01_quantification_dilutions',
    'data_stage01_quantification_LLOQAndULOQ',
    'data_stage01_quantification_mqresultstable',
    'data_stage01_quantification_normalized',
    'data_stage01_quantification_peakInformation',
    'data_stage01_quantification_peakResolution',
    'data_stage01_quantification_physiologicalRatios_averages',
    'data_stage01_quantification_physiologicalRatios_replicates',
    'data_stage01_quantification_QCs',
    'data_stage01_quantification_replicates',
    'data_stage01_quantification_replicatesmi',
    'visualization_user_login',]
tables_view = [
    'quantitation_method',
    'experiment',
    'sample',
    'sample_description',
    'visualization_role',
    'visualization_user',
    'visualization_user_password',
    'visualization_user_pipelines',
    'visualization_user_projects',
    'visualization_pipeline',
    'visualization_pipeline_description',
    'visualization_pipeline_status',
    'visualization_project',
    'visualization_project_description',
    'visualization_project_status',]
privileges_view = ['SELECT'];

#for user in users:
#    #create the new user
#    pg_orm.create_user(
#    session,
#    user_I = user,
#    password_I = user,
#    verbose_I=True
#    );

#for user in users:
#    #grant privileges to the new user
#    pg_orm.grant_privileges(
#        session,
#        user_I = user,
#        privileges_I = privileges_modify,
#        tables_I = tables_modify,
#        schema_I='public',
#        verbose_I=True
#        );
#    #grant privileges to the new user
#    pg_orm.grant_privileges(
#        session,
#        user_I = user,
#        privileges_I = privileges_view,
#        tables_I = tables_view,
#        schema_I='public',
#        verbose_I=True
#        );

pg_orm.alter_table_action(
    session,
    action_I='ENABLE ROW LEVEL SECURITY',
    tables_I=["visualization_user_pipelines"],
    schema_I='public',
    verbose_I=True
    )

tables_modify_1 = [
    'data_stage01_quantification_mqresultstable',
]

##tables_modify_1
#for user in users:
#    pg_orm.drop_policy(
#        session,
#        user_I = user,
#        privileges_I = privileges_modify,
#        tables_I = tables_modify_1,
#        schema_I='public',
#        verbose_I=True
#        );
    #pg_orm.create_policy(
    #    session,
    #    user_I = user,
    #    privileges_I = privileges_modify,
    #    tables_I = tables_modify_1,
    #    schema_I='public',
    #    using_I="sample_name LIKE '%_BloodProject01_%'",
    #    with_check_I="sample_name LIKE '%_BloodProject01_%'",
    #    verbose_I = True
    #    );

session.close();