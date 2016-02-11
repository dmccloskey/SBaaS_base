from .sbaas_base import sbaas_base
from sqlalchemy.exc import SQLAlchemyError

class sbaas_base_query_initialize(sbaas_base):
    def initialize_table_sqlalchemyModel(self, model_I):
        try:
            model_I.__table__.create(self.engine,True);
        except SQLAlchemyError as e:
            print(e);