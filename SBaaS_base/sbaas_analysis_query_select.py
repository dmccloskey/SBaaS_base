from .sbaas_base_query_select import sbaas_base_query_select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from sqlalchemy.sql import select

class sbaas_analysis_query_select(sbaas_base_query_select):

    def get_rows_analysisID_sqlalchemyModel(self,
        model_I,analysis_id_I,used__I=True):
        '''Query rows from a sqlalchemy model table by analysis_id_I
        INPUT_I
        model_I = sqlalchemy class object
        analysis_id_I = string, analysis_id
        used__I = Boolean, used_
        OUTPUT:
        rows_O = listDict of rows
        '''
        try:
            data = self.session.query(model_I).filter(
                    model_I.analysis_id.like(analysis_id_I),
                    model_I.used_.is_(used__I)).all();
            rows_O = []
            if data: 
                for d in data:
                    rows_O.append(d.__repr__dict__());
            return rows_O;
        except SQLAlchemyError as e:
            print(e);
            
    def get_rowsAsDict_analysisID_sqlalchemyModel(self,
                model_I,column_I,analysis_id_I,used__I=True,
                ):
        '''Query rows as a dictionary of table column values
        from a sqlalchemy model table by analysis_id_I
        INPUT_I
        model_I = sqlalchemy class object
        column_I = column name to use as the dictionary key for unique column values
        analysis_id_I = string, analysis_id
        used__I = Boolean, used_
        OUTPUT:
        rows_O = listDict of rows
        '''
        try:
            if column_comparator_I == '<':
                data = self.session.query(model_I).filter(
                    model_I.analysis_id.like(analysis_id_I),
                    model_I.used_.is_(used__I)).all();
            rows_O = {};
            if data: 
                for d in data:
                    tmp = d.__repr__dict__();
                    if tmp[column_I] in rows_O.keys():
                        rows_O[column_I].append(tmp);
                    else:
                        rows_O[column_I] = [];
                        rows_O[column_I].append(tmp);
            return rows_O;
        except SQLAlchemyError as e:
            print(e);
            
    def get_rowsAsDict_analysisID_sqlalchemyModel(self,
                model_I,column_I,analysis_id_I,column_comparator_I = '<',column_value_I = 3,used__I=True,
                ):
        '''Query rows as a dictionary of table column values
        from a sqlalchemy model table by analysis_id_I
        INPUT_I
        model_I = sqlalchemy class object
        column_I = column name to use as the dictionary key for unique column values
        analysis_id_I = string, analysis_id
        used__I = Boolean, used_
        OUTPUT:
        rows_O = listDict of rows
        '''
        try:
            column = self.get_columnAttribute_sqlalchemyModel(model_I,column_I);
            if column_comparator_I == '<':
                data = self.session.query(model_I).filter(
                    model_I.analysis_id.like(analysis_id_I),
                    column < column_value_I,
                    model_I.used_.is_(used__I)).order_by(
                    column.asc()).all();
            rows_O = {};
            if data: 
                for d in data:
                    tmp = d.__repr__dict__();
                    if tmp[column_I] in rows_O.keys():
                        rows_O[column_I].append(tmp);
                    else:
                        rows_O[column_I] = [];
                        rows_O[column_I].append(tmp);
            return rows_O;
        except SQLAlchemyError as e:
            print(e);

