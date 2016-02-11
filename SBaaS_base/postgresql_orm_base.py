
from sqlalchemy import Table, MetaData, create_engine, Column, Integer, Boolean,\
    String, Float, Text, ForeignKey, and_, or_, not_, distinct, select, Sequence,\
    DateTime, Date, UniqueConstraint, ForeignKeyConstraint,PrimaryKeyConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
import datetime
Base = declarative_base();