from sqlalchemy import Integer, Column, String, Float, ForeignKey
from sqlalchemy import create_engine 
from sqlalchemy.orm import sessionmaker, relationship
from data_harmonization.main.code.tiger.database.Tables.Bottom import Base


class Pbna(Base):
	__tablename__ = 'pbna'

	id=Column(Integer(), primary_key=True)
	Name = Column(String)
	Address = Column(String)
	City = Column(String)
	State = Column(String)
	Zip = Column(Integer)
	source = Column(String)
	CISID = Column(Float)
	COF = Column(Float)
	cluster_id = Column(Integer)
	confidence = Column(Float)
