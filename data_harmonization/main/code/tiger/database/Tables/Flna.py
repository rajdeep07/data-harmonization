from sqlalchemy import Integer, Column, String, Float, ForeignKey
from sqlalchemy import create_engine 
from sqlalchemy.orm import sessionmaker, relationship
from data_harmonization.main.code.tiger.database.Tables.Bottom import Base


class Flna(Base):
	__tablename__ = 'flna'

	id=Column(Integer(), primary_key=True)
	Name = Column(String(12))
	Address = Column(String(12))
	City = Column(String(12))
	State = Column(String(12))
	Zip = Column(Integer)
	source = Column(String(12))
	CISID = Column(Float)
	COF = Column(Float)
	cluster_id = Column(Integer)
	confidence = Column(Float)
