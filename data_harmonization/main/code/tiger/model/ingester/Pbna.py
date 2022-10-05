from sqlalchemy import BigInteger, Text, Float, Column
from sqlalchemy import create_engine 
from sqlalchemy.orm import sessionmaker, relationship
from data_harmonization.main.code.tiger.model.ingester.Bottom import Base


class Pbna(Base):
	__tablename__ = 'pbna'

	id=Column(BigInteger, primary_key=True)
	Name = Column(Text)
	Address = Column(Text)
	City = Column(Text)
	State = Column(Text)
	Zip = Column(BigInteger)
	source = Column(Text)
	CISID = Column(Float)
	COF = Column(Float)
	cluster_id = Column(BigInteger)
	confidence = Column(Float)

	def __repr__(self) -> str:
		return f'<Pbna Name:{self.Name} Address:{self.Address} City:{self.City} State:{self.State} Zip:{self.Zip} source:{self.source} CISID:{self.CISID} COF:{self.COF} cluster_id:{self.cluster_id} confidence:{self.confidence} >'