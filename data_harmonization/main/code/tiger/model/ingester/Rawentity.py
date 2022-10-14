from sqlalchemy import BigInteger, Text, Float, Column
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship
from data_harmonization.main.code.tiger.model.ingester.Bottom import Base


class Rawentity(Base):
	__tablename__ = 'rawentity'

	id=Column(BigInteger, primary_key=True)
	Name = Column(Text)
	Address = Column(Text)
	City = Column(Text)
	State = Column(Text)
	Zip = Column(BigInteger)
	source = Column(Text)
	cluster_id = Column(BigInteger)
	confidence = Column(Float)

	@staticmethod
	def get_schema() -> dict:
		return {'Name': 'object', 'Address': 'object', 'City': 'object', 'State': 'object', 'Zip': 'int64', 'source': 'object', 'cluster_id': 'int64', 'confidence': 'float64', 'id': 'int64'}

	def __repr__(self) -> str:
		return f'<Rawentity Name:{self.Name} Address:{self.Address} City:{self.City} State:{self.State} Zip:{self.Zip} source:{self.source} cluster_id:{self.cluster_id} confidence:{self.confidence} id:{self.id} >'