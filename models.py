from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

"""
init SQLAlchemy
"""
engine = create_engine('postgresql://postgres:password@localhost/olympics_full', echo=False)
Base = declarative_base(engine)

def loadSession():
    metadata = Base.metadata
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

"""
models
"""

class Athlete(Base):
    __tablename__ = 'athletes'
    __table_args__ = {'autoload':True}

class Event(Base):
    __tablename__ = 'events'
    __table_args__ = {'autoload':True}
    
class Sport(Base):
    __tablename__ = 'sports'
    __table_args__ = {'autoload':True}
    
class City(Base):
    __tablename__ = 'cities'
    __table_args__ = {'autoload':True}

class Country(Base):
    __tablename__ = 'countries'
    __table_args__ = {'autoload':True}

class Olympics(Base):
    __tablename__ = 'olympics'
    __table_args__ = {'autoload':True}

class Medal(Base):
    __tablename__ = 'medals'
    __table_args__ = {'autoload':True}
