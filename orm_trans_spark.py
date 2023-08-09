from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class userinfo(Base):
    __tablename__ = 'user_info'
    summoner_id = Column(String, primary_key = True)
    summoner_name = Column(String)
    QType = Column(String)

engine = create_engine('sqlite:///sqlite_database_orm.db')
Session = sessionmaker(bind = engine)
session = Session()

df = [] 

rec = df.to_dict(orient = 'records')

for records in rec:
    user_info = userinfo(**records)
    session.add(user_info)

session.commit()
session.close()