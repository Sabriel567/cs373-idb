import csv
import pprint
import sys
import inflection
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

"""
init SQLAlchemy
"""
engine = create_engine('postgresql://postgres:password@localhost/olympics', echo=False, isolation_level="AUTOCOMMIT")
Base = declarative_base(engine)
conn = engine.connect()

""" QUERY USED TO SELECT MEDAL ID's
select m.id, e.id, e.name as event, e.gender, s.id, s.name as sport, o.id, o.year, c.id, c.name, m.rank, a.id, a.first_name, a.last_name  from medals as m
join events as e on e.id = m.event_id
join sports as s on s.id = e.sport_id
join athletes as a on a.id = m.athlete_id
join countries as c on c.id = m.country_id
join olympics as o on o.id = m.olympic_id
where 
        e.name  not like '%team%' 
        and e.name  not like '%doubles%'
        and e.name not in ('basketball', 'baseball', 'football', 'handball', 'hockey')
        and o.year not in (1968,1972,1976,1980,1984,1988,1992,1996,2000,2004)
        and s.id not in (1,2,3, 10,11,15,4,17,16,20,25,28,32,34)
order by year, s.id, e.id
"""


def create_sub_sql(created_table_id, pull_table):
  return """select {1}.{0}id from medals as m
          join events as e on e.id = m.event_id
          join olympics as o on o.id = m.olympic_id
          join cities as c on c.id = o.city_id
          where m.id in (3,4,7,
                          1212,1213,1215,
                          2446,2498,2515,
                          3950,3927,3936,3920,
                          5382,5383,5384,
                          6949,6946,6948,
                          8349,8348,8345,8351,
                          10487,10507,10511,
                          12646,12648,12647,
                          14909,14907,14925,14927,
                          17179,17180,17162
                          )
          group by {1}.{0}id""".format(created_table_id, pull_table)
"""
  SPORTS
"""
created_table_id = "sport_"
created_table = "sports"
pull_table = "e"
table_columns = "id, name"
table_column_types = "id int, name text"
sql = """INSERT INTO {2} select {1}  from 
      dblink('dbname=olympics_full'::text, 
        'select {1} from {2} 
        where id in ({0})') AS t1 ({3})
      """.format(create_sub_sql(created_table_id, pull_table), table_columns, created_table, table_column_types)
print(sql)
conn.execute(sql)


"""
  EVENTS
"""
created_table_id = "event_"
created_table = "events"
pull_table = "m"
table_columns = "id, sport_id, name, gender"
table_column_types = "id int, sport_id int, name text, gender text"
sql = """INSERT INTO {2} select {1}  from 
      dblink('dbname=olympics_full'::text, 
        'select {1} from {2} 
        where id in ({0})') AS t1 ({3})
      """.format(create_sub_sql(created_table_id, pull_table), table_columns, created_table, table_column_types)
print(sql)
conn.execute(sql)
  
"""
  COUNTRIES
"""
created_table_id = "country_"
created_table = "countries"
pull_table = "m"
table_columns = "id, name, noc"
table_column_types = "id int, name text, noc text"
sql = """INSERT INTO {2} select {1}  from 
      dblink('dbname=olympics_full'::text, 
        'select {1} from {2} 
        where id in ({0})') AS t1 ({3})
      """.format(create_sub_sql(created_table_id, pull_table), table_columns, created_table, table_column_types)
print(sql)
conn.execute(sql)

pull_table = "c"
sql = """INSERT INTO {2} select {1}  from 
      dblink('dbname=olympics_full'::text, 
        'select {1} from {2} 
        where id in ({0}) and id not in ({4})') AS t1 ({3})
      """.format(create_sub_sql(created_table_id, pull_table), table_columns, created_table, table_column_types,create_sub_sql(created_table_id, "m"))
print(sql)
conn.execute(sql)

"""
  CITIES
"""
created_table_id = "city_"
created_table = "cities"
pull_table = "o"
table_columns = "id, name, country_id"
table_column_types = "id int, name text, country_id int"
sql = """INSERT INTO {2} select {1}  from 
      dblink('dbname=olympics_full'::text, 
        'select {1} from {2} 
        where id in ({0})') AS t1 ({3})
      """.format(create_sub_sql(created_table_id, pull_table), table_columns, created_table, table_column_types)
print(sql)
conn.execute(sql)

"""
  Athletes
"""
created_table_id = "athlete_"
created_table = "athletes"
pull_table = "m"
table_columns = "id, first_name, last_name, gender"
table_column_types = "id int, first_name text, last_name text, gender text"
sql = """INSERT INTO {2} select {1}  from 
      dblink('dbname=olympics_full'::text, 
        'select {1} from {2} 
        where id in ({0})') AS t1 ({3})
      """.format(create_sub_sql(created_table_id, pull_table), table_columns, created_table, table_column_types)
print(sql)
conn.execute(sql)

"""
  Olympics
"""
created_table_id = "olympic_"
created_table = "olympics"
pull_table = "m"
table_columns = "id, year, season, city_id"
table_column_types = "id int, year int, season text, city_id int"
sql = """INSERT INTO {2} select {1}  from 
      dblink('dbname=olympics_full'::text, 
        'select {1} from {2} 
        where id in ({0})') AS t1 ({3})
      """.format(create_sub_sql(created_table_id, pull_table), table_columns, created_table, table_column_types)
created_table_id = "sport"
print(sql)
conn.execute(sql)

"""
  Medals 
"""
created_table_id = ""
created_table = "medals"
pull_table = "m"
table_columns = "id, rank, athlete_id, event_id, country_id, olympic_id"
table_column_types = "id int, rank text, athlete_id int, event_id int, country_id int, olympic_id int"
sql = """INSERT INTO {2} select {1}  from 
      dblink('dbname=olympics_full'::text, 
        'select {1} from {2} 
        where id in ({0})') AS t1 ({3})
      """.format(create_sub_sql(created_table_id, pull_table), table_columns, created_table, table_column_types)
created_table_id = "sport"
print(sql)
conn.execute(sql)
