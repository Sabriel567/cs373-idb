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
engine = create_engine('postgresql://postgres:password@localhost/olympics_full', echo=False, isolation_level="AUTOCOMMIT")
Base = declarative_base(engine)
conn = engine.connect()

"""
  Data files
"""
DATA_ROOT = "/home/hannahb/data/"
MEDAL_DATA = DATA_ROOT + "OlympicsData-Medalists.csv"
SPORT_DATA = DATA_ROOT + "OlympicsData-Sports.csv"
CITY_DATA = DATA_ROOT + "cities.txt"
COUNTRY_DATA = DATA_ROOT + "IOCCOUNTRYCODES.csv"
COUNTRY_CODE_DATA = DATA_ROOT + "countryInfo.txt"
SUMMER_CITIES_DATA = DATA_ROOT + "SummerOlympicMedals.csv"
WINTER_CITIES_DATA = DATA_ROOT + "WinterOlympicsMedals.csv"

sports_dict = {}
country_code = {}
pp = pprint.PrettyPrinter(indent=4)
csv.field_size_limit(sys.maxsize)


def pop_sports(conn, pp):
  with open(SPORT_DATA) as csvfile:
    reader = csv.DictReader(csvfile)
    conn.execute("TRUNCATE sports CASCADE;");
    conn.execute("ALTER SEQUENCE sports_id_seq RESTART WITH 1;");
    for row in reader:
      sql = "Select id from sports where name = '{0}';".format(row['Sport'])
      res = conn.execute(sql);
      ret = res.first()
      if(ret == None):
        sql = "INSERT INTO sports (name) VALUES ('{0}') returning id;".format(row['Sport'])
        res = conn.execute(sql);
        sports_dict[row['SportID']] = res.first()[0]

def pop_events(conn, pp):
  conn.execute("TRUNCATE events CASCADE;");
  conn.execute("ALTER SEQUENCE events_id_seq RESTART WITH 1;");
  with open(MEDAL_DATA) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
      sql = "SELECT id FROM events WHERE name = '{0}' and sport_id = {1} and gender = '{2}';".format(row['Event'],sports_dict[row['SportID']], row['Gender'])
      res = conn.execute(sql);
      ret = res.first();
      if(ret == None):
        sql = "INSERT INTO events (sport_id, name, gender) VALUES ({0},'{1}', '{2}');".format(sports_dict[row['SportID']], row['Event'], row['Gender'])
        res = conn.execute(sql);

def pop_countries(conn, pp):
  with open(COUNTRY_DATA) as csvfile:
    reader = csv.DictReader(csvfile)
    conn.execute("TRUNCATE countries CASCADE;");
    conn.execute("ALTER SEQUENCE countries_id_seq RESTART WITH 1;");
    for row in reader:
      sql = "Select id from countries where name = '{0}'".format(row['Country'].replace("'", "''").replace("*", ""))
      res = conn.execute(sql);
      ret = res.first();
      if(ret == None):
        sql = "INSERT INTO countries (name, noc) VALUES ('{0}','{1}');".format(row['Country'].replace("'", "''").replace("*", ""),row['Int Olympic Committee code']); 
        res = conn.execute(sql);

def pop_cities(conn, pp):
  with open(COUNTRY_CODE_DATA) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
      country_code[row['ISO']] = row['Country']

  with open(CITY_DATA, encoding='utf-8') as csvfile:
    conn.execute("TRUNCATE cities CASCADE;");
    conn.execute("ALTER SEQUENCE cities_id_seq RESTART WITH 1;");
    reader = csv.DictReader(csvfile, delimiter=':')
    for row in reader:
      sql = """INSERT INTO cities (name, country_id)
              SELECT '{0}', id from countries 
              where noc = '{1}';""".format(row['name'].replace("'", "''"), row['countrycode'])
      res = conn.execute(sql);

def pop_years(conn, pp):
  conn.execute("TRUNCATE olympics CASCADE;");
  conn.execute("ALTER SEQUENCE olympics_id_seq RESTART WITH 1;");
  with open(SUMMER_CITIES_DATA) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
      year = int(row['Edition'])
      if(year >=  1968):
        sql = "Select id FROM olympics WHERE year = {0} and season = 'Summer';".format(year)
        res = conn.execute(sql);
        ret = res.first();
        if(ret == None):
          sql = """INSERT INTO olympics (city_id, year, season) 
                    SELECT cities.id as city_id, 
                    {0}, 'Summer' from cities 
                    where name = '{1}'
                    GROUP BY city_id
                    LIMIT 1""".format(row['Edition'], row['City'].replace("'", "''"))

          res = conn.execute(sql);
      
  with open(WINTER_CITIES_DATA) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
      sql = "Select id FROM olympics WHERE year = {0} and season = 'Winter';".format(row['Year'])
      res = conn.execute(sql);
      ret = res.first();
      if(ret == None):
        sql = """INSERT INTO olympics (city_id, year, season) 
                  SELECT case when (select count(1) from cities where name like '{1}%%') > 1 then 1 else id end as city_id, 
                  {0}, 'Winter' from cities 
                  where name like '{1}%%'
                  group by city_id""".format(row['Year'], row['City'].replace("'", "''"))
        res = conn.execute(sql);

def pop_athletes(conn, pp):
  with open(MEDAL_DATA) as csvfile:
    reader = csv.DictReader(csvfile)
    conn.execute("TRUNCATE athletes CASCADE;");
    conn.execute("ALTER SEQUENCE athletes_id_seq RESTART WITH 1;");
    for row in reader:
      last_name = (row['Athlete'].split(', '))[0].replace("'", "''")
      first_name = row['Athlete'].split(', ')[1].replace("'", "''") if len(row['Athlete'].split(', ')) == 2 else ""
      sql = """SELECT id FROM athletes 
             WHERE first_name = '{0}' AND last_name = '{1}';""".format(inflection.titleize(first_name), 
                                                                    inflection.titleize(last_name))
      res = conn.execute(sql);
      ret = res.first();
      if(ret == None):
        sql = """INSERT INTO athletes 
              (first_name, last_name, gender) 
              VALUES ('{0}','{1}','{2}');""".format(inflection.titleize(first_name), 
                                                 inflection.titleize(last_name),
                                                 row['Gender'])
        res = conn.execute(sql)

def pop_medals(conn, pp):
  with open(MEDAL_DATA) as csvfile:
    #conn.execute("TRUNCATE nocs CASCADE;");
    conn.execute("TRUNCATE medals CASCADE;");
    conn.execute("ALTER SEQUENCE medals_id_seq RESTART WITH 1;");
    reader = csv.DictReader(csvfile)
    for row in reader:
      last_name = (row['Athlete'].split(', '))[0].replace("'", "''")
      first_name = row['Athlete'].split(', ')[1].replace("'", "''") if len(row['Athlete'].split(', ')) == 2 else ""
      year = int(row['Edition'])
      if(year >= 1968 and row['Season'] == 'Summer'):
        sql = """with
          event as (select id from events where name = '{0}' and sport_id = {1} and gender = '{2}'),
          athlete as (select id from athletes where first_name = '{3}' and last_name = '{4}'),
          country as (select id from countries where noc = '{5}'),
          olympic as (select id from olympics where year = {6} and season = '{7}')
 
              INSERT INTO medals (event_id, athlete_id, olympic_id, country_id, rank) 
                (SELECT event.id, athlete.id, olympic.id, country.id, '{8}' from event, athlete, olympic, country) returning id; 
                """.format(row['Event'], 
                          sports_dict[row['SportID']],
                          row['Gender'],
                          inflection.titleize(first_name), 
                          inflection.titleize(last_name), 
                          row['NOC'], 
                          row['Edition'], 
                          row['Season'],
                          row['Medal'])
        ret = conn.execute(sql)
"""
        try:  
          ret.first()[0]
        except:
          print("ERROR")
          sql = "INSERT INTO nocs (noc) VALUES ('{0}')".format(row['NOC'])
          ret = conn.execute(sql)
          print(row)
"""          

pop_sports(conn, pp)
pop_events(conn, pp)
pop_countries(conn, pp)
pop_cities(conn, pp)
pop_years(conn, pp)
pop_athletes(conn, pp)
pop_medals(conn, pp)
