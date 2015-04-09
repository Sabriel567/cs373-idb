from bs4 import BeautifulSoup
import urllib
from urllib.request import urlopen
import csv
import pprint
import sys
import inflection
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import unicodedata

"""
init SQLAlchemy
"""
engine = create_engine('postgresql://postgres@localhost/olympics_dev', echo=False, isolation_level="AUTOCOMMIT")
Base = declarative_base(engine)
conn = engine.connect()

rows = conn.execute("Select first_name::text || ' ' || last_name::text from athletes")

num_found = 0;
not_found = 0;
for r in rows:
  athlete_name = r[0]
  print(athlete_name)

  url = "http://www.olympic.org/{0}".format(athlete_name.replace(' ','-'))
  try:
    page = urlopen(url)
    soup = BeautifulSoup(page.read())

    path = soup.find(id='ctl00_mainContent_AthleteIdentityCardBlock_CardDetails_IdentityCardImage_HtmlImage1')
    if(path != None):
      num_found = num_found + 1;
      print("Found: {0}".format(num_found))
    else: 
      print("Not Found: {0}".format(not_found))
      not_found = not_found + 1;
  except: 
    print('EXCEPTION')
    pass


print(num_found, not_found)
