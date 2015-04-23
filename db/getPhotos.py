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

save_base = "/home/hannahb/cs373-idb/static/img/scrapeathlete/" 
base_url = "http://www.olympic.org"

url = "http://www.olympic.org/"

"""
page = urlopen(url)
soup = BeautifulSoup(page.read())

path = soup.findAll(id='ctl00_mainContent_AthleteIdentityCardBlock_CardDetails_IdentityCardImage_HtmlImage1')
if(path != []):
  imgUrl = path[0]["src"]
  print("url: {0}".format(base_url + imgUrl))
  urllib.request.urlretrieve((base_url + imgUrl).replace(' ', '%20'), save_base + "Michael_Phelps.jpg")
"""

num_found = 0;
not_found = 0;
checked = 0;
for r in rows:
  if (checked % 100 == 0):
    print("Checked: {0}; Found: {1}".format(checked, num_found))
  checked = checked + 1 

  athlete_name = r[0]
  url = "http://www.olympic.org/{0}".format(athlete_name.replace(' ','-'))
  try:
    page = urlopen(url)
    soup = BeautifulSoup(page.read())

    path = soup.findAll(id='ctl00_mainContent_AthleteIdentityCardBlock_CardDetails_IdentityCardImage_HtmlImage1')
    if(path != []):
      num_found = num_found + 1;
      imgUrl = path[0]["src"]
      save_path = (save_base + athlete_name.replace(' ','_') + ".jpg")
      urllib.request.urlretrieve((base_url + imgUrl).replace(' ', '%20'), save_path)
    else: 
      not_found = not_found + 1;
  except: 
    pass


print(num_found, not_found)
