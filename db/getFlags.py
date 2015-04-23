from bs4 import BeautifulSoup as bs
import inflection
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
import os

"""
init SQLAlchemy
"""
engine = create_engine('postgresql://postgres@localhost/olympics_dev', echo=False, isolation_level="AUTOCOMMIT")
Base = declarative_base(engine)
conn = engine.connect()

rows = conn.execute("Select name from countries")

fail_count = 0
suc_count = 0

base_url = "www.public-domain-image.com"
save_base = "/home/hannahb/cs373-idb/static/img/scrapeflag"

def full_path(country_name):
  return "http://www.public-domain-image.com/flags-of-the-world-public-domain-images-pictures/flag-of-{0}.jpg.html".format(country_name.lower().replace(' ', '-'))


for r in rows:
  country = r[0]
  path = full_path(country)
  try:
    page = urlopen(path)
    soup = bs(page.read())

    img = soup.findAll("img")[0]

    pull_path = base_url + img["src"]
    save_path = save_base + "/{0}.jpg".format(country.replace(' ', '_'))
    os.system("wget  --referer={2} {0} -O {1}".format(pull_path, save_path, path))
    suc_count = suc_count + 1
  except urllib.error.HTTPError as err:
    print('{0} {1}'.format(country, sys.exc_info()[0]))
    fail_count = fail_count + 1
    pass

print("Successes: {0} Failed: {1}".format(suc_count, fail_count))
