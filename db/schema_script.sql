drop table if exists countries cascade;

CREATE TABLE countries(
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE,
  noc TEXT UNIQUE
);

drop table if exists athletes cascade;

CREATE TABLE athletes(
  id SERIAL PRIMARY KEY,
  first_name TEXT,
  last_name TEXT,
  gender TEXT
);


drop table if exists medals cascade;

CREATE TABLE medals(
  id SERIAL PRIMARY KEY,
  rank TEXT,
  athlete_id INT,
  event_id INT,
  country_id INT,
  olympic_id INT
);


drop table if exists events cascade;

CREATE TABLE events(
  id SERIAL PRIMARY KEY,
  sport_id INT,
  name TEXT,
  gender TEXT
);


drop table if exists sports cascade;

CREATE TABLE sports(
  id SERIAL PRIMARY KEY,
  name TEXT 
);

drop table if exists olympics cascade;

CREATE TABLE olympics(
  id SERIAL PRIMARY KEY,
  year INT,
  season TEXT, --summer or winter
  city_id INT
);

drop table if exists cities cascade;

CREATE TABLE cities(
  id SERIAL PRIMARY KEY,
  name TEXT,
  country_id INT
);


ALTER TABLE medals ADD CONSTRAINT athlete_id_fkey FOREIGN KEY(athlete_id) REFERENCES athletes(id);
ALTER TABLE medals ADD CONSTRAINT event_id_fkey FOREIGN KEY(event_id) REFERENCES events(id);
ALTER TABLE medals ADD CONSTRAINT country_id_fkey FOREIGN KEY(country_id) REFERENCES countries(id);
ALTER TABLE medals ADD CONSTRAINT olympic_id_fkey FOREIGN KEY(olympic_id) REFERENCES olympics(id);

ALTER TABLE events ADD CONSTRAINT sport_id_fkey FOREIGN KEY(sport_id) REFERENCES sports(id);

ALTER TABLE olympics ADD CONSTRAINT city_id_fkey FOREIGN KEY(city_id) REFERENCES cities(id);
ALTER TABLE cities ADD CONSTRAINT country_id_fkey FOREIGN KEY(country_id) REFERENCES countries(id);

ALTER TABLE olympics ADD CONSTRAINT olympics_year_season_unique UNIQUE (year,season);
ALTER TABLE sports ADD CONSTRAINT sports_name_unique UNIQUE (name);
ALTER TABLE countries ADD CONSTRAINT countries_noc_unique UNIQUE (noc);
ALTER TABLE athletes ADD CONSTRAINT athletes_name_unique UNIQUE (first_name, last_name);

