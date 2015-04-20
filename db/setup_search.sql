drop table if exists complete;
CREATE TABLE complete(
        athlete_name text, 
        athlete_id int,
        event_name text, event_id int,
        sport_name text, sport_id int,
        olympic_year text, olympic_id int, 
        city_name text, city_id int,
        country_rep text, country_rep_id int,
        country_host text, country_host_id int,
        tsv tsvector
);
CREATE TRIGGER tsvectorupdate BEFORE INSERT OR UPDATE
ON complete FOR EACH ROW EXECUTE PROCEDURE
tsvector_update_trigger(tsv, 'pg_catalog.english', athlete_name, event_name, sport_name, olympic_year, city_name, country_rep, country_host);

INSERT INTO complete (athlete_name, 
athlete_id, 
event_name, 
event_id, 
sport_name, 
sport_id, 
country_rep,
country_rep_id,
country_host,
country_host_id,
olympic_year, 
olympic_id, 
city_name, 
city_id) 
(select a.first_name || ' ' || a.last_name, a.id, e.name,e.id, s.name,s.id, c.name, c.id, ch.name, ch.id, o.year::text, o.id, ci.name , ci.id  
from athletes as a 
join medals as m on m.athlete_id = a.id
join events as e on e.id = m.event_id
join sports as s on s.id = e.sport_id
join countries as c on c.id = m.country_id
join olympics as o on o.id = m.olympic_id
join cities as ci on ci.id = o.city_id
join countries as ch on ch.id = ci.country_id
);

CREATE INDEX gist_ind on complete USING gist(tsv);
