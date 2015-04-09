from flask import jsonify
from flask.ext.restful import Resource, abort
from sqlalchemy import distinct, func, String
from sqlalchemy.sql.expression import cast
from sqlalchemy.dialects.postgresql import array
from six import string_types
from itertools import count

import models as db

def add_keys(keys, row):
    
    if row is None:
        return dict()
    
    key_iter = iter(keys)
    
    dictionary = dict()
    for r in row:
        key = next(key_iter)
        
        if hasattr(r, '__iter__') and not isinstance(r, string_types):
            
            assert hasattr(key, '__iter__') and not isinstance(key, string_types)
            
            if all(map(lambda x: not hasattr(x, '__iter__') or isinstance(x, string_types), r)):
                dictionary[key[0]] = add_keys(key[1], r)
            else:
                dictionary[key[0]] = [add_keys(key[1], i) for i in r if any(i)]
        else:
            assert not hasattr(key, '__iter__') or isinstance(key, string_types)
            
            dictionary[key] = r
            
    return dictionary

def list_of_dict_to_dict_of_dict(l):
    return dict(zip(count(0), l))

class OlympicGamesList(Resource):
    def get(self):
        """
        Gathers all olympics from the database with their data
        return a json object representing the olympics
        """

        session = db.loadSession()

        # Make the sql query
        result = session.query(
            # What to select
            # distinct (because of multiple medals) has to go on the first element though we want distinct event ids
            distinct(db.Olympics.id),
            db.Olympics.year,
            db.Olympics.season,
            db.City.name,
            db.Country.name,
            # array_agg_cust so that each now will be an INDIVIDUAL olympic games
            func.array_agg_cust(distinct(array([cast(db.Event.id, String), db.Event.name, db.Sport.name])))
            )\
            .select_from(db.Olympics)\
            .join(db.City)\
            .join(db.Country)\
            .join(db.Medal,             db.Medal.olympic_id==db.Olympics.id)\
            .join(db.Event)\
            .join(db.Sport)\
            .group_by(db.Olympics.id,
            db.Olympics.year,
            db.Olympics.season,
            db.City.name,
            db.Country.name)\
            .all() # Actually executes the query and returns a list of tuples
        
        session.close()
        
        keys = ('id', 'year', 'season', 'city', 'country', ('events', ('id', 'name', 'sport')))
        
        all_olympics_dict = list_of_dict_to_dict_of_dict(add_keys(keys, row) for row in result)
        
        return jsonify(all_olympics_dict)
    
class IndividualOlympicGames(Resource):
    def get(self, olympic_id):
        """
        Gather specified olympics from the database with its data
        olympic_id a non-zero, positive int
        return a json object representing the olympic games
        """
        session = db.loadSession()

        assert type(olympic_id) == int

        # Make the sql query
        result = session.query(
            # What to select
            # distinct (because of multiple medals per event) has to go on the first element though we want distinct event ids
            distinct(db.Olympics.id),
            db.Olympics.year,
            db.Olympics.season,
            db.City.name,
            db.Country.name,
            # array_agg_cust so that each now will be an INDIVIDUAL olympic games
            func.array_agg_cust(distinct(array([cast(db.Event.id, String), db.Event.name, db.Sport.name])))
            )\
            .select_from(db.Olympics)\
            .join(db.City)\
            .join(db.Country)\
            .join(db.Medal,             db.Medal.olympic_id==db.Olympics.id)\
            .join(db.Event)\
            .join(db.Sport)\
            .filter(
                # What to filter by (where clause)
                db.Olympics.id==olympic_id)\
            .group_by(db.Olympics.id,
            db.Olympics.year,
            db.Olympics.season,
            db.City.name,
            db.Country.name)\
            .first() # Actually executes the query and returns a tuple
        
        session.close()
        
        keys = ('id', 'year', 'season', 'city', 'country', ('events', ('id', 'name', 'sport')))

        olympics_dict = add_keys(keys, result)

        return jsonify(olympics_dict)

class CountriesList(Resource):
    def get(self):
        """
        Gathers all countries from the database with their data
        return a json object representing the countries
        """
        
        session = db.loadSession()

        # Make the sql query
        result = session.query(
            # What to select
            # outerjoin defaults to a LEFT outer join, NOT full outer join
            db.Country.id,
            db.Country.name,
            func.array_agg_cust(array([cast(db.Olympics.id, String), cast(db.Olympics.year, String), db.Olympics.season, db.City.name]))
            )\
            .select_from(db.Country)\
            .outerjoin(db.City)\
            .outerjoin(db.Olympics)\
            .group_by(db.Country.id,
            db.Country.name)\
            .all() # Actually executes the query and returns a list of tuples
        
        session.close()
        
        keys = ('id', 'name', ('olympics-hosted', ('id', 'year', 'season', 'city')))
        
        all_countries_dict = list_of_dict_to_dict_of_dict(add_keys(keys, row) for row in result)
        
        return jsonify(all_countries_dict)

class IndividualCountry(Resource):
    def get(self, country_id):
        """
        Gather specified country from the database with its data
        country_id a non-zero, positive int
        return a json object representing the country
        """
        session = db.loadSession()

        assert type(country_id) == int

        # Make the sql query
        result = session.query(
            # What to select
            # outerjoin defaults to a LEFT outer join, NOT full outer join
            db.Country.id,
            db.Country.name,
            func.array_agg_cust(array([cast(db.Olympics.id, String), cast(db.Olympics.year, String), db.Olympics.season, db.City.name]))
            )\
            .select_from(db.Country)\
            .outerjoin(db.City)\
            .outerjoin(db.Olympics)\
            .filter(
                # What to filter by (where clause)
                db.Country.id==country_id)\
            .group_by(db.Country.id,
            db.Country.name)\
            .first() # Actually executes the query and returns a tuple
        
        session.close()
        
        keys = ('id', 'name', ('olympics-hosted', ('id', 'year', 'season', 'city')))

        country_dict = add_keys(keys, result)

        return jsonify(country_dict)
    
class EventsList(Resource):
    def get(self):
        """
        Gathers all events from the database with their data
        return a json object representing the events
        """
        
        session = db.loadSession()

        # Make the sql query
        result = session.query(
            # What to select
            # distinct because of multiple medals per event
            distinct(db.Event.id),
            db.Event.name,
            db.Sport.name,
            func.array_agg_cust(distinct(array([cast(db.Olympics.id, String), cast(db.Olympics.year, String), db.Olympics.season])))
            )\
            .select_from(db.Event)\
            .join(db.Sport)\
            .join(db.Medal)\
            .join(db.Olympics)\
            .group_by(db.Event.id,
            db.Event.name,
            db.Sport.name)\
            .all() # Actually executes the query and returns a list of tuples
        
        session.close()
        
        keys = ('id', 'name', 'sport', ('olympics', ('id', 'year', 'season')))
        
        all_events_dict = list_of_dict_to_dict_of_dict(add_keys(keys, row) for row in result)
        
        return jsonify(all_events_dict)
    
class IndividualEvent(Resource):
    def get(self, event_id):
        """
        Gather specified event from the database with its data
        event_id a non-zero, positive int
        return a json object representing the event
        """
        session = db.loadSession()

        assert type(event_id) == int

        # Make the sql query
        result = session.query(
            # What to select
            # distinct because of multiple medals per event
            distinct(db.Event.id),
            db.Event.name,
            db.Sport.name,
            func.array_agg_cust(distinct(array([cast(db.Olympics.id, String), cast(db.Olympics.year, String), db.Olympics.season])))
            )\
            .select_from(db.Event)\
            .join(db.Sport)\
            .join(db.Medal)\
            .join(db.Olympics)\
            .filter(
                # What to filter by (where clause)
                db.Event.id==event_id)\
            .group_by(db.Event.id,
            db.Event.name,
            db.Sport.name)\
            .first() # Actually executes the query and returns a tuple
        
        session.close()
        
        keys = ('id', 'name', 'sport', ('olympics', ('id', 'year', 'season')))
        
        event_dict = add_keys(keys, result)

        return jsonify(event_dict)
    
class AthletesList(Resource):
    def get(self):
        """
        Gathers all athletes from the database with their data
        return a json object representing the athletes
        """
        
        session = db.loadSession()

        # Make the sql query
        result = session.query(
            # What to select
            db.Athlete.id,
            db.Athlete.first_name,
            db.Athlete.last_name,
            db.Athlete.gender,
            func.array_agg_cust(array([cast(db.Medal.id, String), db.Medal.rank, db.Event.name, db.Sport.name, db.Olympics.season, cast(db.Olympics.year, String), db.Country.name]))
            )\
            .select_from(db.Athlete)\
            .join(db.Medal)\
            .join(db.Event)\
            .join(db.Sport)\
            .join(db.Olympics)\
            .join(db.Country)\
            .group_by(db.Athlete.id,
            db.Athlete.first_name,
            db.Athlete.last_name,
            db.Athlete.gender)\
            .all() # Actually executes the query and returns a list of tuples
        
        session.close()
        
        keys = ('id', 'first', 'last', 'gender', ('medals', ('id', 'rank', 'event', 'sport', 'season', 'year', 'repr')))
        
        all_athletes_dict = list_of_dict_to_dict_of_dict(add_keys(keys, row) for row in result)
        
        return jsonify(all_athletes_dict)
    
class IndividualAthlete(Resource):
    def get(self, athlete_id):
        """
        Gather specified athlete from the database with its data
        athlete_id a non-zero, positive int
        return a json object representing the athlete
        """
        session = db.loadSession()

        
        # Make the sql query
        result = session.query(
            # What to select
            db.Athlete.id,
            db.Athlete.first_name,
            db.Athlete.last_name,
            db.Athlete.gender,
            func.array_agg_cust(array([cast(db.Medal.id, String), db.Medal.rank, db.Event.name, db.Sport.name, db.Olympics.season, cast(db.Olympics.year, String), db.Country.name]))
            )\
            .select_from(db.Athlete)\
            .join(db.Medal)\
            .join(db.Event)\
            .join(db.Sport)\
            .join(db.Olympics)\
            .join(db.Country)\
            .filter(
                # What to filter by (where clause)
                db.Athlete.id==athlete_id)\
            .group_by(db.Athlete.id,
            db.Athlete.first_name,
            db.Athlete.last_name,
            db.Athlete.gender)\
            .first() # Actually executes the query and returns a tuple
        
        session.close()
        
        keys = ('id', 'first', 'last', 'gender', ('medals', ('id', 'rank', 'event', 'sport', 'season', 'year', 'repr')))
        
        athlete_dict = add_keys(keys, result)

        return jsonify(athlete_dict)
    
class MedalsList(Resource):
    def get(self):
        """
        Gathers all medals from the database with their data
        return a json object representing the medals
        """
        
        session = db.loadSession()

        # Make the sql query
        result = session.query(
            # What to select
            db.Medal.id,
            db.Medal.rank,
            db.Athlete.first_name + ' ' + db.Athlete.last_name,
            db.Event.name,
            db.Sport.name,
            db.Olympics.year,
            db.City.name,
            db.Country.name
            )\
            .select_from(db.Medal)\
            .join(db.Athlete)\
            .join(db.Event)\
            .join(db.Sport)\
            .join(db.Olympics)\
            .join(db.City)\
            .join(db.Country)\
            .all() # Actually executes the query and returns a list of tuples
        
        session.close()
        
        keys = ('id', 'rank', 'athlete', 'event', 'sport', 'year', 'city', 'country')
        
        all_medals_dict = list_of_dict_to_dict_of_dict(add_keys(keys, row) for row in result)
        
        return jsonify(all_medals_dict)
    
class IndividualMedal(Resource):
    def get(self, medal_id):
        """
        Gather specified medal from the database with its data
        medal_id a non-zero, positive int
        return a json object representing the medal
        """
        session = db.loadSession()

        assert type(medal_id) == int
        
        # Make the sql query
        result = session.query(
            # What to select
            db.Medal.id,
            db.Medal.rank,
            db.Athlete.first_name + ' ' + db.Athlete.last_name,
            db.Event.name,
            db.Sport.name,
            db.Olympics.year,
            db.City.name,
            db.Country.name
            )\
            .select_from(db.Medal)\
            .join(db.Athlete)\
            .join(db.Event)\
            .join(db.Sport)\
            .join(db.Olympics)\
            .join(db.City)\
            .join(db.Country)\
            .filter(
                # What to filter by (where clause)
                db.Medal.id==medal_id)\
            .first() # Actually executes the query and returns a tuple
        
        session.close()
        
        keys = ('id', 'rank', 'athlete', 'event', 'sport', 'year', 'city', 'country')

        medal_dict = add_keys(keys, result)

        return jsonify(medal_dict)
    
class MedalByRankList(Resource):
    def get(self, rank):
        """
        Gathers all medals from the database with their data
        return a json object representing the medals
        """
        
        rank=rank.lower()
        rank=rank.capitalize()
        
        if not(rank=='Gold' or rank=='Silver' or rank=='Bronze'):
            abort(404, message="rank must be one of the following: Gold, Silver, or Bronze".format(rank))
        
        session = db.loadSession()

        # Make the sql query
        result = session.query(
            # What to select
            db.Medal.id,
            db.Medal.rank,
            db.Athlete.first_name + ' ' + db.Athlete.last_name,
            db.Event.name,
            db.Sport.name,
            db.Olympics.year,
            db.City.name,
            db.Country.name
            )\
            .select_from(db.Medal)\
            .join(db.Athlete)\
            .join(db.Event)\
            .join(db.Sport)\
            .join(db.Olympics)\
            .join(db.City)\
            .join(db.Country)\
            .filter(
            # What to filter by (where clause)
            db.Medal.rank==rank)\
            .all() # Actually executes the query and returns a list of tuples
        
        session.close()
        
        keys = ('id', 'rank', 'athlete', 'event', 'sport', 'year', 'city', 'country')
        
        all_medals_dict = list_of_dict_to_dict_of_dict(add_keys(keys, row) for row in result)
        
        return jsonify(all_medals_dict)