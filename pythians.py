from flask import Flask, render_template
from flask.ext.restful import Api
from sqlalchemy import distinct, func, desc, and_, case, or_, String
from sqlalchemy.orm import aliased
from sqlalchemy.sql.functions import coalesce
from sqlalchemy.sql.expression import cast
from sqlalchemy.dialects.postgresql import array
from random import randint
from re import search as regex_search, split as regex_split

import models as db
from pythiansapp import app
from api import add_keys
from tests import get_test_results

import json, requests

@app.route('/index/')
@app.route('/home/')
@app.route('/')
def index(): 

    """
    renders index.html with the requested database data
    returns the rendered index.html page
    """
    # Get a database session from SQLAlchemy 
    session = db.loadSession()

    # sports - [{"sport_id" : id, "sport_name" : name, "total_athletes" : total}]
    sports = []
    sports_keys = ('sport_id', 'sport_name', 'total_athletes')

    # games - [{"country_id" : id, "country_name" : name, "city_name" : name, "olympic_id" : id, "olympic_year" : year}]
    games = []
    games_keys = ('country_id', 'country_name', 'city_name', 'olympic_id', 'olympic_year')

    # countries - [{"country_id" : id, "country_name" : name, "years_hosted" : [{"olympic_id" : id, "olympic_year" : year}], 
    #               "num_athlete" : count, "num_gold" : golds, "num_silver" : silvers, "num_bronze" : bronzes}]
    countries = []
    countries_keys = ('country_id', 'country_name', ('years_hosted',('olympic_id', 'olympic_year')), 'num_athlete', 'num_gold', 'num_silver', 'num_bronze')

    # athletes - [{"athlete_id" : id, "athlete_name" : first_name + " " + last_name, "num_gold" : golds, "num_silver" : silvers, "num_bronze" : bronzes, 
    #               "olympics_table" : [{"olympic_id" : id, "olympic_name" : city_name + " " + olympic_year, 
    #                 "sport_id" : id, "sport_name" : name, "event_id" : id, "event_name" : name, "medal_rank" : rank}]}]
    athletes = []
    athletes_keys = ('athlete_id', 'athlete_name', ('olympics_table', ('olympic_name', 'olympic_id', 'country_id', 'country_name', 'sport_id', 'sport_name', 'event_id', 'event_name', 'medal_rank')), 'num_gold', 'num_silver', 'num_bronze')
    
    featured_games = session.query(db.Country.id,
                                   db.Country.name, 
                                    db.City.name,  
                                    db.Olympics.id,
                                    db.Olympics.year)\
                      .select_from(db.Olympics)\
                      .join(db.City)\
                      .join(db.Country)\
                      .all()

    random_games = get_random_rows(4, featured_games)
    games = [add_keys(games_keys, row) for row in random_games]

    featured_sports = session.query(db.Sport.id, 
                                    db.Sport.name, 
                                    func.count(db.Medal.athlete_id))\
                      .select_from(db.Sport)\
                      .join(db.Event)\
                      .join(db.Medal)\
                      .group_by(db.Sport.id,
                                db.Sport.name)\
                      .all()

    random_sports = get_random_rows(4, featured_sports)
    sports = [add_keys(sports_keys, row) for row in random_sports]
    
    featured_countries = session.query(db.Country.id, 
                                db.Country.name,  
                                func.array_agg_cust(distinct(array([db.Olympics.id, db.Olympics.year]))),
                                func.count(distinct(db.Medal.athlete_id)),
                                func.sum(case([(db.Medal.rank=='Gold', 1)], else_=0)).label('gold'), 
                                func.sum(case([(db.Medal.rank=='Silver', 1)], else_=0)).label('silver'), 
                                func.sum(case([(db.Medal.rank=='Bronze', 1)], else_=0)).label('bronze'))\
                                .select_from(db.Country)\
                                .join(db.City)\
                                .join(db.Olympics)\
                                .join(db.Medal)\
                                .group_by(db.Country.name,
                                          db.Country.id)\
                                .all()
                            
    random_countries = get_random_rows(4, featured_countries)
    countries = [add_keys(countries_keys, row) for row in random_countries]

    featured_athletes = session.query(db.Athlete.id,
                                      db.Athlete.first_name + " " + db.Athlete.last_name,
                                      func.array_agg_cust(array([db.City.name + ' ' + cast(db.Olympics.year, String), cast(db.Olympics.id, String), cast(db.Country.id, String), db.Country.name, cast(db.Sport.id, String), db.Sport.name, cast(db.Event.id, String), db.Event.name, db.Medal.rank])),
                                      func.sum(case([(db.Medal.rank=='Gold', 1)], else_=0)).label('gold'),
                                      func.sum(case([(db.Medal.rank=='Silver', 1)], else_=0)).label('silver'),
                                      func.sum(case([(db.Medal.rank=='Bronze', 1)], else_=0)).label('bronze'))\
                                      .select_from(db.Athlete)\
                                      .join(db.Medal)\
                                        .join(db.Country)\
                                        .join(db.Event)\
                                        .join(db.Sport)\
                                        .join(db.Olympics)\
                                        .join(db.City)\
                                      .group_by(db.Athlete.id,
                                                db.Athlete.first_name + " " + db.Athlete.last_name)\
                                      .all()
    random_athletes = get_random_rows(4, featured_athletes)
    athletes = [add_keys(athletes_keys, row) for row in random_athletes]
    
    # Close the database session from SQLAlchemy
    session.close()

    # Get the rendered page
    rendered_page = render_template('index.html', 
                                    featured_games = games, 
                                    featured_sports = sports,
                                    featured_countries = countries,
                                    featured_athletes = athletes)

    assert(rendered_page is not None)

    return rendered_page

@app.route('/games/')
def games():

    """
    renders games.html with the requested database data
    returns the rendered games.html page
    """

    # Get a database session from SQLAlchemy 
    session = db.loadSession()

    # all_games - [{"olympic_name" : city_name + " " + olympic_year, "olympic_id" : id, "olympic_year": year}]
    all_games = []
    keys = ('olympic_name', 'olympic_year', 'olympic_id')

    all_games_query = session.query(db.City.name + " " + cast(db.Olympics.year, String), 
                                    db.Olympics.year,
                                    db.Olympics.id)\
                    .select_from(db.Olympics)\
                    .join(db.City)\
                    .order_by(db.Olympics.year)\
                    .all()

    all_games = [add_keys(keys, row) for row in all_games_query]

    # Close the database session from SQLAlchemy
    session.close()

    # Get the rendered page
    rendered_page = render_template('games.html', all_games = all_games)

    assert(rendered_page is not None)

    return rendered_page

@app.route('/games/<int:game_id>')
def games_id(game_id):

    """
    renders games.html with the requested olympics data using the given game_id
    returns the rendered games.html page
    """

    # Get a database session from SQLAlchemy
    session = db.loadSession()

    # host_city - the hosting city
    host_city = ""

    # year - the game year
    year = ""

    # top_athletes - [{"athlete_id" : id, "athlete_name" : first_name + " " + last_name, "country_id" : rep. country id, "country_name" : rep. country, 
    #                   "num_gold" : golds, "num_silver" : silvers, "num_bronze" : bronzes}]
    top_athletes = []
    top_athletes_keys = ('athlete_id', 'athlete_name', 'country_id', 'country_name', 'num_gold', 'num_silver', 'num_bronze')

    # top_countries - [{"country_id" : id, "country_name" : name, "num_gold" : country golds, "num_silver" : country silvers, "num_bronze" : country bronzes}]
    top_countries = []
    top_countries_keys = ('country_id', 'country_name', 'num_gold', 'num_silver', 'num_bronze')

    # all_events - [{"event_id" : id, "event_name" : name}]
    all_events = []
    events_keys = ('event_id', 'event_name')

    # all_countries - [{"country_id" : id, "country_name" : name}]
    all_countries = []
    countries_keys = ('country_id', 'country_name')

    host_query = session.query(db.City.name, 
                                db.Olympics.year)\
                    .select_from(db.Olympics)\
                    .filter(game_id == db.Olympics.id)\
                    .join(db.City)\
                    .all()

    host_city = host_query[0][0]
    year = host_query[0][1]

    top_athletes_query = session.query(db.Athlete.id,
                                       db.Athlete.first_name + ' ' + db.Athlete.last_name,
                                        db.Country.id,
                                        db.Country.name,
                                        func.sum(case([(db.Medal.rank=='Gold', 1)], else_=0)).label('gold'), 
                                        func.sum(case([(db.Medal.rank=='Silver', 1)], else_=0)).label('silver'), 
                                        func.sum(case([(db.Medal.rank=='Bronze', 1)], else_=0)).label('bronze')
                                      )\
                                      .select_from(db.Athlete)\
                                      .join(db.Medal)\
                                      .join(db.Country)\
                                      .filter(db.Medal.olympic_id == game_id)\
                                      .group_by(db.Athlete.id, db.Athlete.first_name, db.Athlete.last_name,
                                                db.Country.id, db.Country.name)\
                                      .order_by(func.sum(case([(db.Medal.rank=='Gold', 1)], else_=0)))\
                                      .limit(3)\
                                      .all()

    top_athletes = [add_keys(top_athletes_keys, row) for row in top_athletes_query]

    top_countries_query = session.query(db.Country.id,
                                        db.Country.name,
                                        func.sum(case([(db.Medal.rank=='Gold', 1)], else_=0)).label('gold'), 
                                        func.sum(case([(db.Medal.rank=='Silver', 1)], else_=0)).label('silver'), 
                                        func.sum(case([(db.Medal.rank=='Bronze', 1)], else_=0)).label('bronze')
                                      )\
                                      .select_from(db.Athlete)\
                                      .join(db.Medal)\
                                      .join(db.Country)\
                                      .filter(db.Medal.olympic_id == game_id)\
                                      .group_by(db.Country.id, db.Country.name)\
                                      .order_by(func.sum(case([(db.Medal.rank=='Gold', 1)], else_=0)).desc())\
                                      .all()
    
    top_countries = [add_keys(top_countries_keys, row) for row in top_countries_query]

    all_events_query = session.query(distinct(db.Event.id),
                                db.Event.name)\
                    .select_from(db.Event)\
                    .join(db.Medal)\
                    .join(db.Olympics)\
                    .filter(game_id == db.Olympics.id)\
                    .all()

    all_events = [add_keys(events_keys, row) for row in all_events_query]

    all_countries_query = session.query(distinct(db.Country.id), 
                                    db.Country.name)\
                        .select_from(db.Country)\
                        .join(db.Medal)\
                        .join(db.Olympics)\
                        .filter(game_id == db.Olympics.id)\
                        .all()

    all_countries = [add_keys(countries_keys, row) for row in all_countries_query]

    # Close the database session from SQLAlchemy
    session.close()

    # Get the rendered page
    rendered_page = render_template('games.html',
                                    host_city = host_city,
                                    year = year,
                                    top_athletes = top_athletes,
                                    top_countries = top_countries,
                                    all_events = all_events,
                                    all_countries = all_countries)

    assert(rendered_page is not None)

    return rendered_page

@app.route('/sports/')
@app.route('/sports/<string:sortBy>')
def sports(sortBy=None):

    """
    renders sports.html with the requested database data
    returns the rendered sports.html page
    """
    # Get a database session from SQLAlchemy
    session = db.loadSession()

    # featured sports - [{"sport_id" : id, "sport_name" : name}]
    featured_sports = [] 

    # sports - [{"sport_id" : id, "sport_name" : name}]
    sports = []
    
    sports_keys = ('sport_id', 'sport_name')

    sports_query = session.query(db.Sport.id, 
                            db.Sport.name)\
                            .select_from(db.Sport)\
                            .all()
    
    sports = [add_keys(sports_keys, row) for row in sports_query]

    random_sports = get_random_rows(3, sports_query)
    featured_sports = [add_keys(sports_keys, row) for row in random_sports]

    # Close the database session from SQLAlchemy
    session.close()

    if sortBy == "sort-by-asc" :
        sort = "by-asc"
    elif sortBy == "sort-by-desc" :
        sort = "by-desc"
    else :
        sort = "by-asc"

    # Get the rendered page
    rendered_page = render_template('sports.html',
                                    featured_sports = featured_sports,
                                    sports = sports, sortBy = sort)

    assert(rendered_page is not None)

    return rendered_page

@app.route('/sports/<int:sport_id>')
def sports_id(sport_id):

    """
    renders sports.html with the requested sports data using the given sports_id
    returns the rendered sports.html page
    """
    # Get a database session from SQLAlchemy
    session = db.loadSession()

    sport_name = session.query(db.Sport.name).select_from(db.Sport).filter(db.Sport.id == sport_id).first()[0]
    

    # top medalists - [{"athlete_id" : id, "athlete_name" : first_name + " " + last_name, "num_medals": total_medals }]
    top_medalists = []
    medalists_keys = ('athlete_id', 'athlete_name', 'num_medals')
    
    # sport_events - [{"event_id" : id, "event_name" : name}]
    sport_events = []
    event_keys = ('event_id', 'event_name')

    top_medalists_query = session.query(db.Athlete.id, 
                                    db.Athlete.first_name + ' ' + db.Athlete.last_name, 
                                    func.count(db.Medal.id))\
                            .select_from(db.Sport)\
                            .filter(db.Sport.id == sport_id)\
                            .join(db.Event)\
                            .join(db.Medal)\
                            .join(db.Athlete)\
                            .group_by(db.Athlete.id,
                                      db.Athlete.first_name + ' ' + db.Athlete.last_name)\
                            .order_by(func.count(db.Medal.id).desc())\
                            .all()

    top_medalists = [add_keys(medalists_keys, row) for row in top_medalists_query]

    events = session.query(db.Event.id,
                           db.Event.name)\
                .select_from(db.Event)\
                .filter(db.Event.sport_id == sport_id)\
                .all()
            
    sport_events = [add_keys(event_keys, row) for row in events]

    # Close the database session from SQLAlchemy
    session.close()

    # Get the rendered page
    rendered_page = render_template('sports.html', top_medalists = top_medalists,
                                    sport_events = sport_events, sport_name = sport_name)

    assert(rendered_page is not None)

    return rendered_page

@app.route('/events/')
@app.route('/events/<string:sortBy>')
def events(sortBy=None):

    """
    renders events.html with the requested database data
    returns the rendered events.html page
    """
    # Get a database session from SQLAlchemy
    session = db.loadSession()

    # events - [{"event_id" : id, "event_name" : name, "sport_id": id, "sport_name": name}]
    all_events = []
    events_keys = ('event_id', 'event_name', 'sport_id', 'sport_name')

    # featured events - [{"event_id" : id, "event_name" : name, "sport_id": id, "sport_name": name}]
    featured_events = []

    all_events_query = session.query(db.Event.id, 
                                    db.Event.name,
                                    db.Sport.id,
                                    db.Sport.name)\
                            .select_from(db.Event)\
                            .join(db.Sport)\
                            .all()
    
    all_events = [add_keys(events_keys, row) for row in all_events_query]

    random_events = get_random_rows(3, all_events_query)
    featured_events = [add_keys(events_keys, row) for row in random_events]

    # Close the database session from SQLAlchemy
    session.close()

    if sortBy == "sort-by-event-asc" :
        sort = "by-event-asc"
    elif sortBy == "sort-by-event-desc" :
        sort = "by-event-desc"
    elif sortBy == "sort-by-sport-asc" :
        sort = "by-sport-asc"
    elif sortBy == "sort-by-sport-desc" :
        sort = "by-sport-desc"
    else :
        sort = "by-event-asc"

    # Get the rendered page
    rendered_page = render_template('events.html', 
                                    featured_events = featured_events,
                                    all_events = all_events, sortBy = sort)

    assert(rendered_page is not None)

    return rendered_page  

@app.route('/events/<int:event_id>')
def events_id(event_id):

    """
    renders events.html with the requested event data using the given event_id
    returns the rendered events.html page
    """
    # Get a database session from SQLAlchemy
    session = db.loadSession()

    # medalists = [{"type": Gold, "athletes": [{"athlete_id" : id, "athlete_name" : first_name + " " + last_name, "country_id" : id, "country_name" : name, "medal_rank" : rank, "olympic_year" : year, "olympic_id" : id}]},
    #              {"type": "Silver", "athletes": [{"athlete_id" : id, "athlete_name" : first_name + " " + last_name, "country_id" : id, "country_name" : name, "medal_rank" : rank, "olympic_year" : year, "olympic_id" : id}]},
    #              {"type": "Bronze", "athletes": [{"athlete_id" : id, "athlete_name" : first_name + " " + last_name, "country_id" : id, "country_name" : name, "medal_rank" : rank, "olympic_year" : year, "olympic_id" : id}]}]
    medalists = []
    medalists_keys = ('type', 'medal_weight', ('athletes', ('athlete_id', 'athlete_name', 'country_id', 'country_name', 'olympic_id', 'olympic_year')))

    events_name = session.query(db.Event.name, 
                                db.Sport.name)\
                              .select_from(db.Event)\
                              .join(db.Sport)\
                              .filter(db.Event.id == event_id).all()

    event_name = events_name[0][1] +": "+ events_name[0][0]

    medalists_query = session.query(db.Medal.rank,
                                    case([(db.Medal.rank=='Gold', 3), (db.Medal.rank=='Silver', 2), (db.Medal.rank=='Bronze', 1)], else_=0).label('medal_weight'),
                                    func.array_agg_cust(array([cast(db.Athlete.id, String), 
                                    db.Athlete.first_name + ' ' + db.Athlete.last_name, 
                                    cast(db.Country.id, String), 
                                    db.Country.name,
                                    cast(db.Olympics.id, String), 
                                    cast(db.Olympics.year, String)])))\
                                .select_from(db.Medal)\
                                .join(db.Event)\
                                .join(db.Athlete)\
                                .join(db.Olympics)\
                                .join(db.Country)\
                                .filter(db.Event.id == event_id)\
                                .group_by(db.Medal.rank)\
                                .order_by('medal_weight')\
                                .all()
    
    medalists = [add_keys(medalists_keys, row) for row in medalists_query]

    # Close the database session from SQLAlchemy 
    session.close()

    # Get the rendered page
    rendered_page = render_template('events.html',
                            medalists = medalists,
                            event_name = event_name)

    assert(rendered_page is not None)

    return rendered_page


@app.route('/athletes/')
@app.route('/athletes/<string:sortBy>')
def athletes(sortBy=None):

    """
    renders athletes.html with the requested database data
    returns the rendered athletes.html page
    """
    # Get a database session from SQLAlchemy
    session = db.loadSession()

    # [{"athlete_id" : id, "athlete_name" : name, "country":{"country_id" : id, "country_name" : name, "latest_year": year, "latest_year_id": id}, 
    #                   "sports" : [{"sport_id" : id, "sport_name" : name}], "olympics" : [{"olympic_id" : id, "olympic_year" : year}], "num_medal" : total medals}]
    all_athletes_list = []
    keys = ('athlete_id', 'athlete_name', ('country', ('latest_year', 'latest_year_id', 'country_id', 'country_name')), ('sports', ('sport_id', 'sport_name')), ('olympics', ('olympic_id', 'olympic_year')), 'num_medal')

    result = session.query(
                db.Athlete.id,
                db.Athlete.first_name + ' ' + db.Athlete.last_name,
                func.max(array([cast(db.Olympics.year, String), cast(db.Olympics.id, String), cast(db.Country.id, String), db.Country.name])),
                func.array_agg_cust(distinct(array([cast(db.Sport.id, String), db.Sport.name]))),
                func.array_agg_cust(distinct(array([db.Olympics.id, db.Olympics.year]))),
                func.count(db.Medal.id).label('total_medals'))\
            .select_from(db.Athlete)\
            .join(db.Medal)\
            .join(db.Country)\
            .join(db.Event)\
            .join(db.Sport)\
            .join(db.Olympics)\
            .group_by(db.Athlete.id,
                db.Athlete.first_name + ' ' + db.Athlete.last_name,)\
            .all()

    # Close the database session from SQLAlchemy
    session.close()

    if sortBy == "sort-by-name" :
        sort = "by-name"
    elif sortBy == "sort-by-country" :
        sort = "by-country"
    elif sortBy == "sort-by-sport" :
        sort = "by-sport"
    elif sortBy == "sort-by-game" :
        sort = "by-game"
    elif sortBy == "sort-by-year" :
        sort = "by-year"
    else :
        sort = "by-id"

    all_athletes_list = [ add_keys(keys, row) for row in result]

    # Get the rendered page
    rendered_page = render_template('athletes.html',
            athletes=all_athletes_list, sortBy=sort)

    assert(rendered_page is not None)

    return rendered_page

@app.route('/athletes/<int:athlete_id>')
def athlete_id(athlete_id):

    """
    renders athletes.html with the requested athletes data using the given athlete_id
    returns the rendered athletes.html page
    """
    # Get a database session from SQLAlchemy
    session = db.loadSession()

    # top events list - [{"sport_id" : id, "sport_name" : name, "event_id" : id, "event_name" : name, "num_gold" : golds, "num_silver" : silvers, "num_bronze" : bronzes}] 
    top_events_list = []
    top_events_keys = ('event_id', 'event_name', 'sport_id', 'sport_name', 'num_gold', 'num_silver', 'num_bronze')

    # all_events - [{"olympic_year" : year, "olympic_id" : id, "country_id" : id, "country_name" : name, 
    #                   "event_id" : id, "event_name" : name, "sport_id" : id, "sport_name" : name, "medal_rank" : rank}]
    all_events = []
    all_events_keys = ('olympic_id', 'olympic_year', 'country_id', 'country_name', 'event_id', 'event_name', 'sport_id', 'sport_name', 'medal_rank')

    # athletes dict - {"athlete_id" : id, "athlete_name" : name, "athlete_gender" : gender, "country_id" : rep. country id, 
    #                   "country_name" : rep. country name, "num_medals" : total medals, "athlete_top_events" : top_events_list, "athlete_events" : all_events,
    #                   "sports" : [{'sport_id': id, 'sport_name': name }],
    #                   "olympics" : [{'olympic_id' : id, 'olympic_year' : year}]}
    athlete_dict = dict()
    athlete_keys = ('athlete_id', 'athlete_name', 'athlete_gender', 'country_id', 'country_name', ('sports', ('sports_id', 'sports_name')), ('olympics',('olympic_id', 'olympic_year')), 'num_medals')

    # Make a subquery to get the athlete's latest country represented
    get_athlete_sub = session.query(
        db.Medal.athlete_id,
        db.Country.id,
        db.Country.name)\
        .select_from(db.Medal)\
        .join(db.Country)\
        .join(db.Olympics)\
        .filter(db.Medal.athlete_id==athlete_id)\
        .order_by(db.Olympics.year.desc())\
        .limit(1)\
        .subquery()

    # Make a query to get the athlete's data
    athlete_data = session.query(
                db.Athlete.id,
                db.Athlete.first_name + ' ' + db.Athlete.last_name,
                db.Athlete.gender,
                get_athlete_sub.c.id,
                get_athlete_sub.c.name,
                func.array_agg_cust(distinct(array([cast(db.Sport.id, String), db.Sport.name]))),
                func.array_agg_cust(distinct(array([db.Olympics.id, db.Olympics.year]))),
                func.count(db.Medal.id))\
            .select_from(db.Athlete)\
            .join(db.Medal)\
            .join(get_athlete_sub, db.Athlete.id==get_athlete_sub.c.athlete_id)\
            .join(db.Event)\
            .join(db.Sport)\
            .join(db.Olympics)\
            .filter(db.Athlete.id == athlete_id)\
            .group_by(
                db.Athlete.id,
                db.Athlete.first_name + ' ' + db.Athlete.last_name,
                db.Athlete.gender,
                get_athlete_sub.c.id,
                get_athlete_sub.c.name)\
            .first()
        
    athlete_dict = add_keys(athlete_keys, athlete_data)
    
    # Make a query to get the top events for the athlete
    top_events_query = session.query(
            db.Event.id,
            db.Event.name,
            db.Sport.id,
            db.Sport.name,
            func.sum(case([(db.Medal.rank=='Gold', 1)], else_=0)).label('gold'),
            func.sum(case([(db.Medal.rank=='Silver', 1)], else_=0)).label('silver'),
            func.sum(case([(db.Medal.rank=='Bronze', 1)], else_=0)).label('bronze')
        )\
        .select_from(db.Event)\
        .join(db.Medal)\
        .join(db.Sport)\
        .filter(db.Medal.athlete_id==athlete_id)\
        .group_by(
            db.Event.id,
            db.Event.name,
            db.Sport.id,
            db.Sport.name)\
        .order_by('gold', 'silver', 'bronze')\
        .limit(3)\
        .all()
    
    # Put the results in a list of dictionaries
    top_events_list = [add_keys(top_events_keys, row) for row in top_events_query]
    
    # Make a query to get the games participated for the athlete
    events_query = session.query(
            db.Olympics.id,
            db.Olympics.year,
            db.Country.id,
            db.Country.name,
            db.Event.id,
            db.Event.name,
            db.Sport.id,
            db.Sport.name,
            db.Medal.rank
        )\
        .select_from(db.Olympics)\
        .join(db.Medal)\
        .join(db.Event)\
        .join(db.Sport)\
        .join(db.Country)\
        .filter(db.Medal.athlete_id==athlete_id)\
        .all()

    all_events = [add_keys(all_events_keys, row) for row in events_query]
    
    athlete_dict['athlete_top_events'] = top_events_list
    athlete_dict['athlete_events'] = all_events
    
    # Close database session from SQLAlchemy
    session.close()

    # Get the rendered page
    rendered_page = render_template('athletes.html', **athlete_dict)

    assert(rendered_page is not None)

    return rendered_page


@app.route('/countries/')
@app.route('/countries/<string:sortBy>')
def countries(sortBy=None): 
    """
    renders countries.html with the requested database data
    returns the rendered countries.html page
    """
    # Get a database session from SQLAlchemy
    session = db.loadSession()

    # featured countries - [{"country_id" : id, "country_name" : name, "years_hosted" : [{"olympic_id" : id, "olympic_year" : year}], 
    #                        "num_medal" : total country medals, "num_medalist" : total country medalists}] 
    featured_countries = []
    featured_country_keys = ('country_id', 'country_name', ('years_hosted', ('olympic_id', 'olympic_year')), 'num_medal', 'num_medalist')

    # all_countries - [{"country_id" : id, "country_name" : name, "years_hosted" : [{"olympic_id" : id, "olympic_year" : year}], 
    #                   "num_medal" : total country medals, "num_athlete" : total country athletes}]
    all_countries = []
    country_keys = ('country_id', 'country_name', ('years_hosted', ('olympic_id', 'olympic_year')), 'num_medal', 'num_athlete')

    countries = session.query(db.Country.id, 
                                db.Country.name,  
                                func.array_agg_cust(distinct(array([db.Olympics.id, db.Olympics.year]))),
                                func.count(db.Medal.id), 
                                func.count(distinct(db.Medal.athlete_id)))\
                                .select_from(db.Country)\
                                .outerjoin(db.Medal)\
                                .outerjoin(db.City)\
                                .outerjoin(db.Olympics)\
                                .group_by(db.Country.name, db.Country.id)\
                                .all()

    random_countries = get_random_rows(4, countries)
    featured_countries = [add_keys(featured_country_keys, row) for row in random_countries]
    
    all_countries = [add_keys(country_keys, row) for row in countries]

    # Close the database session from SQLAlchemy
    session.close()

    if sortBy == "sort-by-name" :
        sort = "by-name"
    elif sortBy == "sort-by-medals" :
        sort = "by-medals"
    elif sortBy == "sort-by-medalists" :
        sort = "by-medalists"
    else :
        sort = "by-id"


    # Get the rendered page
    rendered_page = render_template('countries.html',
                            all_countries = all_countries,
                            featured_countries = featured_countries,
                            sortBy=sort)

    assert(rendered_page is not None)

    return rendered_page

@app.route('/countries/<int:country_id>')
def country_id(country_id):

    """
    renders countries.html with the requested countries data using the given country_id
    returns the rendered countries.html page
    """
    # Get a database session from SQLAlchemy
    session = db.loadSession()

    # country name
    country_name = session.query(db.Country.name)\
                            .select_from(db.Country)\
                            .filter(db.Country.id == country_id)\
                            .all()[0][0]

    # total gold medals
    total_gold_count_medals = session.query(func.coalesce(func.sum(case([(db.Medal.rank == 'Gold', 1)], else_=0)), 0), 
                                        func.count(db.Medal.id))\
                                .select_from(db.Country)\
                                .filter(db.Country.id == country_id)\
                                .join(db.Medal)\
                                .all()

    # total medals overall
    total_medals = total_gold_count_medals[0][1]
    total_gold_medals = total_gold_count_medals[0][0]

    # total athletes
    total_athletes = session.query(func.count(distinct(db.Medal.athlete_id)))\
                            .select_from(db.Medal)\
                            .filter(db.Medal.country_id == country_id)\
                            .all()[0][0]

    # years hosted = [{"olympic_id" : id, "olympic_year" : olympic_year}]
    years_hosted = []

    years_hosted_query = session.query(db.Olympics.id, 
                                    db.Olympics.year)\
                            .select_from(db.Country)\
                            .filter(db.Country.id == country_id)\
                            .join(db.City)\
                            .join(db.Olympics)\
                            .all()

    for r in years_hosted_query:
        years_hosted.append({'olympic_id':r[0], 'olympic_year':r[1]})
    
    # top medalists - [{"athlete_id" : id, "athlete_first_name" : first_name, "athlete_last_name" : last_name, "athlete_gender" : gender}]
    top_medalists = []

    top_medalists_query = session.query(db.Athlete.id, 
                                    db.Athlete.first_name, 
                                    db.Athlete.last_name, 
                                    db.Athlete.gender)\
                            .select_from(db.Medal)\
                            .filter(db.Medal.country_id == country_id)\
                            .join(db.Athlete)\
                            .order_by(func.sum(case([(db.Medal.rank == 'Gold', 1)], else_=0)).desc())\
                            .group_by(db.Athlete.id, 
                                        db.Athlete.first_name, 
                                        db.Athlete.last_name, 
                                        db.Athlete.gender)\
                            .all()

    for r in top_medalists_query:
        top_medalists.append({'athlete_id':r[0], 'athlete_first_name':r[1], 'athlete_last_name':r[2], 'athlete_gender':r[3]})

    # top years - [{"olympic_id" : id, "olympic_year" : year, "num_medal" : total medals, 
    #               "medalists" : [{"athlete_id" : id, "athlete_name" : first_name + " " + last_name, 
    #                               "num_gold" : golds, "num_silver" : silvers, "num_bronze" : bronzes, "num_medal" : medals}]}]
    top_years = []

    top_years_query = session.query(db.Olympics.id, 
                                    db.Olympics.year, 
                                    db.Athlete.id, 
                                    db.Athlete.first_name + " " + db.Athlete.last_name, 
                                func.sum(case([(db.Medal.rank == 'Gold', 1)], else_=0)),
                                func.sum(case([(db.Medal.rank == 'Silver', 1)], else_=0)),
                                func.sum(case([(db.Medal.rank == 'Bronze', 1)], else_=0)),
                                func.count(1))\
                                .select_from(db.Olympics)\
                                .join(db.Medal)\
                                .join(db.Athlete)\
                                .filter(db.Medal.country_id == country_id)\
                                .group_by(db.Olympics.id, 
                                            db.Olympics.year, 
                                            db.Athlete.id, 
                                            db.Athlete.first_name, 
                                            db.Athlete.last_name)\
                                .order_by(db.Olympics.year)\
                                .all()

    top = {}
    for athlete in top_years_query:
        if athlete[0] not in top:
            top[athlete[0]] = [athlete[1], 0, []]
        top[athlete[0]][2].append(athlete[2:])
        top[athlete[0]][1] += athlete[-1]
        
    top_years_unpacked = list(top.items())
    top_years_unpacked.sort(key = lambda x : x[1][1], reverse = True)
    
    for year in top_years_unpacked:
        top_years.append({'olympic_id':year[0], 'olympic_year':year[1][0], 'num_medal':year[1][1], 
                            'medalists':[{'athlete_id':i[0], 'athlete_name':i[1], 'num_gold':i[2], 'num_silver':i[3], 'num_bronze':i[4], 'num_medal':i[5]} for i in year[1][2]]})

    # top events - [{"event_id" : id, "event_name" : name, "num_medal" : total medals}]
    top_events = []

    top_events_query = session.query(db.Event.id, 
                                db.Event.name, 
                                func.count(db.Medal.id))\
                        .select_from(db.Medal)\
                        .filter(db.Medal.country_id == country_id)\
                        .join(db.Event)\
                        .order_by(func.count(db.Medal.id).desc())\
                        .group_by(db.Event.id)\
                        .limit(4)\
                        .all()

    for r in top_events_query:
        top_events.append({'event_id':r[0], 'event_name':r[1], 'num_medal':r[2]})

    # Close the database session from SQLAlchemy
    session.close()

    # Get the rendered page
    rendered_page = render_template("countries.html",
                            country_id = country_id,
                            country_name = country_name,
                            total_gold_medals = total_gold_medals,
                            total_medals = total_medals,
                            total_athletes = total_athletes,
                            years_hosted = years_hosted,
                            top_medalists = top_medalists,
                            top_years = top_years,
                            top_events = top_events)

    assert(rendered_page is not None)

    return rendered_page

@app.route('/about/')
def about():

    """
    renders about.html 
    returns the rendered about.html page
    """
    

    team = []

    team.append({   "name" : "Patrick Aupperle", \
                    "bio" : "Senior at University of Texas at Austin, seeking a B.S. in Computer Science", \
                    "hobbies" : "Programming, Anime, Visual Novels", \
                    "responsibilities" : ["Virtual Machine setup", "Backend / Flask endpoints"], \
                    "commits" : 15, \
                    "issues": 2, \
                    "tests" : 0 })

    team.append({   "name" : "Ben Pang", \
                    "bio" : "Senior at University of Texas at Austin, seeking a B.S. in Computer Science", \
                    "hobbies" : "Video Games, Programming, Music, Drumming", \
                    "responsibilities" : ["Backend/Flask endpoints", "Unit Testing", "Flask Model Design"], \
                    "commits" : 19, \
                    "issues": 11, \
                    "tests" : 53 })

    team.append({   "name" : "Hannah Burch", \
                    "bio" : "Senior at University of Texas at Austin, seeking a B.S. in Computer Science", \
                    "hobbies" : "Programming, Gaming, Drinking (clear liquids most of the time)", \
                    "responsibilities" : ["Backend/Flask", "Endpoints/Unit Testing", "Flask Model Design"], \
                    "commits" : 27, \
                    "issues": 12, \
                    "tests" : 16 })

    team.append({   "name" : "Andy Fan", \
                    "bio" : "Senior at University of Texas at Austin, seeking a B.S. in Computer Science and B.S. in Radio-TV-Film", \
                    "hobbies" : "Programming, digital media production, filmmaking, gaming", \
                    "responsibilities" : ["Frontend Design/Development","Flask model design"], \
                    "commits" : 75, \
                    "issues": 16, \
                    "tests" : 0 })

    team.append({   "name" : "Jacob Kovar", \
                    "bio" : "Senior at University of Texas at Austin, seeking a B.S. in Computer Science", \
                    "hobbies" : "Juggling, Tae Kwon Do, and video games", \
                    "responsibilities" : ["Backend","Flask endpoints","Unit testing","Flask model design"], \
                    "commits" : 18, \
                    "issues": 73, \
                    "tests" : 27 })

    team.append({   "name" : "Eric Su", \
                    "bio" : "Senior at University of Texas at Austin, seeking a B.A. in Computer Science", \
                    "hobbies" : "Singing, Guitar", \
                    "responsibilities" : ["Frontend Design/Development"], \
                    "commits" : 62, \
                    "issues": 48, \
                    "tests" : 0 })

    # Get the rendered page
    rendered_page = render_template('about.html', team=team)

    assert(rendered_page is not None)

    return rendered_page

@app.route('/testresults/')
def testresults():

    """
    renders testresults.html 
    returns the rendered testresults.html page
    """
    
    results = get_test_results()
    
    # Get the rendered page
    rendered_page = render_template('testresults.html', results=results)

    assert(rendered_page is not None)

    return rendered_page

@app.route('/search/?q=<string:search_criteria>')
def search(search_criteria=None):
    
    """ dictionary -
    {
        "or":
        {
            "Athletes":    [{"id":id "name": name, "terms_matched": [matched terms], "items_matched": [matched items] }],
            "Sports":      [{"id":id "name": name, "terms_matched": [matched terms], "items_matched": [matched items] }],
            "Events":      [{"id":id "name": name, "terms_matched": [matched terms], "items_matched": [matched items] }],
            "Years":       [{"id":id "name": name, "terms_matched": [matched terms], "items_matched": [matched items] }],
            "Countries":   [{"id":id "name": name, "terms_matched": [matched terms], "items_matched": [matched items] }]
        },
        
        "and":
        {
            "Athletes":    [{"id":id "name": name, "terms_matched": [matched terms], "items_matched": [matched items] }],
            "Sports":      [{"id":id "name": name, "terms_matched": [matched terms], "items_matched": [matched items] }],
            "Events":      [{"id":id "name": name, "terms_matched": [matched terms], "items_matched": [matched items] }],
            "Years":       [{"id":id "name": name, "terms_matched": [matched terms], "items_matched": [matched items] }],
            "Countries":   [{"id":id "name": name, "terms_matched": [matched terms], "items_matched": [matched items] }]
        }
    }
    
    [matched terms] - ["<b>Matched_term</b>", ...]
    [matched items] - [ 'athlete':
                            {'athlete_name': athlete_name, 'athlete_id': athlete_id},
                        'sport':
                            {'sport_name': sport_name, 'sport_id': sport_id},
                        'event':
                            {'event_name': event_name, 'event_id': event_id},
                        'olympics':
                            {'olympic_year': olympic_year, 'olympic_id': olympic_id},
                        'city':
                            {'city_name': city_name, 'city_id': olympic_id}, Because no city id link
                        'country_rep':
                            {'country_rep_name': country_rep_name, 'country_rep_id': country_rep_id},
                        'country_host':
                            {'country_host_name': country_host_name, 'country_host_id': country_host_id }}, ...]
    """
    
    # Pillar names to display on search page
    #   bool_type used only for algorithm and
    #   Countries repeat in order to combine host and repr countries under one category
    categories = ('bool_type', 'Athletes', 'Sports', 'Events', 'Years', 'City', 'Countries', 'Countries')
    row_keys = ( ('athlete',    ('athlete_name', 'athlete_id')),
                ('sport',       ('sport_name', 'sport_id')),
                ('event',       ('event_name', 'event_id')),
                ('olympics',    ('olympic_year', 'olympic_id')),
                ('city',        ('city_name', 'olympic_id')), # Because no city id link
                ('country_rep', ('country_rep_name', 'country_rep_id')),
                ('country_host',('country_host_name', 'country_host_id')))
    
    # Make empty dictionaries, ignoring 'bool_type' key
    dictionary = {'or': {k:(set(), []) for k in categories[1:]},
                  'and': {k:(set(), []) for k in categories[1:]}}

    # Check if no search result
    if search_criteria != None:
        
        # Split by any number of spaces, then filter out the likely empty strings
        #   so that when joining each element of the string array it doesn't
        #   include empty string elements which would error out the database
        search_criteria_seq = list(filter(lambda x: x != '', regex_split('[ ]+', search_criteria)))
        
        or_search  = ' | '.join(search_criteria_seq)
        and_search = ' & '.join(search_criteria_seq)
        
        result = db.execute_search(or_search, and_search)
        
        for row in result:
            
            # Add the categories to the row
            catergoried_row = zip(categories, row)
            
            # Get or/and element
            bool_type = next(catergoried_row)[1]
            
            # Used to gather all items within the row
            #   All categories of this row will have the same copy of this dictionary
            #   So updating this dictionary updates all of the category's lists
            items_matched = add_keys(row_keys, row[1:])
            
            # Used to gather all matched terms
            #   All categories of this row will have the same copy of this list
            #   So updating this list updates all of the category's lists
            terms_matched = []
            
            for col in catergoried_row:
                category, name_id_pair = col
                name, id = name_id_pair
                
                # Find the term that matched for this row and add it to the list
                match = regex_search('<b>.*</b>', name)
                if(match is not None):
                    terms_matched.append(match.group())
                
                contains_set, category_list = dictionary[bool_type][category]
                
                # Add the dictionary to the list if it wasn't already added, barring duplicates
                if id not in contains_set:
                    item = {'id':id, 'name':name, 'terms_matched':terms_matched, 'items_matched':[items_matched]}
                    category_list.append(item)
                    contains_set.add(id)
                # If a duplicate was found, then find the 
                else:
                    for d in category_list:
                        if d['id']==id:
                            d['items_matched'].append(items_matched)
                            
        
        # Make category keys only have the list as a value
        for bool_type_dict in dictionary.values():
            for category in bool_type_dict:
                contains_set, category_list = bool_type_dict[category]
                bool_type_dict[category] = category_list
    
    results = dictionary
    
    rendered_page = render_template('search.html', **results)

    assert(rendered_page is not None)

    return rendered_page

@app.route('/starlords/')
def starlords():

    """
    renders starlords.html 
    returns the rendered starlords.html page
    """

    """
    starlords API critique

    1. no comments on the apiary API on units for some attributes - for example, star Luminosity is measured in units of L0 according to wikipedia
    2. some attributes have inconsistent units: planet length_of_day - some use earth hours, some use earth days
    3. apiary API does not reflect the actual data structures returned, example below:

        actual:
            
            all_constellations = {"page" : page_num, "total_pages" : total_pages, "num_results" : num_results, "objects": [{constellations}]}

        expected:

            all_constellations = {[constellations]}

    4. querying any specific object from their endpoints always requires the 'id' of the objects, so we're forced to always query all the objects first
       before being able to query specific ones. The key should be something more meaningful instead of some arbitrary database id that has no meaning to users.

    """

    host = "http://104.130.244.239/api/"

    all_constellations = json.loads(requests.get(host + "constellation").text)
    all_families = json.loads(requests.get(host + "family").text)
    all_stars = json.loads(requests.get(host + "star").text)
    all_planets = json.loads(requests.get(host + "planet").text)

    # average number of stars per constellation
    avgStarsPerConst = len(all_stars['objects']) / len(all_constellations['objects'])

    # average number of constellations per constellation family
    avgConstPerFamily = len(all_constellations['objects']) / len(all_families['objects'])

    # brightest constellation - {"constellation" : "const_name", "luminosity" : luminosity}
    # unit of luminosity is L0, where 1 L0 is equal to the luminosity of the sun (measured in Watts)
    b_const = ""
    max_luminosity = 0

    for const in all_constellations['objects']:
        const_luminosity = 0
        for star in all_stars['objects']:
            if star['fk_constellation_star'] == const['id']:
                const_luminosity += star['luminosity']

        if const_luminosity > max_luminosity:
            b_const = const['name']
            max_luminosity = const_luminosity

    brightestConst = {"constellation" : b_const, "luminosity" : max_luminosity}

    # hottest star - {"star" : "star_name", "temp" : temp}
    # unit of temperature is K, kelvins
    h_star = ""
    max_temp = 0

    for star in all_stars['objects']:
        if star['temperature'] > max_temp:
            h_star = star['name']
            max_temp = star['temperature']

    hottestStar = {"star": h_star, "temp" : max_temp}

    # planet with largest number of moons - {"planet" : "planet_name", "num_moons" : num_moons}
    # planet with longest days - {"planet" : "planet_name", "num_days" : num_days}
    # !! STARLORDS API BUG !! - some of their longest days are measured in hours and some are in days (ex. Jupiter is '9' hours and Mercury is '58' days)
    nm_planet = ""
    max_moons = 0

    ld_planet = ""
    longest_day = 0

    for planet in all_planets['objects']:
        if planet['moons'] > max_moons:
            nm_planet = planet['name']
            max_moons = planet['moons']
        if planet['length_of_day'] > longest_day:
            ld_planet = planet['name']
            longest_day = planet['length_of_day']

    planetWithMostMoons = {"planet" : nm_planet, "num_moons" : max_moons}
    planetWithLongestDay = {"planet" : ld_planet, "num_days" : longest_day}

    # pillars - ["starlords_api_endpoint"]
    pillars = ["constellation", "ExoPlanet", "family", "moon", "planet", "star"]

    rendered_page = render_template("starlords.html", 
                                    pillars=pillars,
                                    avgStarsPerConst=avgStarsPerConst,
                                    avgConstPerFamily=avgConstPerFamily,
                                    brightestConst=brightestConst,
                                    hottestStar=hottestStar,
                                    planetWithMostMoons=planetWithMostMoons,
                                    planetWithLongestDay=planetWithLongestDay)

    assert(rendered_page is not None)

    return rendered_page

@app.route('/starlords/<string:pillar>')
def starlords_pillar(pillar):

    """
    renders starlords.html 
    returns the rendered starlords.html page
    """

    host = "http://104.130.244.239/"

    get_text = requests.get(host + "api/"  + pillar).text
    results = json.loads(get_text)

    # Get the rendered page
    rendered_page = render_template('starlords.html', host=host, pillar=pillar, results=results)

    assert(rendered_page is not None)

    return rendered_page

@app.errorhandler(404)
def page_not_found(e):

    """
    renders 404.html
    returns the rendered 404.html page
    """
    # Get the rendered page
    rendered_page = render_template('404.html'), 404

    assert(rendered_page is not None)

    return rendered_page

def get_random_rows(num_rows, results):
    assert num_rows > 0
    
    num_rows = min(num_rows, len(results))
    randoms = []
    
    num_to_subtract = 1
    
    while len(randoms) < num_rows:
        random_index = randint(0, len(results)-num_to_subtract)
        random_result = results[random_index]
        randoms.append(random_result)
        del results[random_index]
        results.append(random_result)
        num_to_subtract += 1
        
    return randoms
    

"""
main
"""
if __name__ == '__main__':
    # app.debug = True
    app.run(host='0.0.0.0', port=5000)
