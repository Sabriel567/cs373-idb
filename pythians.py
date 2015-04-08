from flask import Flask, render_template, jsonify
from flask.ext.restful import Api
from sqlalchemy import distinct, func, desc, and_, case, or_, String
from sqlalchemy.orm import aliased
from sqlalchemy.sql.functions import coalesce
from sqlalchemy.sql.expression import cast
from sqlalchemy.dialects.postgresql import array
from random import randint
from six import string_types

import models as db

from api import OlympicGamesList, IndividualOlympicGames, CountriesList, IndividualCountry, EventsList, IndividualEvent, AthletesList, IndividualAthlete, MedalsList, IndividualMedal, MedalByRankList, add_keys

"""
init Flask
"""
app = Flask(__name__)

restful_api = Api(app)
restful_api.add_resource(OlympicGamesList, '/scrape/olympics')
restful_api.add_resource(IndividualOlympicGames, '/scrape/olympics/<int:olympic_id>')
restful_api.add_resource(CountriesList, '/scrape/countries')
restful_api.add_resource(IndividualCountry, '/scrape/countries/<int:country_id>')
restful_api.add_resource(EventsList, '/scrape/events')
restful_api.add_resource(IndividualEvent, '/scrape/events/<int:event_id>')
restful_api.add_resource(AthletesList, '/scrape/athletes')
restful_api.add_resource(IndividualAthlete, '/scrape/athletes/<int:athlete_id>')
restful_api.add_resource(MedalsList, '/scrape/medals')
restful_api.add_resource(IndividualMedal, '/scrape/medals/<int:medal_id>')
restful_api.add_resource(MedalByRankList, '/scrape/medals/<rank>')

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

    # games - [{"country_id" : id, "country_name" : name, "city_name" : name, "olympic_id" : id, "olympic_year" : year}]
    games = []

    # TODO: fix "years_hosted" array aggregate
    # countries - [{"country_id" : id, "country_name" : name, "years_hosted" : [{"olympic_id" : id, "olympic_year" : year}], 
    #               "num_athlete" : count, "num_gold" : golds, "num_silver" : silvers, "num_bronze" : bronzes}]
    countries = []

    # athletes - [{"athlete_id" : id, "athlete_name" : first_name + " " + last_name, "num_gold" : golds, "num_silver" : silvers, "num_bronze" : bronzes, 
    #               "country_id" : id, "country_name" : country_name,  
    #               "olympics_table" : [{"olympics" : {"olympic_id" : id, "olympic_year" : year, "olympic_name" : city_name + " " + olympic_year}, 
    #                 "sport" : {"sport_id" : id, "sport_name" : name}, 
    #                 "event": {"event_id" : id, "event_name" : name, "medal_rank" : rank}}]}]
    athletes = dict()

    featured_games = session.query(db.Country.name, 
                                    db.Country.id, 
                                    db.City.name, 
                                    db.Olympics.year, 
                                    db.Olympics.id)\
                      .select_from(db.Olympics)\
                      .join(db.City)\
                      .join(db.Country)\
                      .limit(4)\
                      .all()

    games = [{'country_id': row[1],
               'country_name': row[0],
               'city_name': row[2],
               'olympic_id': row[4],
               'olympic_year': row[3],
              } for row in featured_games]

    featured_sports = session.query(db.Sport.id, 
                                    db.Sport.name, 
                                    func.count(db.Medal.athlete_id))\
                      .select_from(db.Sport)\
                      .join(db.Event)\
                      .join(db.Medal)\
                      .group_by(db.Sport.id, db.Sport.name)\
                      .limit(4)\
                      .all()

    sports = [{'sport_id':row[0],
               'sport_name':row[1],
               'total_athletes':row[2]
              }for row in featured_sports]
    
    featured_countries = session.query(db.Country.id, 
                                db.Country.name,  
                                func.array_agg(distinct(db.Olympics.year)),
                                func.count(distinct(db.Medal.athlete_id)),
                                func.sum(case([(db.Medal.rank=='Gold', 1)], else_=0)).label('gold'), 
                                func.sum(case([(db.Medal.rank=='Silver', 1)], else_=0)).label('silver'), 
                                func.sum(case([(db.Medal.rank=='Bronze', 1)], else_=0)).label('bronze'))\
                                .select_from(db.Country)\
                                .join(db.City)\
                                .join(db.Olympics)\
                                .join(db.Medal)\
                                .group_by(db.Country.name, db.Country.id)\
                                .limit(4)\
                                .all()

    countries = [{'country_id':row[0],
                  'country_name':row[1],
                  'years_hosted':row[2],
                  'num_athlete':row[3],
                  'num_gold':row[4],
                  'num_silver':row[5],
                  'num_bronze':row[6]
                 } for row in featured_countries]

    featured_athletes = session.query(db.Athlete.first_name + " " + db.Athlete.last_name,
                                      db.Athlete.id, 
                                      func.sum(case([(db.Medal.rank=='Gold', 1)], else_=0)).label('gold'),
                                      func.sum(case([(db.Medal.rank=='Silver', 1)], else_=0)).label('silver'),
                                      func.sum(case([(db.Medal.rank=='Bronze', 1)], else_=0)).label('bronze'))\
                                      .select_from(db.Athlete)\
                                      .join(db.Medal)\
                                      .join(db.Olympics)\
                                      .join(db.Country)\
                                      .group_by(db.Athlete.first_name + " " + db.Athlete.last_name, db.Athlete.id)\
                                      .limit(4)\
                                      .all()

    athlete_ids = [row[1] for row in featured_athletes]

    featured_athlete_events = session.query(db.Athlete.id, 
                                            db.City.name, 
                                            db.Country.id, 
                                            db.Country.name, 
                                            db.Olympics.id,
                                            db.Olympics.year, 
                                            db.Sport.id, 
                                            db.Sport.name, 
                                            db.Event.id, 
                                            db.Event.name,
                                            db.Medal.rank)\
                                            .select_from(db.Athlete)\
                                            .join(db.Medal)\
                                            .join(db.Country)\
                                            .join(db.Event)\
                                            .join(db.Sport)\
                                            .join(db.Olympics)\
                                            .join(db.City)\
                                            .filter(or_(db.Athlete.id == athlete_ids[0],
                                                        db.Athlete.id == athlete_ids[1],
                                                        db.Athlete.id == athlete_ids[2],))\
                                            .all()
    for r in featured_athletes:
        athletes[r[1]] = {'athlete_id':r[1],
                        'athlete_name':r[0],
                        'num_gold':r[2],
                        'num_silver':r[3],
                        'num_bronze':r[4],
                        'country_name':"",
                        'year': 0,
                        'olympics_table':[]
                        }

    for e in featured_athlete_events: 
        athletes[e[0]]['olympics_table'].append({'olympics':{'olympic_id':e[4], 'olympic_year':e[5], 'olympic_name':(str(e[1]) + " " + str(e[5]))},
                                        'sport':{'sport_id':e[6], 'sport_name':e[7]},
                                        'event':{'event_id':e[8], 'event_name':e[9], 'medal_rank':e[10]}})

        if athletes[e[0]]['year'] <= e[5]:
            athletes[e[0]]['year'] = e[5]
            athletes[e[0]]['country_id'] = e[2]
            athletes[e[0]]['country_name'] = e[3]

    # Close the database session from SQLAlchemy
    session.close()
    
    # Get the rendered page
    rendered_page = render_template('index.html', 
                                    featured_games = games, 
                                    featured_sports = sports,
                                    featured_countries = countries,
                                    featured_athletes_pic = athletes)

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

    # all_games - [{"olympic_name" : city_name + " " + olympic_year, "olympic_id" : id}]
    all_games = []

    all_games_query = session.query(db.City.name, 
                                    db.Olympics.year, 
                                    db.Olympics.id)\
                    .select_from(db.Olympics)\
                    .join(db.City)\
                    .order_by(db.Olympics.year)\
                    .all()

    for r in all_games_query:
        all_games.append({'olympic_name' : (r[0]) + " " + str(r[1]), 'olympic_id' : r[2]})

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

    # top_countries - [{"country_id" : id, "country_name" : name, "num_gold" : country golds, "num_silver" : country silvers, "num_bronze" : country bronzes}]
    top_countries = []

    # all_events - [{"event_id" : id, "sport_id" : id, "event_name" : name}]
    all_events = []

    # all_countries - [{"country_id" : id, "country_name" : name, "country_NOC" : NOC}]
    all_countries = []

    host_query = session.query(db.City.name, 
                                db.Olympics.year)\
                    .select_from(db.Olympics)\
                    .filter(game_id == db.Olympics.id)\
                    .join(db.City)\
                    .all()

    host_city = host_query[0][0]
    year = host_query[0][1]

    top_athletes_query = session.query(db.Athlete.id, db.Athlete.first_name, db.Athlete.last_name,
                                        db.Country.id, db.Country.name,
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


    for r in top_athletes_query:
        top_athletes.append({'athlete_id':r[0], 'athlete_name':r[1] + " " + r[2], 'country_id':r[3], 'country_name':r[4], 'num_gold':r[5], 'num_silver':r[6], 'num_bronze':r[7]})


    top_countries_query = session.query(db.Country.id, db.Country.name,
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
    
    for r in top_countries_query:
        top_countries.append({'country_id':r[0], 'country_name':r[1], 'num_gold':r[2], 'num_silver':r[3], 'num_bronze':r[4]})

    all_events_query = session.query(distinct(db.Event.id), 
                                db.Event.sport_id, 
                                db.Event.name)\
                    .select_from(db.Event)\
                    .join(db.Medal)\
                    .join(db.Olympics)\
                    .filter(game_id == db.Olympics.id)\
                    .all()

    for r in all_events_query:
        all_events.append({'event_id':r[0], 'sport_id':r[1], 'event_name':r[2]})

    all_countries_query = session.query(distinct(db.Country.id), 
                                    db.Country.name, 
                                    db.Country.noc)\
                        .select_from(db.Country)\
                        .join(db.Medal)\
                        .join(db.Olympics)\
                        .filter(game_id == db.Olympics.id)\
                        .all()

    for r in all_countries_query:
        all_countries.append({'country_id':r[0], 'country_name':r[1], 'country_NOC':r[2]})

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
def sports():

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

    sports_query = session.query(db.Sport.id, 
                            db.Sport.name)\
                            .select_from(db.Sport)\
                            .all()
    
    for r in sports_query:
        sports.append({'sport_id':r[0], 'sport_name':r[1]})

    # pick 3 random sports for featured_sports
    while len(featured_sports) < 3:
        sport = sports[randint(0, len(sports)) - 1]
        if sport not in featured_sports:
            featured_sports.append(sport)

    # Close the database session from SQLAlchemy
    session.close()

    # Get the rendered page
    rendered_page = render_template('sports.html',
                                    featured_sports = featured_sports,
                                    sports = sports)

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

    # top medalists - [{"athlete_id" : id, "athlete_name" : first_name + " " + last_name}]
    top_medalists = []

    top_medalists_query = session.query(db.Athlete.id, 
                                    db.Athlete.first_name + ' ' + db.Athlete.last_name, 
                                    func.count(db.Medal.rank))\
                            .select_from(db.Sport)\
                            .filter(db.Sport.id == sport_id)\
                            .join(db.Event)\
                            .join(db.Medal)\
                            .join(db.Athlete)\
                            .group_by(db.Athlete.id)\
                            .order_by(func.count(db.Medal.rank).desc())\
                            .all()

    for r in top_medalists_query:
        top_medalists.append({'athlete_id':r[0], 'athlete_name':r[1]})

    # TODO: add list of events

    # Close the database session from SQLAlchemy
    session.close()

    # Get the rendered page
    rendered_page = render_template('sports.html', top_medalists = top_medalists)

    assert(rendered_page is not None)

    return rendered_page

@app.route('/events/')
def events():

    """
    renders events.html with the requested database data
    returns the rendered events.html page
    """
    # Get a database session from SQLAlchemy
    session = db.loadSession()

    # events - [{"event_id" : id, "event_name" : name}]
    all_events = []

    # featured events - [{"event_id" : id, "event_name" : name}]
    featured_events = []

    all_events_query = session.query(db.Event.id, 
                                        db.Event.name)\
                            .select_from(db.Event)\
                            .all()
    
    for r in all_events_query:
        all_events.append({'event_id':r[0], 'event_name':r[1]})

    # featured_events - pick 3 random events
    while len(featured_events) < 3:
        event = all_events[randint(0, len(all_events)) - 1]
        if event not in featured_events:
            featured_events.append(event)

    # Close the database session from SQLAlchemy
    session.close()
    
    # Get the rendered page
    rendered_page = render_template('events.html', 
                                    featured_events = featured_events,
                                    all_events = all_events)

    assert(rendered_page is not None)

    return rendered_page 

@app.route('/events/<int:event_id>')
def events_id(event_id):

    """
    renders sports.html with the requested event data using the given event_id
    returns the rendered events.html page
    """
    # Get a database session from SQLAlchemy
    session = db.loadSession()

    # medalists = {"Gold" : [{"athlete_id" : id, "athlete_name" : first_name + " " + last_name, "country_id" : id, 
    #                                   "country_name" : name, "medal_rank" : rank, "olympic_year" : year, "olympic_id" : id}],
    #               "Silver" : [{"athlete_id" : id, "athlete_name" : first_name + " " + last_name, "country_id" : id, 
    #                                       "country_name" : name, "medal_rank" : rank, "olympic_year" : year, "olympic_id" : id}],
    #               "Bronze" : [{"athlete_id" : id, "athlete_name" : first_name + " " + last_name, "country_id" : id, 
    #                                      "country_name" : name, "medal_rank" : rank, "olympic_year" : year, "olympic_id" : id}]}
    medalists = []

    events_name = session.query(db.Event.name, 
                                db.Sport.name)\
                              .select_from(db.Event)\
                              .join(db.Sport)\
                              .filter(db.Event.id == event_id).all()

    event_name = events_name[0][1] +": "+ events_name[0][0]

    medalists_query = session.query(db.Athlete.id, 
                                    db.Athlete.first_name, 
                                    db.Athlete.last_name, 
                                    db.Country.id, 
                                    db.Country.name, 
                                    db.Event.name,
                                    db.Olympics.id, 
                                    db.Olympics.year, 
                                    db.Medal.rank)\
                                    .select_from(db.Medal)\
                                    .join(db.Event)\
                                    .join(db.Athlete)\
                                    .join(db.Olympics)\
                                    .join(db.Country)\
                                    .filter(db.Event.id == event_id).all()
                                    
    medalists = dict()
    for m in medalists_query:
      if m[8] not in medalists:
        medalists[m[8]] = ([{'athlete_id':int(m[0]),
                                'athlete_name': m[1] + " " + m[2],
                                'country_id':int(m[3]),
                                'country_name': m[4],
                                'rank': m[8],
                                'year':m[7],
                                'olympic_id':m[6]
                                }])
      else:
        medalists[m[8]].append([{'athlete_id':int(m[0]),
                                'athlete_name': m[1] + " " + m[2],
                                'country_id':int(m[3]),
                                'country_name': m[4],
                                'rank': m[8],
                                'year':m[7],
                                'olympic_id':m[6]
                                }])

    # Close the database session from SQLAlchemy 
    session.close()

    # Get the rendered page
    rendered_page = render_template('events.html',
                            medalists = medalists,
                            event_name = event_name)

    assert(rendered_page is not None)

    return rendered_page

@app.route('/athletes/')
def athletes():

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

    all_athletes_list = [ add_keys(keys, row) for row in result]

    # Get the rendered page
    rendered_page = render_template('athletes.html', athletes=all_athletes_list)

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

    # top events list - [{"sport_id" : id, "sport" : name, "event_id" : id, "event" : name, "num_gold" : golds, "num_silver" : silvers, "num_bronze" : bronzes}] 
    top_events_list = []

    # olympics dict - {olympic_year : [{"olympic_id" : id, "country_id" : id, "country_name" : name, 
    #                   "event_id" : id, "event_name" : name, "sport_id" : id, "sport_name" : name, "medal_rank" : rank}]}
    olympics_dict = dict()


    # athletes dict - {"athlete_id" : id, "athlete_name" : name, "athlete_gender" : gender, "country_id" : rep. country id, 
    #                   "country_name" : rep. country name,
    #                   "sports" : [{'sport_id': id, 'sport_name': name }],
    #                   "olympics" : [{'olympic_id' : id, 'olympic_year' : year}],
    #                   "num_medal" : total medals, "athlete_top_events" : top_events_list, "athlete_olympics" : olympics_dict}
    athletes_dict = dict()

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
                db.Athlete.first_name,
                db.Athlete.last_name,
                db.Athlete.gender,
                get_athlete_sub.c.id,
                get_athlete_sub.c.name,
                db.Sport.id,
                db.Sport.name,
                db.Olympics.id,
                db.Olympics.year,
                func.count(db.Medal.id))\
            .select_from(db.Athlete)\
            .join(db.Medal)\
            .join(get_athlete_sub, db.Athlete.id==get_athlete_sub.c.athlete_id)\
            .join(db.Event)\
            .join(db.Sport)\
            .join(db.Olympics)\
            .group_by(
                db.Athlete.id,
                db.Athlete.first_name,
                db.Athlete.last_name,
                db.Athlete.gender,
                get_athlete_sub.c.id,
                get_athlete_sub.c.name,
                db.Sport.id,
                db.Sport.name,
                db.Olympics.id,
                db.Olympics.year)\
            .all()
    
    # Make a query to get the top events for the athlete
    top_events_query = session.query(
            db.Event.id,
            db.Event.name,
            db.Sport.id,
            db.Sport.name,
            func.sum(case([(db.Medal.rank=='Gold', 1)], else_=0)).label('gold'), func.sum(case([(db.Medal.rank=='Silver', 1)], else_=0)).label('silver'), func.sum(case([(db.Medal.rank=='Bronze', 1)], else_=0)).label('bronze')
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
    top_events_list = [{
        'sport_id':r[2],
        'sport_name': r[3],
        'event_id':r[0],
        'event_name': r[1],
        'num_gold': r[4],
        'num_silver': r[5],
        'num_bronze': r[6]
        } for r in top_events_query]
    
    # Make a query to get the games participated for the athlete
    games_part = session.query(
            db.Olympics.id,
            db.Olympics.year,
            db.Country.id,
            db.Country.name,
            db.Event.id,
            db.Event.name,
            db.Event.id,
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

    for row in games_part:
        olympic_year = row[1]
        
        if olympic_year not in olympics_dict:
            olympics_dict[olympic_year] = [{
                'olympic_id': row[0],
                'country_id': row[2],
                'country_name': row[3],
                'event_id': row[4],
                'event_name': row[5],
                'sport_id': row[6],
                'sport_name': row[7],
                'medal_rank': row[8]}]
        else:
            olympics_dict[olympic_year].append({
                'olympic_id': row[0],
                'country_id': row[2],
                'country_name': row[3],
                'event_id': row[4],
                'event_name': row[5],
                'sport_id': row[6],
                'sport_name': row[7],
                'medal_rank': row[8]})
    
    # athletes dict - {"athlete_id" : id, "athlete_name" : name, "athlete_gender" : gender, "country_id" : rep. country id, 
    #                   "country_name" : rep. country name, "num_medal" : total medals, "athlete_top_events" : top_events_list, "athlete_olympics" : olympics_dict}
    
    athlete_dict = {
        'athlete_id': athlete_data[0][0],
        'athlete_name': athlete_data[0][1] + ' ' + athlete_data[0][2],
        'athlete_gender': athlete_data[0][3],
        'country_id':athlete_data[0][4],
        'country_name':athlete_data[0][5],
        'sports': [{'sport_id': x[0], 'sport_name': x[1]} for x in {(r[6],r[7]) for r in athlete_data}],
        'olympics': [{'olympic_id': r[8], 'olympic_year': r[9]} for r in athlete_data],
        'num_medals':athlete_data[0][10],
        'athlete_top_events': top_events_list,
        'athlete_olympics': olympics_dict
        }
    
    # Close database session from SQLAlchemy
    session.close()

    # Get the rendered page
    rendered_page = render_template('athletes.html', **athlete_dict)

    assert(rendered_page is not None)

    return rendered_page

@app.route('/countries/')
def countries(): 
    """
    renders countries.html with the requested database data
    returns the rendered countries.html page
    """
    # Get a database session from SQLAlchemy
    session = db.loadSession()

    # featured countries - [{"country_id" : id, "country_name" : name, "years_hosted" : [{"olympic_id" : id, "olympic_year" : year}], 
    #                        "num_medal" : total country medals, "num_medalist" : total country medalists}] 
    featured_countries = []

    # TODO: somehow get the olympic id with the olympic years

    # all_countries - [{"country_id" : id, "country_name" : name, "years_hosted" : [{"olympic_id" : id, "olympic_year" : year}], 
    #                   "num_medal" : total country medals, "num_athlete" : total country athletes}]
    all_countries = []

    countries = session.query(db.Country.id, 
                                db.Country.name,  
                                func.array_agg(distinct(db.Olympics.year)),
                                func.count(db.Medal.id), 
                                func.count(distinct(db.Medal.athlete_id)))\
                                .select_from(db.Country)\
                                .outerjoin(db.Medal)\
                                .outerjoin(db.City)\
                                .outerjoin(db.Olympics)\
                                .group_by(db.Country.name, db.Country.id)\
                                .all()

    # pick top 3 countries for featured_countries
    while len(featured_countries) < 3:
        country = countries[randint(0, len(countries)) - 1]
        if country not in featured_countries:
            featured_countries.append({'country_id': country[0], 'country_name': country[1], 'years_hosted': {'olympic_year':y for y in country[2]}, 'num_medal': country[3], 'num_medalist':country[4]})
    
    for country in countries:
        all_countries.append({'country_id': country[0], 'country_name': country[1], 'years_hosted': {'olympic_year':y for y in country[2]}, 'num_medal': country[3], 'num_athlete':country[4]}) 

    # Close the database session from SQLAlchemy
    session.close()

    # Get the rendered page
    rendered_page = render_template('countries.html',
                            all_countries = all_countries,
                            featured_countries = featured_countries)

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
    # Get the rendered page
    rendered_page = render_template('about.html')

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

"""
main
"""
if __name__ == '__main__':
    # app.debug = True
    app.run(host='0.0.0.0', port=5005)
