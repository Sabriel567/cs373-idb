from flask import Flask, render_template, jsonify
from scrape import scrape_api
import models as db
from sqlalchemy import distinct, func, desc, and_, case, or_
from sqlalchemy.sql.functions import coalesce
from sqlalchemy.orm import aliased
from random import randint

"""
init Flask
"""
app = Flask(__name__)
app.register_blueprint(scrape_api, url_prefix='/scrape')

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

    # countries - [{"country_id" : id, "country_name" : name, "years_hosted" : years, 
    #               "athlete_count" : count, "golds" : golds, "silvers" : silvers, "bronzes" : bronzes}]
    countries = []

    # athletes - {"id" : athlete_id, "name" : first_name last_name, "gold" : gold, "silver" : silver, "bronze" : bronze, "country" : country_name, 
    #               [{"game" : (olympic_id, "olympic city year"), "sport" : (sport_id, sport_name), "event": (event_id, event_name), "medal" : rank}]}
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
                  'athlete_count':row[3],
                  'golds':row[4],
                  'silvers':row[5],
                  'bronzes':row[6]
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
        athletes[r[1]] = {'id':r[1],
                        'name':r[0],
                        'gold':r[2],
                        'silver':r[3],
                        'bronze':r[4],
                        'country':"",
                        'year': 0,
                        'events':[]
                        }

    for e in featured_athlete_events: 
        athletes[e[0]]['events'].append({'game':(e[4],str(e[1]) + " "+ str(e[5])),
                                      'sport':(e[6], e[7]),
                                      'event':(e[8], e[9]),
                                      'medal':e[10]
                                      })
        if athletes[e[0]]['year'] <= e[5]:
            athletes[e[0]]['year'] = e[5]
            athletes[e[0]]['country'] = (e[2], e[3])

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

    # random_game_banner - a random game banner
    random_game_banner = None

    # all_games - [(host_country_banner, "city_name game_year", game_id)]
    all_games = []

    all_games_query = session.query(db.City.name, 
                                    db.Olympics.year, 
                                    db.Olympics.id)\
                    .select_from(db.Olympics)\
                    .join(db.City)\
                    .order_by(db.Olympics.year)\
                    .all()

    for r in all_games_query:
        host_country_banner = None
        all_games.append((host_country_banner, str(r[0]) + " " + str(r[1]), r[2]))

    # Close the database session from SQLAlchemy
    session.close()

    # Get the rendered page
    rendered_page = render_template('games.html',
                                    random_game_banner = random_game_banner,
                                    all_games = all_games)

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

    # random_game_banner - a random game banner
    random_game_banner = None

    # host_country_banner - the host country banner
    host_country_banner = None

    # host_city - the hosting city
    host_city = ""

    # year - the game year
    year = ""

    # top_athletes - [(athlete_id, "first_name last_name", "rep_country", total_g, total_s, total_b)]
    top_athletes = []

    # top_countries - [(country_id, "country_name", c_total_g, c_total_s, c_total_b)]
    top_countries = []

    # all_events - [(event_id, sport_id, "name")]
    all_events = []

    # all_countries - [(country_id, "name", NOC)]
    all_countries = []

    host_query = session.query(db.City.name, 
                                db.Olympics.year)\
                    .select_from(db.Olympics)\
                    .filter(game_id == db.Olympics.id)\
                    .join(db.City)\
                    .all()

    host_city = host_query[0][0]
    year = host_query[0][1]

    athletes_query = session.query(distinct(db.Athlete.id).label('athlete_id'))\
                            .select_from(db.Athlete)\
                            .join(db.Medal)\
                            .filter(game_id == db.Medal.olympic_id)\
                            .subquery()

    total_athletes_g = session.query(athletes_query.c.athlete_id.label('id'), 
                                    func.count(db.Medal.rank).label('num_gold'))\
                            .select_from(db.Medal)\
                            .filter(db.Medal.rank == "Gold")\
                            .filter(game_id == db.Medal.olympic_id)\
                            .join(athletes_query, db.Medal.athlete_id == athletes_query.c.athlete_id)\
                            .group_by(athletes_query.c.athlete_id)\
                            .subquery()

    total_athletes_s = session.query(athletes_query.c.athlete_id.label('id'), 
                                    func.count(db.Medal.rank).label('num_silver'))\
                            .select_from(db.Medal)\
                            .filter(db.Medal.rank == "Silver")\
                            .filter(game_id == db.Medal.olympic_id)\
                            .join(athletes_query, db.Medal.athlete_id == athletes_query.c.athlete_id)\
                            .group_by(athletes_query.c.athlete_id)\
                            .subquery()

    total_athletes_b = session.query(athletes_query.c.athlete_id.label('id'), 
                                    func.count(db.Medal.rank).label('num_bronze'))\
                            .select_from(db.Medal)\
                            .filter(db.Medal.rank == "Bronze")\
                            .filter(game_id == db.Medal.olympic_id)\
                            .join(athletes_query, db.Medal.athlete_id == athletes_query.c.athlete_id)\
                            .group_by(athletes_query.c.athlete_id)\
                            .subquery()

    top_athletes_query = session.query(distinct(db.Athlete.id), 
                                        db.Athlete.first_name, 
                                        db.Athlete.last_name, 
                                        db.Country.name,
                                        coalesce(total_athletes_g.c.num_gold, 0),
                                        coalesce(total_athletes_s.c.num_silver, 0),
                                        coalesce(total_athletes_b.c.num_bronze, 0))\
                                .select_from(db.Athlete)\
                                .join(db.Medal)\
                                .filter(game_id == db.Medal.olympic_id)\
                                .join(db.Country)\
                                .outerjoin(total_athletes_g, and_(db.Athlete.id == total_athletes_g.c.id))\
                                .outerjoin(total_athletes_s, and_(db.Athlete.id == total_athletes_s.c.id))\
                                .outerjoin(total_athletes_b, and_(db.Athlete.id == total_athletes_b.c.id))\
                                .order_by(coalesce(total_athletes_g.c.num_gold, 0).desc())\
                                .limit(4)\
                                .all()

    for r in top_athletes_query:
        top_athletes.append(r)

    
    countries_query = session.query(distinct(db.Country.id).label('country_id'))\
                            .select_from(db.Country)\
                            .join(db.Medal)\
                            .filter(game_id == db.Medal.olympic_id)\
                            .subquery()

    medals_query = session.query(db.Medal.event_id.label('event_id'), 
                                    db.Medal.country_id.label('country_id'), 
                                    db.Medal.rank.label('rank'), 
                                    db.Medal.olympic_id.label('olympic_id'))\
                            .select_from(db.Medal)\
                            .group_by(db.Medal.event_id, db.Medal.rank, db.Medal.country_id, db.Medal.olympic_id)\
                            .subquery()

    total_countries_g = session.query(countries_query.c.country_id.label('id'),
                                        func.count(medals_query.c.rank).label('num_gold'))\
                            .select_from(medals_query)\
                            .filter(medals_query.c.rank == "Gold")\
                            .filter(game_id == medals_query.c.olympic_id)\
                            .join(countries_query, medals_query.c.country_id == countries_query.c.country_id)\
                            .group_by(countries_query.c.country_id)\
                            .subquery()

    total_countries_s = session.query(countries_query.c.country_id.label('id'),
                                        func.count(medals_query.c.rank).label('num_silver'))\
                            .select_from(medals_query)\
                            .filter(medals_query.c.rank == "Silver")\
                            .filter(game_id == medals_query.c.olympic_id)\
                            .join(countries_query, medals_query.c.country_id == countries_query.c.country_id)\
                            .group_by(countries_query.c.country_id)\
                            .subquery()

    total_countries_b = session.query(countries_query.c.country_id.label('id'),
                                        func.count(medals_query.c.rank).label('num_bronze'))\
                            .select_from(medals_query)\
                            .filter(medals_query.c.rank == "Bronze")\
                            .filter(game_id == medals_query.c.olympic_id)\
                            .join(countries_query, medals_query.c.country_id == countries_query.c.country_id)\
                            .group_by(countries_query.c.country_id)\
                            .subquery()

    top_countries_query = session.query(distinct(db.Country.id), 
                                        db.Country.name,
                                        coalesce(total_countries_g.c.num_gold, 0),
                                        coalesce(total_countries_s.c.num_silver, 0),
                                        coalesce(total_countries_b.c.num_bronze, 0))\
                                .select_from(db.Country)\
                                .join(db.Medal)\
                                .filter(game_id == db.Medal.olympic_id)\
                                .outerjoin(total_countries_g, and_(db.Country.id == total_countries_g.c.id))\
                                .outerjoin(total_countries_s, and_(db.Country.id == total_countries_s.c.id))\
                                .outerjoin(total_countries_b, and_(db.Country.id == total_countries_b.c.id))\
                                .order_by(coalesce(total_countries_g.c.num_gold, 0).desc())\
                                .limit(4)\
                                .all()
    
    for r in top_countries_query:
        top_countries.append(r)

    all_events = session.query(distinct(db.Event.id), 
                                db.Event.sport_id, 
                                db.Event.name)\
                    .select_from(db.Event)\
                    .join(db.Medal)\
                    .join(db.Olympics)\
                    .filter(game_id == db.Olympics.id)\
                    .all()

    all_countries = session.query(distinct(db.Country.id), 
                                    db.Country.name, 
                                    db.Country.noc)\
                        .select_from(db.Country)\
                        .join(db.Medal)\
                        .join(db.Olympics)\
                        .filter(game_id == db.Olympics.id)\
                        .all()

    # Close the database session from SQLAlchemy
    session.close()

    # Get the rendered page
    rendered_page = render_template('games.html',
                                    random_game_banner = random_game_banner,
                                    host_country_banner = host_country_banner,
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

    # stock sports banner
    stock_sports_banner = None 

    # featured sports - [(id, "name")]
    featured_sports = [] 

    # sports - [(id, "name")]
    sports = session.query(db.Sport.id, 
                            db.Sport.name)\
                            .select_from(db.Sport)\
                            .all()
    
    # pick 3 random sports for featured_sports
    while len(featured_sports) < 3:
        sport = sports[randint(0, len(sports)) - 1]
        if sport not in featured_sports:
            featured_sports.append(sport)

    # Close the database session from SQLAlchemy
    session.close()

    # Get the rendered page
    rendered_page = render_template('sports.html',
                                    stock_sports_banner = stock_sports_banner,
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

    # sports banner
    sports_banner = None

    # top medalists - [(athlete_id, "name", "results", "year")]
    top_medalists = session.query(db.Athlete.id, 
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

    # Close the database session from SQLAlchemy
    session.close()


    # Get the rendered page
    rendered_page = render_template('sports.html',
                                    sports_banner = sports_banner,
                                    top_medalists = top_medalists)

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

    # stock events banner
    stock_events_banner = None

    # events - [(img, event_id, "name")]
    all_events = []

    # featured events - [(img, event_id, "name")]
    featured_events = []

    events = session.query(db.Event.id, db.Event.name)\
                            .select_from(db.Event)\
                            .all()
    
    """
    # featured_events - pick 3 random events
    while len(featured_events) < 3:
        event = events[randint(0, len(events)) - 1]
        if tuple([None] + list(event)) not in featured_events:
            featured_events.append(tuple([None] + list(event)))
    """

    all_events = [(None,) + event for event in events]

    # Close the database session from SQLAlchemy
    session.close()
    
    # Get the rendered page
    rendered_page = render_template('events.html',
                                    stock_events_banner = stock_events_banner,
                                    featured_events = all_events)

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

    # stock events banner
    stock_events_banner = None
    
    # medalists [(game_id, "city + year", (gold_athlete_id, gold athlete photo, "name"), 
    #                                     (silver_athlete_id, silver athlete photo, "name"),
    #                                     (bronze_athlete_id, bronze athlete photo, "name"))]
    medalists = []

    gold_medalists = session.query(db.Athlete.id.label("gold_id"),
                                    db.Athlete.first_name.label("first_name"),
                                    db.Athlete.last_name.label("last_name"),
                                    db.Medal.olympic_id.label("olympic_id"))\
                                    .select_from(db.Medal)\
                                    .filter(db.Medal.event_id == event_id)\
                                    .filter(db.Medal.rank == "Gold")\
                                    .join(db.Athlete)\
                                    .subquery()
    
    silver_medalists = session.query(db.Athlete.id.label("silver_id"),
                                    db.Athlete.first_name.label("first_name"),
                                    db.Athlete.last_name.label("last_name"),
                                    db.Medal.olympic_id.label("olympic_id"))\
                                    .select_from(db.Medal)\
                                    .filter(db.Medal.event_id == event_id)\
                                    .filter(db.Medal.rank == "Silver")\
                                    .join(db.Athlete)\
                                    .subquery()

    bronze_medalists = session.query(db.Athlete.id.label("bronze_id"),
                                    db.Athlete.first_name.label("first_name"),
                                    db.Athlete.last_name.label("last_name"),
                                    db.Medal.olympic_id.label("olympic_id"))\
                                    .select_from(db.Medal)\
                                    .filter(db.Medal.event_id == event_id)\
                                    .filter(db.Medal.rank == "Bronze")\
                                    .join(db.Athlete)\
                                    .subquery()

    medalists_query = session.query(db.Olympics.id, 
                                    db.City.name, 
                                    db.Olympics.year, 
                                    gold_medalists.c.gold_id,
                                    gold_medalists.c.first_name + " " + gold_medalists.c.last_name,
                                    silver_medalists.c.silver_id,
                                    silver_medalists.c.first_name + " " + silver_medalists.c.last_name,
                                    bronze_medalists.c.bronze_id,
                                    bronze_medalists.c.first_name + " " + bronze_medalists.c.last_name)\
                                    .select_from(db.City)\
                                    .join(db.Olympics)\
                                    .join(gold_medalists)\
                                    .join(silver_medalists)\
                                    .join(bronze_medalists)\
                                    .all()

    for game in medalists_query:
        medalists.append((game[0], game[1] + " " + str(game[2]), 
                            (game[3], None, game[4]),
                            (game[5], None, game[6]),
                            (game[7], None, game[8])))

    # Close the database session from SQLAlchemy 
    session.close()

    # Get the rendered page
    rendered_page = render_template('events.html',
                            stock_events_banner = stock_events_banner,
                            medalists = medalists)

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

    # {athlete_id : {"id" : athlete_id, "name" : athlete_name, "country_id" : id, "country" : name, 
    #                   "sports" : [(sport_id, sport_name)], "years" : [(olympic_id, year)], "total_medals" : total}}
    all_athletes_dict = dict()

    result = session.query(
                db.Athlete.id,
                db.Athlete.first_name + ' ' + db.Athlete.last_name,
                db.Country.id,
                db.Country.name,
                db.Sport.id,
                db.Sport.name,
                db.Olympics.id,
                db.Olympics.year,
                func.count(db.Medal.id).label('total_medals'))\
            .select_from(db.Athlete).join(db.Medal)\
            .join(db.Country)\
            .join(db.Event)\
            .join(db.Sport)\
            .join(db.Olympics)\
            .group_by(db.Athlete.id,
                db.Athlete.first_name + ' ' + db.Athlete.last_name,
                db.Country.id,
                db.Country.name,
                db.Sport.id,
                db.Sport.name,
                db.Olympics.id,
                db.Olympics.year,)\
            .all()
    
    # Make an entry for every athlete in a dictionary and
    #   update their data when their row repeats
    for row in result:
        athlete_id = row[0]

        if athlete_id not in all_athletes_dict:
            all_athletes_dict[athlete_id] = {
                'id':athlete_id,
                'name':row[1],
                'country_id': row[2],
                'country': row[3],
                'sports':[(row[4], row[5])],
                'years':[(row[6],row[7])],
                'total_medals':row[8],
                'latest_year':row[7]}
        else:
            athlete = all_athletes_dict[athlete_id]
            
            if athlete['latest_year'] >= row[7]:
                athlete['latest_year'] = row[7]
                athlete['country_id'] = row[2]
                athlete['country'] = row[3]
                
            athlete['sports'] += [(row[4],row[5])]
            athlete['years'] += [(row[6],row[7])]

    # Close the database session from SQLAlchemy
    session.close()

    # Get the rendered page
    rendered_page = render_template('athletes.html', athletes=all_athletes_dict)

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

    # top events list - [{"sport_id" : id, "sport" : name, "event_id" : id, "event" : name, "gold" : golds, "silver" : silvers, "bronze" : bronzes}] 
    top_events_list = []

    # olympics dict - {olympic_year : [{"olympic_id" : id, "country_id" : id, "country" : name, 
    #                   "event_id" : id, "event" : name, "sport_id" : id, "sport" : name, "medal" : rank}]}
    olympics_dict = dict()


    # athletes dict - {"id" : id, "name" : name, "gender" : gender, "country_repr_id" : id, "country_repr" : name, "sports" : [(sport_id, sport_name)], 
    #                   "years" : [(olympic_id, year)], "total_medals" : total, "top_events" : top_events_list, "games_part" : olympics_dict}
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
        'sport': r[3],
        'event_id':r[0],
        'event': r[1],
        'gold': r[4],
        'silver': r[5],
        'bronze': r[6]
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
                'country': row[3],
                'event_id': row[4],
                'event': row[5],
                'sport_id': row[6],
                'sport': row[7],
                'medal': row[8]}]
        else:
            olympics_dict[olympic_year] += ({
                'olympic_id': row[0],
                'country_id': row[2],
                'country': row[3],
                'event_id': row[4],
                'event': row[5],
                'sport_id': row[6],
                'sport': row[7],
                'medal': row[8]},)
    
    athlete_dict = {
        'id': athlete_data[0][0],
        'name': athlete_data[0][1] + ' ' + athlete_data[0][2],
        'gender': athlete_data[0][3],
        'country_repr_id':athlete_data[0][4],
        'country_repr':athlete_data[0][5],
        'sports': list({(r[6],r[7]) for r in athlete_data}),
        'years': [(r[8],r[9]) for r in athlete_data],
        'total_medals':athlete_data[0][10],
        'top_events': top_events_list,
        'games_part': olympics_dict
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

    # stock global banner
    stock_global_banner = None

    # featured countries - [(id, "country name", ["years hosted"], total_medals, num_medalists)] 
    featured_countries = []

    # all_countries - [(id, "name")]
    all_countries = []

    countries = session.query(db.Country.id, 
                                db.Country.name,  
                                func.array_agg(distinct(db.Olympics.year)),
                                func.count(db.Medal.id), 
                                func.count(distinct(db.Medal.athlete_id)))\
                                .select_from(db.Country)\
                                .join(db.City)\
                                .join(db.Olympics)\
                                .join(db.Medal)\
                                .group_by(db.Country.name, db.Country.id)\
                                .all()

    # pick top 3 countries for featured_countries
    while len(featured_countries) < 3:
        country = countries[randint(0, len(countries)) - 1]
        if country not in featured_countries:
            featured_countries.append(country)
    
    for country in countries:
        all_countries.append((country[0], country[1])) 

    # Close the database session from SQLAlchemy
    session.close()

    # Get the rendered page
    rendered_page = render_template('countries.html',
                            stock_global_banner = stock_global_banner,
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

    # country banner
    country_banner = None

    # country name
    country_name = session.query(db.Country.name)\
                            .select_from(db.Country)\
                            .filter(db.Country.id == country_id)\
                            .all()

    # total gold medals
    total_gold_medals = session.query(func.sum(case([(db.Medal.rank == 'Gold', 1)], else_=0)), 
                                        func.count(db.Medal.id))\
                                .select_from(db.Country)\
                                .filter(db.Country.id == country_id)\
                                .join(db.Medal)\
                                .all()

    # total medals overall
    total_medals = total_gold_medals[0][1]

    # total athletes
    total_athletes = session.query(func.count(distinct(db.Medal.athlete_id)))\
                            .select_from(db.Medal)\
                            .filter(db.Medal.country_id == country_id)\
                            .all()

    # years hosted = [(game_id, year)]
    years_hosted = session.query(db.Olympics.id, 
                                    db.Olympics.year)\
                            .select_from(db.Country)\
                            .filter(db.Country.id == country_id)\
                            .join(db.City)\
                            .join(db.Olympics)\
                            .all()
    
    # top medalists - [(id, "first_name", "last_name", "gender")]
    top_medalists = session.query(db.Athlete.id, 
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

    # top years - [(game_id, year, total medals, [(athlete_id, "first_name last_name", num_gold, num_silver, num_bronze, num_medals)])]
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
    
    top_years = []
    for year in top_years_unpacked:
        top_years.append(tuple([year[0]] + year[1]))

    # top events - [(event_id, "event_name", total_medals)]
    # frequently has fewer than 3 events in test database
    top_events = session.query(db.Event.id, 
                                db.Event.name, 
                                func.count(db.Medal.id))\
                        .select_from(db.Medal)\
                        .filter(db.Medal.country_id == country_id)\
                        .join(db.Event)\
                        .order_by(func.count(db.Medal.id).desc())\
                        .group_by(db.Event.id)\
                        .limit(4)\
                        .all()

    # Close the database session from SQLAlchemy
    session.close()

    # Get the rendered page
    rendered_page = render_template("countries.html",
                            country_banner = country_banner,
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
    app.run(host='0.0.0.0', port=5000)
