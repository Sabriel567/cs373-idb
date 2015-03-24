from flask import Flask, render_template, jsonify
from scrape import scrape_api
import models as db
from sqlalchemy import distinct, func, desc, and_
from sqlalchemy.sql.functions import coalesce
from sqlalchemy.orm import aliased
from random import randint

"""
init Flask
"""
app = Flask(__name__)
app.register_blueprint(scrape_api, url_prefix='/scrape')

"""
endpoint defs
"""
"""
@app.route('/')
def hello_world():
    # return q[0].name
    return 'Hello World!'
"""
"""
@app.route('/hello/')
@app.route('/hello/<name>')
def hello(name=None):
    return render_template('hello.html', name=name)
"""

@app.route('/index/')
@app.route('/home/')
@app.route('/')
def index(): 

    featured_games = "Country Year"
    featured_sports = "Sport"
    featured_countries = "Country"
    featured_athletes_pic = "Athlete Portrait"

    return render_template('index.html', featured_games=featured_games,
            featured_sports=featured_sports,
            featured_countries=featured_countries,
            featured_athletes_pic=featured_athletes_pic,
            athlete_name="Michael Phelps", athlete_country="USA",
            num_gold=0, num_silver=0, num_bronze=0)

@app.route('/games/')
def games():

    session = db.loadSession()

    # random_game_banner - a random game banner
    random_game_banner = None

    # all_games - [(host_country_banner, "city_name game_year")]
    all_games = []

    all_games_query = session.query(db.City.name, db.Olympics.year)\
                    .select_from(db.Olympics)\
                    .join(db.City)\
                    .all()

    for r in all_games_query:
        host_country_banner = None
        all_games += (host_country_banner, str(r[0]) + " " + str(r[1]))

    return render_template('games.html',
                            random_game_banner = random_game_banner,
                            all_games = all_games)

@app.route('/games/<int:game_id>')
def games_id(game_id = None):
    
    session = db.loadSession()

    # random_game_banner - a random game banner
    random_game_banner = None

    # host_country_banner - the host country banner
    host_country_banner = None

    # host_city - the hosting city
    host_city = ""

    # year - the game year
    year = ""

    # top_athletes - [("first_name last_name", "rep_country", total_g, total_s, total_b)]
    top_athletes = []

    # top_countries - [("country_name", c_total_g, c_total_s, c_total_b)]
    top_countries = []

    # all_events - [(event_id, sport_id, "name")]
    all_events = []

    # all_countries - [(country_id, "name", NOC)]
    all_countries = []

    host_query = session.query(db.City.name, db.Olympics.year)\
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

    total_athletes_g = session.query(athletes_query.c.athlete_id.label('id'), func.count(db.Medal.rank).label('num_gold'))\
                            .select_from(db.Medal)\
                            .filter(db.Medal.rank == "Gold")\
                            .filter(game_id == db.Medal.olympic_id)\
                            .join(athletes_query, db.Medal.athlete_id == athletes_query.c.athlete_id)\
                            .group_by(athletes_query.c.athlete_id)\
                            .subquery()

    total_athletes_s = session.query(athletes_query.c.athlete_id.label('id'), func.count(db.Medal.rank).label('num_silver'))\
                            .select_from(db.Medal)\
                            .filter(db.Medal.rank == "Silver")\
                            .filter(game_id == db.Medal.olympic_id)\
                            .join(athletes_query, db.Medal.athlete_id == athletes_query.c.athlete_id)\
                            .group_by(athletes_query.c.athlete_id)\
                            .subquery()

    total_athletes_b = session.query(athletes_query.c.athlete_id.label('id'), func.count(db.Medal.rank).label('num_bronze'))\
                            .select_from(db.Medal)\
                            .filter(db.Medal.rank == "Bronze")\
                            .filter(game_id == db.Medal.olympic_id)\
                            .join(athletes_query, db.Medal.athlete_id == athletes_query.c.athlete_id)\
                            .group_by(athletes_query.c.athlete_id)\
                            .subquery()

    top_athletes_query = session.query(distinct(db.Athlete.id), db.Athlete.first_name, db.Athlete.last_name, db.Country.name,
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
                                .limit(3)\
                                .all()

    for r in top_athletes_query:
        top_athletes += r[1:]

    
    countries_query = session.query(distinct(db.Country.id).label('country_id'))\
                            .select_from(db.Country)\
                            .join(db.Medal)\
                            .filter(game_id == db.Medal.olympic_id)\
                            .subquery()

    medals_query = session.query(db.Medal.event_id.label('event_id'), db.Medal.country_id.label('country_id'), 
                                    db.Medal.rank.label('rank'), db.Medal.olympic_id.label('olympic_id'))\
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

    top_countries_query = session.query(distinct(db.Country.id), db.Country.name,
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
                                .limit(3)\
                                .all()
    
    for r in top_countries_query:
        top_countries += r[1:]

    all_events = session.query(distinct(db.Event.id), db.Event.sport_id, db.Event.name)\
                    .select_from(db.Event)\
                    .join(db.Medal)\
                    .join(db.Olympics)\
                    .filter(game_id == db.Olympics.id)\
                    .all()

    all_countries = session.query(distinct(db.Country.id), db.Country.name, db.Country.noc)\
                        .select_from(db.Country)\
                        .join(db.Medal)\
                        .join(db.Olympics)\
                        .filter(game_id == db.Olympics.id)\
                        .all()

    return render_template('games.html',
                            random_game_banner = random_game_banner,
                            host_country_banner = host_country_banner,
                            host_city = host_city,
                            year = year,
                            top_athletes = top_athletes,
                            top_countries = top_countries,
                            all_events = all_events,
                            all_countries = all_countries)

@app.route('/sports/')
def sports():

    session = db.loadSession()

    # stock sports banner
    stock_sports_banner = None 

    # featured sports - [(id, "name")]
    featured_sports = [] 

    # sports - [(id, "name")]
    sports = session.query(db.Sport.id, db.Sport.name)\
                            .select_from(db.Sport)\
                            .all()
    
    while len(featured_sports) < 3:
        sport = sports[randint(0, len(sports)) - 1]
        if sport not in featured_sports:
            featured_sports.append(sport)

    return render_template('sports.html', 
                            stock_sports_banner = stock_sports_banner,
                            featured_sports = featured_sports,
                            sports = sports)

@app.route('/sports/<id>')
def sports_id(sport_id = None):

    session = db.loadSession()

    # sports banner
    sports_banner = None

    # top medalists - [("name", "results", "year")]
    top_medalists = session.query(db.Athlete.first_name, db.Athlete.last_name, func.count(db.Medal.rank))\
                            .select_from(db.Sport)\
                            .filter(db.Sport.id == sport_id)\
                            .join(db.Event)\
                            .join(db.Medal)\
                            .join(db.Athlete)\
                            .group_by(db.Athlete.id)\
                            .order_by(func.count(db.Medal.rank).desc())\
                            .all()

    return render_template('sports.html',
                            sports_banner = sports_banner,
                            top_medalists = top_medalists)

@app.route('/events/')
def events():
    
    session = db.loadSession()

    # stock events banner
    stock_events_banner = None

    # featured events - [(img, "name")]
    featured_events = []

    events = session.query(db.Event.name)\
                            .select_from(db.Event)\
                            .all()
    
    while len(featured_events) < 3:
        event = events[randint(0, len(events)) - 1]
        if (None, event) not in featured_events:
            featured_events.append((None, event))

    return render_template('events.html',
                            stock_events_banner = stock_events_banner,
                            featured_events = featured_events)

@app.route('/athletes/')
def athletes():
    return render_template('athletes.html')

@app.route('/countries/')
def countries():
    return render_template('countries.html')

@app.route('/about/')
def about():
    return render_template('about.html')

"""
main
"""
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
