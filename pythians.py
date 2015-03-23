from flask import Flask, render_template, jsonify
from scrape import scrape_api
import models as db
from sqlalchemy import distinct, func, desc
from sqlalchemy.orm import aliased
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
def games(game=None):

    # random_game_banner - a random game banner
    random_game_banner = None

    # all_games - list of (host_country_banner, "city_name game_year")
    all_games = []

    session = db.loadSession()
    result = session.query(db.City.name, db.Olympics.year)\
                    .select_from(db.Olympics)\
                    .join(db.City)\
                    .all()

    for r in result:
        host_country_banner = None
        all_games += (host_country_banner, str(r[0]) + " " + str(r[1]))

    return render_template('games.html',
                            random_game_banner = random_game_banner,
                            all_games = all_games)

@app.route('/sports/')
def sports():

    featured_sports = "Sport"
    featured_events = "Event"
    featured_athletes_pic = "Athlete Portrait"
    featured_athletes_facts = None

    return render_template('sports.html', featured_sports=featured_sports,
            featured_events=featured_events,
            featured_athletes_pic=featured_athletes_pic,
            featured_athletes_facts=featured_athletes_facts)

@app.route('/athletes/')
def athletes():
    return render_template('athletes.html')

@app.route('/countries/')
def countries():
    return render_template('countries.html')

"""
main
"""
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
