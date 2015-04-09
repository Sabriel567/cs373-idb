from flask import Flask, Blueprint
from flask.ext.restful import Api

from api import OlympicGamesList, IndividualOlympicGames, CountriesList, IndividualCountry, EventsList, IndividualEvent, AthletesList, IndividualAthlete, MedalsList, IndividualMedal, MedalByRankList

"""
init Flask
"""
app = Flask(__name__)

restful_api = Api(app)
restful_api.add_resource(OlympicGamesList, '/scrape/olympics', '/scrape/olympics/')
restful_api.add_resource(IndividualOlympicGames, '/scrape/olympics/<int:olympic_id>')
restful_api.add_resource(CountriesList, '/scrape/countries', '/scrape/countries/')
restful_api.add_resource(IndividualCountry, '/scrape/countries/<int:country_id>')
restful_api.add_resource(EventsList, '/scrape/events', '/scrape/events/')
restful_api.add_resource(IndividualEvent, '/scrape/events/<int:event_id>')
restful_api.add_resource(AthletesList, '/scrape/athletes', '/scrape/athletes/')
restful_api.add_resource(IndividualAthlete, '/scrape/athletes/<int:athlete_id>')
restful_api.add_resource(MedalsList, '/scrape/medals', '/scrape/medals/')
restful_api.add_resource(IndividualMedal, '/scrape/medals/<int:medal_id>')
restful_api.add_resource(MedalByRankList, '/scrape/medals/<rank>') 