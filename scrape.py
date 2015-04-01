from flask import Blueprint, jsonify
import models as db
from sqlalchemy import distinct
from sqlalchemy.orm import aliased

scrape_api = Blueprint('scrape_api',__name__)

"""
List All Olympic Games
"""
@scrape_api.route('/olympics/', methods = ['GET'])
def scrape_all_olympics():
	"""
	Gathers all olympics from the database with their data
	return a json object representing the olympics
	"""
	
	session = db.loadSession()

	# Make the sql query
	result = session.query(
		# What to select
		# distinct (because of multiple medals) has to go on the first element though we want distinct event ids
		distinct(db.Olympics.id), db.Olympics.year, db.Olympics.season, db.City.name, db.Country.name, db.Event.id, db.Event.name, db.Sport.name
		)\
		.select_from(db.Olympics)\
		.join(db.City)\
		.join(db.Country)\
		.join(db.Medal,				db.Medal.olympic_id==db.Olympics.id)\
		.join(db.Event)\
		.join(db.Sport)\
		.all() # Actually executes the query and returns a list of tuples
	
	# Traverse through all the rows, inserting them into a dictionary
	#	to remove the duplicate rows
	all_olympics_dict=dict()
	for r in result:
		olympics_id		= r[0]
		olympics_year	= r[1]
		olympics_season	= r[2]
		olympics_city	= r[3]
		olympics_country= r[4]
		
		# When a year is not in the dict, make an entry with the appropriate data
		if(olympics_id not in all_olympics_dict):
			events_list	= [{'id':r[5], 'name':r[6], 'sport':r[7]}]
			
			all_olympics_dict[olympics_id] = {
				'id':		olympics_id,
				'year':		olympics_year,
				'season':	olympics_season,
				'city':		olympics_city,
				'country':	olympics_country,
				'events':	events_list}
			
		# Otherwise, update the existing entry
		else:
			events_dict = all_olympics_dict[olympics_id]
			events_dict['events'] += ({'id':r[5],'name':r[6],'sport':r[7]},)
	
	# Change the keys to array indexes 
	all_olympics_dict = dict(zip(range(len(all_olympics_dict)),all_olympics_dict.values()))

	return jsonify(all_olympics_dict)

"""
Scrape Olympic Games By ID
"""
@scrape_api.route('/olympics/<int:olympic_id>', methods = ['GET'])
def scrape_olympics_by_id(olympic_id):
	"""
	Gather specified olympics from the database with its data
	olympic_id a non-zero, positive int
	return a json object representing the olympic games
	"""
	session = db.loadSession()

	assert type(olympic_id) == int
	assert olympic_id > 0

	# Make the sql query
	result = session.query(
		# What to select
		# distinct (because of multiple medals per event) has to go on the first element though we want distinct event ids
		distinct(db.Olympics.id), db.Olympics.year, db.Olympics.season, db.City.name, db.Country.name, db.Event.id, db.Event.name, db.Sport.name
		)\
		.select_from(db.Olympics)\
		.join(db.City)\
		.join(db.Country)\
		.join(db.Medal,				db.Medal.olympic_id==db.Olympics.id)\
		.join(db.Event)\
		.join(db.Sport)\
		.filter(
			# What to filter by (where clause)
			db.Olympics.id==olympic_id)\
		.all() # Actually executes the query and returns a list of tuples
	
	olympics_dict= {
					# Get id, year, season, city, and country from tuple.
					# All are repeated, so only need from first row
					'id':		result[0][0],
					'year':		result[0][1],
					'season':	result[0][2],
					'city':		result[0][3],
					'country':	result[0][4],
					# Create a list of dictionaries containing the events data
					'events':	[{'id':r[5], 'name':r[6], 'sport':r[7]} for r in result]}

	return jsonify(olympics_dict)

"""
List All Countries
"""
@scrape_api.route('/countries/', methods = ['GET'])
def scrape_all_countries():
	"""
	Gathers all countries from the database with their data
	return a json object representing the countries
	"""
	
	session = db.loadSession()

	# Make the sql query
	result = session.query(
		# What to select
		# outerjoin defaults to a LEFT outer join, NOT full outer join
		db.Country.id, db.Country.name, db.Olympics.id, db.Olympics.year, db.Olympics.season, db.City.name
		)\
		.select_from(db.Country)\
		.outerjoin(db.City)\
		.outerjoin(db.Olympics)\
		.all() # Actually executes the query and returns a list of tuples
	
	# Traverse through all the rows, inserting them into a dictionary
	#	to remove the duplicate rows
	all_countries_dict=dict()
	for r in result:
		country_id		= r[0]
		country_name	= r[1]
		
		# When a country is not in the dict, make an entry with the appropriate data
		if(country_id not in all_countries_dict):
			olympics_list = [{'id':r[2], 'year':r[3], 'season':r[4], 'city':r[5]}] if r[2] is not None else []
			
			all_countries_dict[country_id] = {
				'id':				country_id,
				'name':				country_name,
				'olympics-hosted':	olympics_list}
			
		# Otherwise, update the existing entry
		else:
			country_dict = all_countries_dict[country_id]
			country_dict['olympics-hosted'] += ({'id':r[2], 'year':r[3], 'season':r[4], 'city':r[5]},)
	
	# Change the keys to array indexes 
	all_countries_dict = dict(zip(range(len(all_countries_dict)),all_countries_dict.values()))
	
	return jsonify(all_countries_dict)

"""
Scrape Country By ID
"""
@scrape_api.route('/countries/<int:country_id>', methods = ['GET'])
def scrape_country_by_id(country_id):
	"""
	Gather specified country from the database with its data
	country_id a non-zero, positive int
	return a json object representing the country
	"""
	session = db.loadSession()

	assert type(country_id) == int
	assert country_id > 0

	# Make the sql query
	result_with_years = session.query(
		# What to select
		# outerjoin defaults to a LEFT outer join, NOT full outer join
		db.Country.id, db.Country.name, db.Olympics.id, db.Olympics.year, db.Olympics.season, db.City.name
		)\
		.select_from(db.Country)\
		.outerjoin(db.City)\
		.outerjoin(db.Olympics)\
		.filter(
			# What to filter by (where clause)
			db.Country.id==country_id)\
		.all() # Actually executes the query and returns a list of tuples
	
	country_dict = {
					# Get name and id from tuple.
					# Both are repeated, so only need from first row
					'id':				result_with_years[0][0],
					'name':				result_with_years[0][1],
					# Grab all olympics data from the rows
					'olympics-hosted':	[{'id':r[2], 'year':r[3], 'season':r[4], 'city':r[5]} for r in result_with_years if r[2] is not None]}

	return jsonify(country_dict)

"""
List All Events
"""
@scrape_api.route('/events/', methods = ['GET'])
def scrape_all_events():
	"""
	Gathers all events from the database with their data
	return a json object representing the events
	"""
	
	session = db.loadSession()

	# Make the sql query
	result = session.query(
		# What to select
		# distinct because of multiple medals per event
		distinct(db.Event.id), db.Event.name, db.Sport.name, db.Olympics.id, db.Olympics.year, db.Olympics.season
		)\
		.select_from(db.Event)\
		.join(db.Sport)\
		.join(db.Medal)\
		.join(db.Olympics)\
		.all() # Actually executes the query and returns a list of tuples
	
	# Traverse through all the rows, inserting them into a dictionary
	#	to remove the duplicate rows
	all_events_dict=dict()
	for r in result:
		event_id	= r[0]
		event_name	= r[1]
		event_sport	= r[2]
		
		# When a event is not in the dict, make an entry with the appropriate data
		if(event_id not in all_events_dict):
			olympics_list	= [{'id':r[3], 'year':r[4], 'season':r[5]}] if r[3] is not None else []
			
			all_events_dict[event_id] = {
				'id':		event_id,
				'name':		event_name,
				'sport':	event_sport,
				'olympics':	olympics_list}
			
		# Otherwise, update the existing entry
		else:
			olympics_dict = all_events_dict[event_id]
			
			if(r[3] is not None):
				olympics_dict['olympics'] += ({'id':r[3], 'year':r[4], 'season':r[5]},)
	
	# Change the keys to array indexes 
	all_events_dict = dict(zip(range(len(all_events_dict)),all_events_dict.values()))
	
	return jsonify(all_events_dict)

"""
Scrape Event By ID
"""
@scrape_api.route('/events/<int:event_id>', methods = ['GET'])
def scrape_event_by_id(event_id):
	"""
	Gather specified event from the database with its data
	event_id a non-zero, positive int
	return a json object representing the event
	"""
	session = db.loadSession()

	assert type(event_id) == int
	assert event_id > 0

	# Make the sql query
	result = session.query(
		# What to select
		# distinct because of multiple medals per event
		distinct(db.Event.id), db.Event.name, db.Sport.name, db.Olympics.id, db.Olympics.year, db.Olympics.season
		)\
		.select_from(db.Event)\
		.join(db.Sport)\
		.join(db.Medal)\
		.join(db.Olympics)\
		.filter(
			# What to filter by (where clause)
			db.Event.id==event_id)\
		.all() # Actually executes the query and returns a list of tuples
	
	event_dict = {
					# Get name, id, and sport from tuple.
					# Both are repeated, so only need from first row
					'id':		result[0][0],
					'name':		result[0][1],
					'sport':	result[0][2],
					# Create a list of dictionaries containing the olympics data
					'olympics':	[{'id':r[3], 'year':r[4], 'season':r[5]} for r in result if r[3] is not None]}

	return jsonify(event_dict)

"""
List All Athletes
"""
@scrape_api.route('/athletes/', methods = ['GET'])
def scrape_all_athletes():
	"""
	Gathers all athletes from the database with their data
	return a json object representing the athletes
	"""
	
	session = db.loadSession()

	# Make the sql query
	result = session.query(
		# What to select
		db.Athlete.id, db.Athlete.first_name, db.Athlete.last_name, db.Athlete.gender, db.Medal.id, db.Medal.rank, db.Event.name, db.Olympics.year, db.Country.name
		)\
		.select_from(db.Athlete)\
		.join(db.Medal)\
		.join(db.Event)\
		.join(db.Olympics)\
		.join(db.Country)\
		.all() # Actually executes the query and returns a list of tuples
	
	# Traverse through all the rows, inserting them into a dictionary
	#	to remove the duplicate rows
	all_athletes_dict=dict()
	for r in result:
		athlete_id		= r[0]
		athlete_first	= r[1]
		athlete_last	= r[2]
		athlete_gender  = r[3]
		
		# When an athlete is not in the dict, make an entry with the appropriate data
		if(athlete_id not in all_athletes_dict):
			medals_list	= [{'id':r[4] , 'rank':r[5], 'event':r[6], 'year':r[7], 'repr':r[8]}]
			
			all_athletes_dict[athlete_id] = {
				'id':		athlete_id,
				'first':	athlete_first,
				'last':		athlete_last,
				'gender':   athlete_gender,
				'medals':	medals_list}
			
		# Otherwise, update the existing entry
		else:
			medals_dict = all_athletes_dict[athlete_id]
			medals_dict['medals'] += ({'id':r[4] , 'rank':r[5], 'event':r[6], 'year':r[7], 'repr':r[8]},)
	
	# Change the keys to array indexes 
	all_athletes_dict = dict(zip(range(len(all_athletes_dict)),all_athletes_dict.values()))
	
	return jsonify(all_athletes_dict)

"""
Scrape Athlete By ID
"""
@scrape_api.route('/athletes/<int:athlete_id>', methods = ['GET'])
def scrape_athlete_by_id(athlete_id):
	"""
	Gather specified athlete from the database with its data
	athlete_id a non-zero, positive int
	return a json object representing the athlete
	"""
	session = db.loadSession()

	assert type(athlete_id) == int
	assert athlete_id > 0
	
	# Make the sql query
	result = session.query(
		# What to select
		db.Athlete.id, db.Athlete.first_name, db.Athlete.last_name, db.Athlete.gender, db.Medal.id, db.Medal.rank, db.Event.name, db.Olympics.year, db.Country.name
		)\
		.select_from(db.Athlete)\
		.join(db.Medal)\
		.join(db.Event)\
		.join(db.Olympics)\
		.join(db.Country)\
		.filter(
			# What to filter by (where clause)
			db.Athlete.id==athlete_id)\
		.all() # Actually executes the query and returns a list of tuples
	
	athlete_dict = {
					# Get name and id from tuple.
					# All are repeated, so only need from first row
					'id':				result[0][0],
					'first':			result[0][1],
					'last':				result[0][2],
					'gender':           result[0][3],
					# Create a list of dictionaries containing the medal data
					'medals':	[{'id':r[4], 'rank':r[5], 'event':r[6], 'year':r[7], 'repr':r[8]} for r in result if r[4] is not None]
					}

	return jsonify(athlete_dict)

"""
List All Medals
"""
@scrape_api.route('/medals/', methods = ['GET'])
def scrape_all_medals():
	"""
	Gathers all medals from the database with their data
	return a json object representing the medals
	"""
	
	session = db.loadSession()

	# Make the sql query
	result = session.query(
		# What to select
		db.Medal.id, db.Medal.rank, db.Athlete.first_name + ' ' + db.Athlete.last_name, db.Event.name, db.Olympics.year, db.City.name, db.Country.name
		)\
		.select_from(db.Medal)\
		.join(db.Athlete)\
		.join(db.Event)\
		.join(db.Olympics)\
		.join(db.City)\
		.join(db.Country)\
		.all() # Actually executes the query and returns a list of tuples
	
	all_medals_dict = {k:{'id':r[0],
				  'rank':r[1],
				  'athlete':r[2],
				  'event':r[3],
				  'year':r[4],
				  'city':r[5],
				  'country':r[6]} for k,r in zip(range(len(result)),result)}
	
	return jsonify(all_medals_dict)

"""
Scrape Medal By ID
"""
@scrape_api.route('/medals/<int:medal_id>', methods = ['GET'])
def scrape_medal_by_id(medal_id):
	"""
	Gather specified medal from the database with its data
	medal_id a non-zero, positive int
	return a json object representing the medal
	"""
	session = db.loadSession()

	assert type(medal_id) == int
	assert medal_id > 0
	
	# Make the sql query
	result = session.query(
		# What to select
		db.Medal.id, db.Medal.rank, db.Athlete.first_name + ' ' + db.Athlete.last_name, db.Event.name, db.Olympics.year, db.City.name, db.Country.name
		)\
		.select_from(db.Medal)\
		.join(db.Athlete)\
		.join(db.Event)\
		.join(db.Olympics)\
		.join(db.City)\
		.join(db.Country)\
		.filter(
			# What to filter by (where clause)
			db.Medal.id==medal_id)\
		.first() # Actually executes the query and returns a tuple
	
	medal_dict = {'id':			result[0],
				  'rank':		result[1],
				  'athlete':	result[2],
				  'event':		result[3],
				  'year':		result[4],
				  'city':		result[5],
				  'country':	result[6]}

	return jsonify(medal_dict)

"""
Retrieve Medals By Rank
"""
@scrape_api.route('/medals/<rank>', methods = ['GET'])
def scrape_medals_by_rank(rank):
	"""
	Gathers all medals from the database with their data
	return a json object representing the medals
	"""
	
	rank=rank.lower()
	rank=rank.capitalize()
	
	# *******************************************
	# NEED REDIRECTION PAGE TO 404 NOT FOUND PAGE
	# THE ASSERT IS A PLACE HOLDER
	# *******************************************
	
	assert rank=='Gold' or rank=='Silver' or rank=='Bronze'
	
	session = db.loadSession()

	# Make the sql query
	result = session.query(
		# What to select
		db.Medal.id, db.Medal.rank, db.Athlete.first_name + ' ' + db.Athlete.last_name, db.Event.name, db.Olympics.year, db.City.name, db.Country.name
		)\
		.select_from(db.Medal)\
		.join(db.Athlete)\
		.join(db.Event)\
		.join(db.Olympics)\
		.join(db.City)\
		.join(db.Country)\
		.filter(
		# What to filter by (where clause)
		db.Medal.rank==rank)\
		.all() # Actually executes the query and returns a list of tuples
	

	all_medals_dict = { k:{'id':r[0],
						'rank':r[1],
						'athlete':r[2],
						'event':r[3],
						'year':r[4],
						'city':r[5],
						'country':r[6]} for k,r in zip(range(len(result)),result)}
	
	return jsonify(all_medals_dict)
 
