from flask import Blueprint, jsonify
import models as db
from sqlalchemy import distinct
from sqlalchemy.orm import aliased

scrape_api = Blueprint('scrape_api',__name__)

@scrape_api.errorhandler(404)
def not_found(error=None):
    message = {
            'status': 404,
            'message': 'Not Found: ' + request.url,
    }
	
    resp = jsonify(message)
    resp.status_code = 404

    return resp

"""
List All Years
"""
@scrape_api.route('/years/', methods = ['GET'])
def scrape_all_years():
	"""
	Gathers all years from the database with their data
	return a json object representing the years
	"""
	
	session = db.loadSession()

	# Make the sql query
	result = session.query(
		# What to select
		# distinct (because of multiple medals) has to go on the first element though we want distinct event ids
		# outerjoin defaults to a LEFT outer join, NOT full outer join
		distinct(db.Year.id), db.Year.year, db.Year.type, db.Country.name, db.Event.id, db.Event.name
		)\
		.select_from(db.Year)\
		.outerjoin(db.Country)\
		.outerjoin(db.Medal)\
		.outerjoin(db.Event)\
		.all() # Actually executes the query and returns a list of tuples
	
	# Traverse through all the rows, inserting them into a dictionary
	#	to remove the duplicate rows
	all_years_dict=dict()
	for r in result:
		year_id		= r[0]
		year_year	= r[1]
		year_type	= r[2]
		year_host	= r[3]
		
		# When a year is not in the dict, make an entry with the appropriate data
		if(year_id not in all_years_dict):
			events_list	= [{'id':r[4] , 'name':r[5]}] if r[4] is not None else []
			
			all_years_dict[year_id] = {
				'id':		year_id,
				'year':		year_year,
				'type':		year_type,
				'host':		year_host,
				'events':	events_list}
			
		# Otherwise, update the existing entry
		else:
			country_dict = all_years_dict[year_id]
			
			if(r[4] is not None):
				country_dict['events'] += ({'id':r[4],'name':r[5]},)
	
	# Change the keys to array indexes 
	all_years_dict = dict(zip(range(len(all_years_dict)),all_years_dict.values()))

	return jsonify(all_years_dict)

"""
Scrape Year By ID
"""
@scrape_api.route('/years/<int:year_id>', methods = ['GET'])
def scrape_year_by_id(year_id):
	"""
	Gather specified year from the database with its data
	year_id a non-zero, positive int
	return a json object representing the year
	"""
	session = db.loadSession()

	assert type(year_id) == int
	assert year_id > 0

	# Make the sql query
	result = session.query(
		# What to select
		# distinct (because of multiple medals per event) has to go on the first element though we want distinct event ids
		# outerjoin defaults to a LEFT outer join, NOT full outer join
		distinct(db.Year.id), db.Year.year, db.Year.type, db.Country.name, db.Event.id, db.Event.name
		)\
		.select_from(db.Year)\
		.outerjoin(db.Country)\
		.outerjoin(db.Medal)\
		.outerjoin(db.Event)\
		.filter(
			# What to filter by (where clause)
			db.Year.id==year_id)\
		.all() # Actually executes the query and returns a list of tuples
	
	year_dict= {
					# Get id, year, type, and host from tuple.
					# All are repeated, so only need from first row
					'id':		result[0][0],
					'year':		result[0][1],
					'type':		result[0][2],
					'host':		result[0][3],
					# Create a list of dictionaries containing the events data
					'events':	[{'id':r[4], 'name':r[5]} for r in result if r[4] is not None]}

	return jsonify(year_dict)

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
	result_with_years = session.query(
		# What to select
		# outerjoin defaults to a LEFT outer join, NOT full outer join
		db.Country.id, db.Country.name, db.Year.id, db.Year.year
		)\
		.select_from(db.Country)\
		.outerjoin(db.Year)\
		.all() # Actually executes the query and returns a list of tuples
	
	# Make the next sql query
	result_with_athletes = session.query(
		# What to select
		db.Country.id, db.Athlete.id, db.Athlete.name
		)\
		.select_from(db.Country)\
		.join(db.Athlete)\
		.all() # Actually executes the query and returns a list of tuples
	
	# Traverse through all the rows, inserting them into a dictionary
	#	to remove the duplicate rows
	all_countries_dict=dict()
	for r in result_with_years:
		country_id		= r[0]
		country_name	= r[1]
		
		# When a country is not in the dict, make an entry with the appropriate data
		# Years has a set to remove duplicates
		if(country_id not in all_countries_dict):
			years_list = [{'id':r[2], 'year':r[3]}] if r[2] is not None else []
			
			all_countries_dict[country_id] = {
				'id':				country_id,
				'name':				country_name,
				'years':			years_list,
				'origin-athletes':	[]}
			
		# Otherwise, update the existing entry
		else:
			country_dict = all_countries_dict[country_id]
			
			if(r[2] is not None):
				country_dict['years'] += (r[2],)
	
	# Traverse through all the rows, adding every athlete found to the appropriate
	#	countries athlete list
	for r in result_with_athletes:
		country_id = r[0]
		all_countries_dict[country_id]['origin-athletes'] += [{'id':r[1] , 'name':r[2]}]
	
	# dict.values() returns a VIEW, so, remove them from the view
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
		db.Country.id, db.Country.name, db.Year.id, db.Year.year
		)\
		.select_from(db.Country)\
		.outerjoin(db.Year)\
		.filter(
			# What to filter by (where clause)
			db.Country.id==country_id)\
		.all() # Actually executes the query and returns a list of tuples
	
	# Make the next sql query
	result_with_athletes = session.query(
		# What to select
		# outerjoin defaults to a LEFT outer join, NOT full outer join
		db.Country.id, db.Athlete.id, db.Athlete.name
		)\
		.select_from(db.Country)\
		.outerjoin(db.Athlete)\
		.filter(
			# What to filter by (where clause)
			db.Country.id==country_id)\
		.all() # Actually executes the query and returns a list of tuples
	
	country_dict = {
					# Get name and id from tuple.
					# Both are repeated, so only need from first row
					'id':				result_with_years[0][0],
					'name':				result_with_years[0][1],
					# Grab all years from the rows
					'years-hosted':		[{'id':r[2], 'year':r[3]} for r in result_with_years if r[2] is not None],
					# Create a list of dictionaries containing the athlete data
					'origin-athletes':	[{'id':r[1], 'name':r[2]} for r in result_with_athletes if r[1] is not None]}

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
		# distinct (because of multiple medals) has to go on the first element though we want distinct event ids
		# outerjoin defaults to a LEFT outer join, NOT full outer join
		distinct(db.Event.id), db.Event.name, db.Year.id, db.Year.year
		)\
		.select_from(db.Event)\
		.outerjoin(db.Medal)\
		.outerjoin(db.Year)\
		.all() # Actually executes the query and returns a list of tuples
	
	# Traverse through all the rows, inserting them into a dictionary
	#	to remove the duplicate rows
	all_events_dict=dict()
	for r in result:
		event_id	= r[0]
		event_name	= r[1]
		
		# When a event is not in the dict, make an entry with the appropriate data
		if(event_id not in all_events_dict):
			years_list	= [{'id':r[2] , 'name':r[3]}] if r[2] is not None else []
			
			all_events_dict[event_id] = {
				'id':		event_id,
				'name':		event_name,
				'years':	years_list}
			
		# Otherwise, update the existing entry
		else:
			year_dict = all_events_dict[event_id]
			
			if(r[2] is not None):
				year_dict['years'] += ({'id':r[2],'name':r[3]},)
	
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
		# distinct (because of multiple medals per event) has to go on the first element though we want distinct event ids
		# outerjoin defaults to a LEFT outer join, NOT full outer join
		distinct(db.Event.id), db.Event.name, db.Year.id, db.Year.year
		)\
		.select_from(db.Event)\
		.outerjoin(db.Medal)\
		.outerjoin(db.Year)\
		.filter(
			# What to filter by (where clause)
			db.Event.id==event_id)\
		.all() # Actually executes the query and returns a list of tuples
	
	event_dict = {
					# Get name and id from tuple.
					# Both are repeated, so only need from first row
					'id':				result[0][0],
					'name':				result[0][1],
					# Create a list of dictionaries containing the year data
					'years':	[{'id':r[2], 'name':r[3]} for r in result if r[2] is not None]}

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

	origin_country	= aliased(db.Country)
	repr_country	= aliased(db.Country)

	# Make the sql query
	result = session.query(
		# What to select
		db.Athlete.id, db.Athlete.name, origin_country.name, db.Medal.id, db.Medal.rank, db.Event.name, db.Year.year, repr_country.name
		)\
		.select_from(db.Athlete)\
		.join(origin_country)\
		.join(db.Medal)\
		.join(db.Event)\
		.join(db.Year, 				db.Year.id==db.Medal.year_id)\
		.join(db.Year_Representing,	db.Athlete.id==db.Year_Representing.athlete_id)\
		.join(repr_country,			db.Year_Representing.country_id==repr_country.id)\
		.all() # Actually executes the query and returns a list of tuples
	
	# Traverse through all the rows, inserting them into a dictionary
	#	to remove the duplicate rows
	all_athletes_dict=dict()
	for r in result:
		athlete_id		= r[0]
		athlete_name	= r[1]
		athlete_origin	= r[2]
		
		# When an athlete is not in the dict, make an entry with the appropriate data
		if(athlete_id not in all_athletes_dict):
			medals_list	= [{'id':r[3] , 'rank':r[4], 'event':r[5], 'year':r[6], 'repr':r[7]}] if r[3] is not None else []
			
			all_athletes_dict[athlete_id] = {
				'id':		athlete_id,
				'name':		athlete_name,
				'medals':	medals_list}
			
		# Otherwise, update the existing entry
		else:
			medals_dict = all_athletes_dict[athlete_id]
			
			if(r[3] is not None):
				medals_dict['medals'] += ({'id':r[3] , 'rank':r[4], 'event':r[5], 'year':r[6], 'repr':r[7]},)
	
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
	
	origin_country	= aliased(db.Country)
	repr_country	= aliased(db.Country)
	
	# Make the sql query
	result = session.query(
		# What to select
		db.Athlete.id, db.Athlete.name, origin_country.name, db.Medal.id, db.Medal.rank, db.Event.name, db.Year.year, repr_country.name
		)\
		.select_from(db.Athlete)\
		.join(origin_country)\
		.join(db.Medal)\
		.join(db.Event)\
		.join(db.Year, 				db.Year.id==db.Medal.year_id)\
		.join(db.Year_Representing,	db.Athlete.id==db.Year_Representing.athlete_id)\
		.join(repr_country,			db.Year_Representing.country_id==repr_country.id)\
		.filter(
			# What to filter by (where clause)
			db.Athlete.id==athlete_id)\
		.all() # Actually executes the query and returns a list of tuples
	
	athlete_dict = {
					# Get name, id, and origin-country from tuple.
					# All are repeated, so only need from first row
					'id':				result[0][0],
					'name':				result[0][1],
					'origin':			result[0][2],
					# Create a list of dictionaries containing the medal data
					'medals':	[{'id':r[3], 'rank':r[4], 'event':r[5], 'year':r[6], 'repr':r[7]} for r in result if r[3] is not None]}

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
		db.Medal.id, db.Medal.rank, db.Athlete.name, db.Event.name, db.Year.year, db.Country.name
		)\
		.select_from(db.Medal)\
		.join(db.Athlete)\
		.join(db.Event)\
		.join(db.Year)\
		.join(db.Country)\
		.all() # Actually executes the query and returns a list of tuples
	
	all_medals_dict = {k:{'id':r[0],
				  'rank':r[1],
				  'athlete':r[2],
				  'event':r[3],
				  'year':r[4],
				  'host':r[5]} for k,r in zip(range(len(result)),result)}
	
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
		db.Medal.id, db.Medal.rank, db.Athlete.name, db.Event.name, db.Year.year, db.Country.name
		)\
		.select_from(db.Medal)\
		.join(db.Athlete)\
		.join(db.Event)\
		.join(db.Year)\
		.join(db.Country)\
		.filter(
			# What to filter by (where clause)
			db.Medal.id==medal_id)\
		.first() # Actually executes the query and returns a tuple
	
	medal_dict = {'id':result[0],
				  'rank':result[1],
				  'athlete':result[2],
				  'event':result[3],
				  'year':result[4],
				  'host':result[5]}

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
		db.Medal.id, db.Medal.rank, db.Athlete.name, db.Event.name, db.Year.year, db.Country.name
		)\
		.select_from(db.Medal)\
		.join(db.Athlete)\
		.join(db.Event)\
		.join(db.Year)\
		.join(db.Country)\
		.filter(
		# What to filter by (where clause)
		db.Medal.rank==rank)\
		.all() # Actually executes the query and returns a list of tuples
	

	all_medals_dict = {k:{'id':r[0],
				  'rank':r[1],
				  'athlete':r[2],
				  'event':r[3],
				  'year':r[4],
				  'host':r[5]} for k,r in zip(range(len(result)),result)}
	
	return jsonify(all_medals_dict)
 
