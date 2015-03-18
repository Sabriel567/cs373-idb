from flask import Flask, render_template, jsonify
import models as db
from sqlalchemy import distinct
"""
init Flask
"""
app = Flask(__name__)

"""
endpoint defs
"""
@app.route('/')
def hello_world():
    # return q[0].name
    return 'Hello World!'

"""
@app.route('/hello/')
@app.route('/hello/<name>')
def hello(name=None):
    return render_template('hello.html', name=name)
"""

"""
List All Years
"""
@app.route('/scrape/years/')
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
	
	# dict.values() returns a VIEW, so, remove them from the view
	all_years_list = [d for d in all_years_dict.values()]
	
	# *****************************************************
    # NEED TO USE JSONIFY BUT FOR SOME REASON IT WON'T WORK
    # *****************************************************
	return str(all_years_list)

"""
Scrape Year By ID
"""
@app.route('/scrape/years/<int:year_id>')
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
		# distinct (because of multiple medals) has to go on the first element though we want distinct event ids
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

	# *****************************************************
	# NEED TO USE JSONIFY BUT FOR SOME REASON IT WON'T WORK
	# *****************************************************
	return str(year_dict)

"""
List All Countries
"""
@app.route('/scrape/countries/')
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
		db.Country.id, db.Country.name, db.Year.year, db.Athlete.id, db.Athlete.name)\
		.outerjoin(db.Year)\
		.outerjoin(db.Athlete)\
		.all() # Actually executes the query and returns a list of tuples
	
	# Traverse through all the rows, inserting them into a dictionary
	#	to remove the duplicate rows
	all_countries_dict=dict()
	for r in result:
		country_id		= r[0]
		country_name	= r[1]
		
		# When a country is not in the dict, make an entry with the appropriate data
		# Years has a set to remove duplicates
		if(country_id not in all_countries_dict):
			years_set		= {r[2]} if r[2] is not None else set()
			athletes_list	= [{'id':r[3] , 'name':r[4]}] if r[3] is not None else []
			
			all_countries_dict[country_id] = {
				'id':				country_id,
				'name':				country_name,
				'years':			years_set,
				'origin-athletes':	athletes_list}
			
		# Otherwise, update the existing entry
		else:
			country_dict = all_countries_dict[country_id]
			
			if(r[2] is not None):
				country_dict['years'] |= {r[2]}
			
			if(r[3] is not None):
				country_dict['origin-athletes'] += ({'id':r[3],'name':r[4]},)
	
	# Get the values from the dictionary
	all_countries_view = all_countries_dict.values()
	
	# Change all the sets to lists
	for entry in all_countries_view:
		entry.update({'years':list(entry['years'])})
	
	# dict.values() returns a VIEW, so, remove them from the view
	all_countries_list = [d for d in all_countries_view]
	
	# *****************************************************
    # NEED TO USE JSONIFY BUT FOR SOME REASON IT WON'T WORK
    # *****************************************************
	return str(all_countries_list)

"""
Scrape Country By ID
"""
@app.route('/scrape/countries/<int:country_id>')
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
	result = session.query(
		# What to select
		# outerjoin defaults to a LEFT outer join, NOT full outer join
		db.Country.id, db.Country.name, db.Year.year, db.Athlete.id, db.Athlete.name
		)\
		.select_from(db.Country)\
		.outerjoin(db.Year)\
		.outerjoin(db.Athlete)\
		.filter(
			# What to filter by (where clause)
			db.Country.id==country_id)\
		.all() # Actually executes the query and returns a list of tuples
	
	country_dict = {
					# Get name and id from tuple.
					# Both are repeated, so only need from first row
					'id':				result[0][0],
					'name':				result[0][1],
					# Grab all years from the rows, but put in a set first to
					#	get rid of duplicates
					'years-hosted':		list({r[2] for r in result if r[2] is not None}),
					# Create a list of dictionaries containing the athlete data
					'origin-athletes':	[{'id':r[3], 'name':r[4]} for r in result if r[3] is not None]}

	# *****************************************************
	# NEED TO USE JSONIFY BUT FOR SOME REASON IT WON'T WORK
	# *****************************************************
	return str(country_dict)

"""
main
"""
if __name__ == '__main__':
    # session = db.loadSession()
    # q = session.query(db.Athlete).all()
    app.run(host='0.0.0.0', port=5005)
