from flask import Flask, render_template
import models as db

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
Scrape Country By ID
"""
@app.route('/scrape/countries/<int:country_id>')
def scrape_country_by_id(country_id):
    session = db.loadSession()
    
    country_info = session.query(db.Country.id,db.Country.name).filter(db.Country.id == country_id).first()
    
    year_info = session.query(db.Year.year).filter(db.Year.country_id == country_id).all()
    
    athlete_info = session.query(db.Athlete.id,db.Athlete.name).filter(db.Athlete.country_id == country_id).all()
    
    country_json = {'id':country_info[0],
     'name':country_info[1],
     'years-hosted':year_info,
     'origin-athletes':[x for x in ({'id':x, 'name':y} for x,y in athlete_info)]}
    
    return str(country_json)

	#str(country_info) + '<br/><br/>' + str(year_info) + '<br/><br/>' + str(athlete_info)


"""
main
"""
if __name__ == '__main__':
    # session = db.loadSession()
    # q = session.query(db.Athlete).all()
    app.run(host='0.0.0.0', port=5005)
