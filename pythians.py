from flask import Flask, render_template, jsonify
import models as db
from scrape import scrape_api

"""
init Flask
"""
app = Flask(__name__)
app.register_blueprint(scrape_api, url_prefix='/scrape')

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

@app.route('/index/') 
def index(name=None, featured_games=None, featured_countries=None, 
        featured_athletes_pic=None, featured_athletes_facts=None): 
    return render_template('index.html', featured_games=featured_games, 
            featured_countries=featured_countries, featured_athletes_pic=featured_athletes_pic, 
            featured_athletes_facts=featured_athletes_facts)

"""
main
"""
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
