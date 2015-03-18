from flask import Flask, render_template, jsonify
import models as db
from scrape import scrape_api

"""
init Flask
"""
app = Flask(__name__)
app.register_blueprint(scrape_api)

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
main
"""
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
