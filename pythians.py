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
    return q[0].name

@app.route('/hello/')
@app.route('/hello/<name>')
def hello(name=None):
    return render_template('hello.html', name=name)

"""
main
"""
if __name__ == '__main__':
    session = db.loadSession()
    q = session.query(db.Athlete).all()
    app.run(host='0.0.0.0', port=5000)
