from flask import Flask, render_template
from calculations import calcddm,calcchinacost,calcInv
app = Flask(__name__)


@app.route('/')
def timecode():
    return render_template('index.html')

@app.route('/testddm')
def test():
    calcddm()
    return "hello"

@app.route('/testchct')
def test2():
    calcchinacost()
    return "hello"

@app.route('/testinv')
def test3():
    print('sjs')
    calcInv()
    return "hello"
