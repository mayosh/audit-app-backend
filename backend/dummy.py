import flask
import json

app = flask.Flask(__name__,
static_url_path='', 
static_folder = "templates/layout",
template_folder = "templates/layout")

app.secret_key = 'cqoOyBUDkUpVsxIilDZRUcEV'

@app.route('/')
def start():
    return flask.redirect('/audit/dummy/1')

@app.route('/audit/dummy/<num>')
def dummy(num):
    file_name = 'dummy.json'
    if num == '1':
        file_name = 'dummy2.json'
    with open(file_name) as f:
        d = json.load(f)
        return flask.render_template('index_new.html', data=d, profile={ 'id': 'XXX123XXX', 'name': 'dummy account' })

