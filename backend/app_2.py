import google.oauth2.credentials
import google_auth_oauthlib.flow

from googleads import adwords
from googleads import oauth2
from google.auth.transport.requests import Request

import googleapiclient.discovery

import flask
import os
import requests
import sys
from io import StringIO
import csv
import datetime
import pickle
import re
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from flask.logging import default_handler
import functools
import inspect
import json


import firebase_admin
from firebase_admin import firestore
from firebase_admin import credentials as firebase_module_credentials


def defauls_period():
    day1 = datetime.datetime.today() - datetime.timedelta(days = 1)
    date90 = day1 - datetime.timedelta(days = 89)
    return {
        'end_date': day1.strftime('%Y%m%d'),
        'start_date': date90.strftime('%Y%m%d')
    }

CLIENT_SECRETS_FILE = 'creds/client-creds.json'
SHEET_SECRETS_FILE = 'creds/sheet_creds.json'
developer_token = 'kRqQZviBtOhZFDatpjTLmw'
sheets_token = '1/M2iYW2N8Evip-ReUMK7Xix6jM6JYmVwZMPLUWSIxUFLbD9jO06plCyRUTZcjCdmx'
PAGE_SIZE = 100
CHANGE_LIMIT = 10
DEFAULT_PERFOMANCE_PERIOD = defauls_period() #'LAST_30_DAYS'
FOLDER_ID = '1SfANYC_jULpbJDXwYz0sEG81ItQWN9aW'

FIRABASE_CRED_FILE = 'creds/firebase-key.json'
firebase_credentials = firebase_module_credentials.Certificate(FIRABASE_CRED_FILE)
firebase_app = firebase_admin.initialize_app(credential=firebase_credentials)

db = firestore.client()

app = flask.Flask(__name__,
static_url_path='', 
static_folder = "templates/layout",
template_folder = "templates/layout")

app.secret_key = 'cqoOyBUDkUpVsxIilDZRUcEV'

async def asyncator(loop, func, *args, **kwargs):
    parted = functools.partial(func, **kwargs)
    result = await loop.run_in_executor(None, parted, *args)
    return result

@app.route('/')
def hello_world():
    app.logger.info('app started successfully')
    return flask.render_template('start_some.html')

    if not app.debug:
        @app.route('/', defaults={'path': ''})
        @app.route('/<path:path>')
        def catch_all(path):
            return flask.render_template("start_some.html")


def getAuthUrl(flask_session, local_proxy=False, redirect='oauth_callback'):
    m_scopes=[oauth2.GetAPIScope('adwords'),
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/userinfo.profile']
    flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE, scopes=m_scopes)

    flow.redirect_uri = flask.request.url_root + redirect
    if app.debug:
        flow.redirect_uri = 'http://localhost:5000/' + redirect  # flask.url_for('oauth2callback')
    if local_proxy:
        flow.redirect_uri = 'http://localhost:8080/oauth-callback'

    authorization_url, state = flow.authorization_url(
          access_type='offline',
          include_granted_scopes='true',  prompt='consent')
    # Store the state so the callback can verify the auth server response.
    flask_session['state'] = state
    flask_session['fl_config'] = flow.client_config
    flask_session['fl_client_type'] = flow.client_type
    flask_session['fl_code_verifier'] = flow.code_verifier
    return authorization_url

def get_profile(clientId):
    adwords_client = get_adwords_client()
    adwords_client.SetClientCustomerId(clientId)
    customer_service = adwords_client.GetService('CustomerService', version='v201809')
    account = customer_service.getCustomers()


    return { 'id': account[0]['customerId'], 'name': account[0]['descriptiveName'] }

@app.route('/oauth_callback')
def oauth_redirected_callback():
    if app.debug:
        print ('hello from oauth2callback. setting oauth_insecure to 1')
        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    if flask.request.url.startswith('http://'):
        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'


    cb_scopes=[oauth2.GetAPIScope('adwords'),
        'https://www.googleapis.com/auth/userinfo.email',
        'https://www.googleapis.com/auth/userinfo.profile']
    app.logger.info( f"session contains {flask.session.keys()}" ) #fl_config
    conf = {'web': flask.session['fl_config']}
    redirect = 'oauth_callback'

    fsession, client_config = (
            google_auth_oauthlib.helpers.session_from_client_config(conf, None))

    flow = google_auth_oauthlib.flow.Flow(fsession, flask.session['fl_client_type'], client_config,
            redirect_uri=None, code_verifier=flask.session['fl_code_verifier'])

    flow.redirect_uri = flask.request.url_root + redirect

    if app.debug:
        flow.redirect_uri = 'http://localhost:5000/' + redirect  # flask.url_for('oauth2callback')
        app.logger.info(f" got url as {flow.redirect_uri}")

    # Use the authorization server's response to fetch the OAuth 2.0 tokens.
    try:
        flow.fetch_token(code=flask.request.args.get('code', ''))
        # pass
    except:
        print("Unexpected error:", sys.exc_info()[0])
        app.logger.error('oauth is broken', exc_info=True)
        app.logger.warning(sys.exc_info()[0])
        raise InvalidUsage('no code', status_code=410)


    # Store credentials in the session.
    # ACTION ITEM: In a production app, you likely want to save these
    #              credentials in a persistent database instead.
    credentials = flow.credentials
    flask.session['credentials'] = credentials_to_dict(credentials)
    print ('credentials from flow', credentials.refresh_token)

    oauth2info = googleapiclient.discovery.build(
      'oauth2', 'v2', credentials=credentials)
    try:
      info = oauth2info.userinfo().get().execute()
    except googleapiclient.errors.HttpError:
      return 'googleapiclient.errors.HttpError', 400

    flask.session['user'] = {
        'gid': info['id'],
        'name': info['name'],
        'email': info['email']
        }

    return flask.redirect('/select_account')

@app.route('/select_account')
def select_ads_account():
    adwords_client = get_adwords_client()
    customer_service = adwords_client.GetService('CustomerService', version='v201809')
    offset = 0
    selector = {
        'fields': ['customerId', 'descriptiveName', 'canManageClients'],
        'paging': {
            'startIndex': str(offset),
            'numberResults': str(PAGE_SIZE)
        }
    }
    try:
        accounts = customer_service.getCustomers(selector)
    except Exception as inst:
        # print(type(inst))    # the exception instance
        print(inst.args[0])     # arguments stored in .args
        # print(inst)          # __str__ allows args to be printed directly,
        if 'NOT_ADS' in inst.args[0]:
            return flask.jsonify({'status':'no_ads'})
        else:
            return flask.abort(404)

    render_list = []
    for customer in accounts:
        listed_account = {
            'name': customer['descriptiveName'],
            'id': customer['customerId'],
            'canManageClients': customer['canManageClients']
        }

        if customer['canManageClients']:
            listed_account['child'] = []
            adwords_managed_client = get_adwords_client()
            adwords_managed_client.SetClientCustomerId(customer['customerId'])
            managed_customer_service = adwords_managed_client.GetService('ManagedCustomerService', version='v201809')
            man_selector = {
                'fields': ['CustomerId', 'Name', 'CanManageClients'],
                'paging': {
                    'startIndex': str(offset),
                    'numberResults': str(PAGE_SIZE)
                }
            }
            page = managed_customer_service.get(man_selector)
            if 'entries' in page:
                for managed_account in (managed for managed in page['entries'] if managed['customerId'] != customer['customerId']) :
                     listed_account['child'].append({
                        'name' : managed_account['name'],
                        'id' : managed_account['customerId'],
                        'canManageClients': managed_account['canManageClients']
                    })
        render_list.append(listed_account)

    return flask.render_template('select_new.html',
        result=json.dumps(render_list, sort_keys = False, indent = 2),
        data = render_list,
        user = flask.session['user'] ) #render_list


import checks

@app.route('/start_audit')
def auth_redirect():
    flask.session.permanent = True
    app.permanent_session_lifetime = datetime.timedelta(days=1)
    auth_url = getAuthUrl(flask.session)
    app.logger.info(f"auth_url is {auth_url}")
    app.logger.info(f"session keys are {flask.session.keys()}")
    return flask.redirect(auth_url)

# @app.route('/test_format')
# def test_format():
#     return flask.render_template('test_format.html', num = 0.81 )

@app.route('/audit/dummy/<num>')
def dummy(num):
    file_name = 'dummy.json'
    if num == '1':
        file_name = 'dummy2.json'
    with open(file_name) as f:
        d = json.load(f)
        return flask.render_template('index_new.html', data=d, profile={ 'id': 'XXX123XXX', 'name': 'dummy account' })

@app.route('/audit/<customerId>')
def check2_0(customerId):

    app.logger.info(f"starting check for customer {customerId}")
    app.logger.info(f"{flask.request.cookies}")
    SCOPES = ['https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets']

    creds = None

    user = flask.session['user']

    # Reference to firebase user document
    lead_ref = db.collection('leads').document(user['gid'])
    adwords_client = get_adwords_client()
    adwords_client.SetClientCustomerId(customerId)
    # customer_service = adwords_client.GetService('CustomerService', version='v201809')
    # account = customer_service.getCustomers()[0]

    try:
        lead_data = lead_ref.get().to_dict()
        if lead_data is None:
            #user doesn't exists
            lead_data = {
                'id': user['gid'],
                'name' : user['name'],
                'email' : user['email'],
                'checks': { customerId : [] }
            }
            lead_ref.set(lead_data)
    except google.cloud.exceptions.NotFound:
        lead_data = {
            'id': user['gid'],
            'name' : user['name'],
            'email' : user['email'],
            'checks': { customerId : [] }
        }
        lead_ref.set(lead_data)
 
    results = []
    total_score = 0
    max_score = 0
    typed_score = {}

    # pushing tasks to async loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    queue = []
    app.logger.info('starting async jobs')
    for item in checks.check_list :
        item['imagename'] = None
        queue.append(asyncator(loop, item['apply'], adwords_client, item, list=True))
    async_resutls = loop.run_until_complete(asyncio.gather(
    *queue,
    return_exceptions=True
    ))
    loop.close()
    account_stats = None
    app.logger.info('quering async jobs results')
    for idx, async_res in enumerate(async_resutls):
        item = checks.check_list[idx]
        if not isinstance(async_res, Exception):
            if item['name'] == 'account_stats':
                account_stats = async_res['rows'][1:-1]
            results.append(async_res)
            if async_res.get('scored', False):
                total_score += async_res.get('scored', 0)
                max_score += max(item['score'].values())
                if not typed_score.get(item['type'], None):
                    typed_score[item['type']] = {'score': 0, 'max_score': 0}
                typed_score[item['type']]['score'] += async_res.get('scored', 0)   
                typed_score[item['type']]['max_score'] += max(item['score'].values())
        else:
            app.logger.warning(f"bad result from {item['name']}:")
            app.logger.exception(async_res)
            results.append({ 'name': item.get('name',  'unknown'), 'error': 'no data' })
    app.logger.info('async jobs ready')
    if account_stats is not None:
        network_data = { row[1]:row[2] for row in account_stats }
    
    response = {
        'results': results,
        'total_score': total_score,
        'max_score': max_score,
        'typed_score': typed_score,
        'types': network_data
    }

    app.logger.info(flask.request.args.get('template', default = 'skip'))
    if flask.request.args.get('template', default = 'skip') != 'skip':
        return flask.render_template('index_new.html', data=response, profile=get_profile(customerId))
    return flask.jsonify(response)

@app.route('/audit/<customerId>/debug/<check_name>')
def check2_0_debug(customerId,check_name):
    check = next((check for check in checks.check_list if check.get('name', None) == check_name), None)
    response = {'error': f"no check '{check_name}'"}
    if not check:
        return flask.jsonify(response)
    adwords_client = get_adwords_client()
    adwords_client.SetClientCustomerId(customerId)
    response = check['apply'](adwords_client, check)
    return flask.jsonify(response)

# helper functions
def credentials_to_dict(credentials):
  return {'token': credentials.token,
          'refresh_token': credentials.refresh_token,
          'token_uri': credentials.token_uri,
          'client_id': credentials.client_id,
          'client_secret': credentials.client_secret,
          'scopes': credentials.scopes}

def get_adwords_client():
    # Load credentials from the session.
    credentials = google.oauth2.credentials.Credentials( #oauth2.credentials.Credentials
    **flask.session['credentials'])
    user_agent = 'test_python3_app'
    # Initialize the AdWords client.
    oauth2_client = oauth2.GoogleRefreshTokenClient(
    credentials.client_id, credentials.client_secret, credentials.refresh_token)
    adwords_client = adwords.AdWordsClient(
        developer_token, oauth2_client, user_agent)
    return adwords_client

def get_reports_rows(report_string):
    reader = csv.reader(report_string.split('\n'))
    affected = []
    for row in reader:
        if row != []:
            affected.append(row)
    return affected


# Flask error handling
class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = flask.jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

if __name__ == "__main__":
    app.run(host='0.0.0.0')
