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
import functools
import inspect


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
static_folder = "../dist/static",
template_folder = "../dist")

handler = RotatingFileHandler('log/cic-ads.log', maxBytes=200000, backupCount=5)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
handler.setLevel(logging.DEBUG)
app.logger.addHandler(handler)  # attach the handler to the app's logger

app.secret_key = 'cqoOyBUDkUpVsxIilDZRUcEV'

async def asyncator(loop, func, *args, **kwargs):
    parted = functools.partial(func, **kwargs)
    result = await loop.run_in_executor(None, parted, *args)
    return result

@app.route('/')
def hello_world():
    app.logger.info('app started successfully')
    return flask.render_template('index.html')

if not app.debug:
    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def catch_all(path):
        return flask.render_template("index.html")

@app.route('/authorize')
def authorize():
    # flow = google_auth_oauthlib.flow.Flow(code_verifier=None)
    # flow = flow.from_client_secrets_file(
    m_scopes=[oauth2.GetAPIScope('adwords'),
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/userinfo.profile']
    flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE, scopes=m_scopes)
    if app.debug:
        print (m_scopes)
    flow.redirect_uri = flask.request.url_root + 'oauth-callback'
    # flow.code_verifier = 's23'
    # flow.client_type = 'web'
    if app.debug:
        flow.redirect_uri = 'http://localhost:8080/oauth-callback'  # flask.url_for('oauth2callback')

    authorization_url, state = flow.authorization_url(
          access_type='offline',
          include_granted_scopes='true',  prompt='consent')
    # Store the state so the callback can verify the auth server response.
    flask.session['state'] = state
    flask.session['fl_config'] = flow.client_config
    flask.session['fl_client_type'] = flow.client_type
    flask.session['fl_code_verifier'] = flow.code_verifier
    app.logger.info(authorization_url)
    # return 'some authorize <a href="' +  authorization_url + '"> click here to authorize</a>'
    return flask.jsonify({'authorization_url': authorization_url})

@app.route('/oauth2callback')
def oauth2callback():
    if app.debug:
        print ('hello from oauth2callback. setting oauth_insecure to 1')
        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    if flask.request.url.startswith('http://'):
        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    # Specify the state when creating the flow in the callback so that it can
    # verified in the authorization server response.

    cb_scopes=[oauth2.GetAPIScope('adwords'),
        'https://www.googleapis.com/auth/userinfo.email',
        'https://www.googleapis.com/auth/userinfo.profile']

    conf = {'web': flask.session['fl_config']}

    # print (cb_scopes)

    fsession, client_config = (
            google_auth_oauthlib.helpers.session_from_client_config(conf, None))

    flow = google_auth_oauthlib.flow.Flow(fsession, flask.session['fl_client_type'], client_config,
            redirect_uri=None, code_verifier=flask.session['fl_code_verifier'])




    # flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
    #   CLIENT_SECRETS_FILE, scopes=[oauth2.GetAPIScope('adwords'),
    #   'https://www.googleapis.com/auth/userinfo.email',
    #   'https://www.googleapis.com/auth/userinfo.profile'], state=flask.request.args.get('state', ''))
    # flow.code_verifier = 's23'

    flow.redirect_uri = flask.request.url_root + 'oauth-callback'# flask.url_for('oauth2callback', _external=True)
    if app.debug:
        flow.redirect_uri = 'http://localhost:8080/oauth-callback'

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

    return  flask.jsonify({
        'gid': info['id'],
        'name': info['name'],
        'email': info['email']
        })

@app.route('/get_user')
def get_user():
    if 'user' in flask.session:
        return flask.jsonify(flask.session['user'])
    else:
        raise InvalidUsage('no user data in session', status_code=410)

@app.route('/get_client_list')
def get_clinet_list():
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
    return flask.jsonify(render_list)

@app.route('/get_profile/<clientId>')
def get_profile(clientId):
    adwords_client = get_adwords_client()
    adwords_client.SetClientCustomerId(clientId)
    customer_service = adwords_client.GetService('CustomerService', version='v201809')
    account = customer_service.getCustomers()

    return flask.jsonify({ 'id': account[0]['customerId'], 'name': account[0]['descriptiveName'] })

@app.route('/check_account/<customerId>/<check_service>')
def check_account(customerId, check_service):
    adwords_client = get_adwords_client()
    adwords_client.SetClientCustomerId(customerId)
    checks = [
    {
        'name': 'conversions_check',
        'description' :'Any conversions set up in tools>conversions?',
        'apply': check_convesions_exist,
        'imagename': '1.png'
    },
    {
        'name': 'broad_modifiers_check',
        'description' :'Full broad matches (not modifier)',
        'apply': full_broad_exist,
        'imagename': '2.png',
        'listed': True
    },
    {
        'name': 'short_modifiers_check',
        'description': '1 or 2 word Broad Match Modifiers',
        'apply': short_broad_exist,
        'imagename': '3.png',
        'listed': True
    },
    {
        'name': 'mobile_firendly_pages',
        'description' :'Landing pages are mobile friendly',
        'apply': mobile_firendly_pages, #landing_home_pages
        'imagename': '4.png',
        'listed': True
    },
    {
        'name': 'landing_home_pages',
        'description' :'Landing Page = homepage (no /anything at end of URL)',
        'apply': landing_home_pages, #has_modifiers
        'imagename': '5.png',
        'listed': True
    },
    {
        'name': 'low_quality_keywords',
        'description' :'No keywords with quality score <5',
        'apply': low_quality_keywords, # has_negatives
        'imagename': '6.png'
    },
    {
        'name': 'has_negatives',
        'description' :'Has Negatives',
        'apply': has_negatives, # has_changes
        'imagename': '7.png'
    },
    {
        'name': 'has_changes',
        'description' :'Change History Has More Than 10 changes in period (last 90 days)',
        'apply': has_changes, # cost_per_conversions
        'imagename': '8.png'
    },
    {
        'name': 'has_more3_ads',
        'description' :'Ad groups have three or more ads',
        'apply': has_more3_ads, # search_ctr
        'imagename': '9.png'
    },
    {
        'name': 'search_ctr',
        'description' :'Search CTR is Less Than 3%',
        'apply': search_ctr, # ave_position
        'imagename': '10.png'
    },
    {
        'name': 'ave_position',
        'description' :'Average Position is Better than 2.1',
        'apply': ave_position, # have_trials
        'imagename': '11.png'
    },
    {
        'name': 'have_trials',
        'description' :'High spending account has experiments happening',
        'apply': have_trials, # have_trials
        'imagename': '12.png'
    },
    {
        'name': 'has_modifiers',
        'description' :'CPC campaigns have bid modifiers in place',
        'apply': has_modifiers, #has_customizers
        'imagename': '13.png',
        'listed': True
    },
    {
        'name': 'has_customizers',
        'description' :'Account has ad customized feeds',
        'apply': has_customizers, #location_interested
        'imagename': '14.png'
    },
    {
        'name': 'bid_strategy',
        'description' :'Account has non manual strategies (applicable for spending over 3K)',
        'apply': bid_strategy, #bid_strategy
        'imagename': '15.png',
        'listed': True
    },
    {
        'name': 'location_interested',
        'description' :'Location targeting set to "physically in" location, not "interested in" default',
        'apply': location_interested, #bid_strategy
        'imagename': '16.png',
        'listed': True
    },
    {
        'name': 'cost_per_conversions',
        'description' :'Cost Per Conversions',
        'apply': cost_per_conversions # impressions_share
    },
    {
        'name': 'impressions_share',
        'description' :'Impressions Share',
        'apply': impressions_share # impressions_share
    }
    ]
    callee = next((item for item in checks if item['name'] == check_service), None)
    print("********************************************", callee)
    if callee['apply']:
        check_result = callee['apply'](adwords_client, callee)
        ## COMBAK:
        # return flask.jsonify(check_result)
        try:
            return callee['apply'](adwords_client, callee)
        except Exception as inst:
            flask.abort(404)
    else:
        raise InvalidUsage('unknown service', status_code=410)
# check functions
def check_convesions_exist(adwords_client, item, list=None):
    tracker_service = adwords_client.GetService(
        'ConversionTrackerService', version='v201809')
    offset = 0
    selector = {
        'fields': ['Id', 'Name', 'Category', 'TrackingCodeType'],
        'paging': {
            'startIndex': str(offset),
            'numberResults': str(PAGE_SIZE)
        }
    }
    conversions = tracker_service.get(selector)
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']
    app.logger.info('Conversions calculation complete %s', conversions.totalNumEntries)
    if conversions.totalNumEntries > 0:
        res['flag'] = 'green'
    else:
        res['flag'] = 'red'
    if list:
        return res
    return flask.jsonify(res)
    # return res

def full_broad_exist(adwords_client, item, list=None):
    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    ids = get_search_campaigns_ids(adwords_client)
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']
    affected = []

    if ids == []:
        res['flag'] = 'other'
    else:
        # Create report query.
        report_query = (adwords.ReportQueryBuilder()
                    .Select('Criteria', 'AdGroupName', 'CampaignName', 'Clicks')
                    .From('KEYWORDS_PERFORMANCE_REPORT')
                    .Where('Criteria').DoesNotContainIgnoreCase('+')
                    .Where('KeywordMatchType').EqualTo('BROAD')
                    .Where('Status').EqualTo('ENABLED')
                    .Where('AdGroupStatus').EqualTo('ENABLED')
                    .Where('CampaignId').In(*ids)
                    # TODO add campaign Ids filter
                    .During('LAST_MONTH')
                    .Build())
        stream_data = report_downloader.DownloadReportAsStringWithAwql(
            report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, include_zero_impressions=True)

        reader = csv.reader(stream_data.split('\n')) # , dialect='excel') .split('\n')
        for row in reader:
            if row != []:
                affected.append(row)

        if len(affected) > 1:
            res['flag'] = 'red'
        else:
            res['flag'] = 'green'

    app.logger.info('%s complete with flag %s', inspect.currentframe().f_code.co_name, res['flag'])

    if list:
        res['rows'] = affected
        return res
    return flask.jsonify(res)

def short_broad_exist(adwords_client, item, list=None):
    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    ids = get_search_campaigns_ids(adwords_client)
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']
    affected = []

    if ids == []:
        res['flag'] = 'other'
    else:
        # Create report query.
        report_query = (adwords.ReportQueryBuilder()
                    .Select('Criteria', 'AdGroupName', 'CampaignName', 'Clicks')
                    .From('KEYWORDS_PERFORMANCE_REPORT')
                    .Where('Criteria').ContainsIgnoreCase('+')
                    .Where('KeywordMatchType').EqualTo('BROAD')
                    .Where('Status').EqualTo('ENABLED')
                    .Where('AdGroupStatus').EqualTo('ENABLED')
                    .Where('CampaignId').In(*get_search_campaigns_ids(adwords_client))
                    .During('LAST_MONTH')
                    .Build())
        stream_data = report_downloader.DownloadReportAsStringWithAwql(
            report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, include_zero_impressions=True)

        reader = csv.reader(stream_data.split('\n')) # , dialect='excel') .split('\n')
        for row in reader:
            if row != [] and len(row[0].split()) < 3:
                affected.append(row)
        res = {}
        res['description'] = item['description']
        res['imagename'] = item['imagename']
        if len(affected) > 1:
            res['flag'] = 'red'
        else:
            res['flag'] = 'green'
        # if list:
        res['rows'] = affected

    app.logger.info('%s complete with flag %s', inspect.currentframe().f_code.co_name, res['flag'])

    if list:
        res['rows'] = affected
        return res
    return flask.jsonify(res)

def mobile_firendly_pages(adwords_client, item, list=None):
    report_downloader = adwords_client.GetReportDownloader(version='v201809')

    # Create report query.
    report_query = (adwords.ReportQueryBuilder()
                  .Select('CampaignName', 'ExpandedFinalUrlString', 'Clicks', 'PercentageMobileFriendlyClicks')
                  .From('LANDING_PAGE_REPORT')
                  .Where('PercentageMobileFriendlyClicks').LessThan(1.0)
                  .During('LAST_MONTH')
                  .Build())
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True)

    reader = csv.reader(stream_data.split('\n')) # , dialect='excel') .split('\n')
    affected = []
    for row in reader:
        if row != []:
            affected.append(row)
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']
    if len(affected) > 1:
        res['flag'] = 'amber'
    else:
        res['flag'] = 'green'

    app.logger.info('%s complete with flag %s', inspect.currentframe().f_code.co_name, res['flag'])

    if list:
        res['rows'] = affected
        return res
    return flask.jsonify(res)

def landing_home_pages(adwords_client, item, list=None):
    report_downloader = adwords_client.GetReportDownloader(version='v201809')

    # Create report query.
    report_query = (adwords.ReportQueryBuilder()
                  .Select('CampaignName', 'ExpandedFinalUrlString', 'Clicks')
                  .From('LANDING_PAGE_REPORT')
                  .During('LAST_MONTH')
                  .Build())
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True)

    reader = csv.reader(stream_data.split('\n')) # , dialect='excel') .split('\n')
    affected = []
    head = False
    check_re = r'^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+\/?)'
    for row in reader:
        if row != []:
            if not head:
                head = True
                url_index = row.index('Expanded landing page')
                affected.append(row)
            else:
                domain = re.search(check_re, row[url_index]).group()
                if domain == row[url_index]:
                    affected.append(row)
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']
    if len(affected) > 1:
        res['flag'] = 'amber'
    else:
        res['flag'] = 'green'

    app.logger.info('%s complete with flag %s', inspect.currentframe().f_code.co_name, res['flag'])

    if list:
        res['rows'] = affected
        return res
    return flask.jsonify(res)

def low_quality_keywords(adwords_client, item, list=None):
    report_downloader = adwords_client.GetReportDownloader(version='v201809')

    # Create report query.
    report_query = (adwords.ReportQueryBuilder()
                  .Select('Criteria', 'CampaignName', 'Clicks', 'QualityScore', 'HasQualityScore')
                  .From('KEYWORDS_PERFORMANCE_REPORT')
                  # .Where('HasQualityScore').In('1','2', '3', '4')
                  .Where('QualityScore').LessThan(5)
                  .During('LAST_MONTH')
                  .Build())
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True)

    reader = csv.reader(stream_data.split('\n'))
    affected = []
    for row in reader:
        if row != []:
            affected.append(row)
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']
    if len(affected) > 1:
        res['flag'] = 'amber'
    else:
        res['flag'] = 'green'
    app.logger.info('%s complete with flag %s', inspect.currentframe().f_code.co_name, res['flag'])
    if list:
        res['rows'] = affected
        return res
    return flask.jsonify(res)

def has_negatives(adwords_client, item, list=None):
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']
    # neg keywords = report of  CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT has entries
    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    report_query = (adwords.ReportQueryBuilder()
                  .Select('CampaignName', 'Criteria')
                  .From('CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT')
    #              .During('LAST_MONTH')
                  .Build())
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True)
    rows = get_reports_rows(stream_data)

    # neg lists = pages of service with type: NEGATIVE_KEYWORDS + status: ENABLED (?)
    sets_service = adwords_client.GetService('SharedSetService', version='v201809')
    offset = 0
    selector = {
        'fields': ['Name', 'Type', 'MemberCount'],
        'paging': {
            'startIndex': str(offset),
            'numberResults': str(PAGE_SIZE)
        },
        'predicates': [
            {
              'field': 'Type',
              'operator': 'EQUALS',
              'values': 'NEGATIVE_KEYWORDS'
            }
        ]
    }
    negative_keywords_lists = sets_service.get(selector)
    # if 'entries' in negative_keywords_lists:
    #     for entry in negative_keywords_lists['entries'][:3]:
    #         print (entry['sharedSetId'])
    #         print (entry['name'])

    if (not 'entries' in negative_keywords_lists) or (len(rows) == 0):
        res['flag'] = 'red'
    else:
        res['flag'] = 'green'
    app.logger.info('%s complete with flag %s', inspect.currentframe().f_code.co_name, res['flag'])
    if list:
        return res
    return flask.jsonify(res)

def has_changes(adwords_client, item, list=None):
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']
    # CustomerSyncService:
    customer_sync_service = adwords_client.GetService(
        'CustomerSyncService', version='v201809')

    # Construct selector and get all changes.
    today = datetime.datetime.today()
    yesterday = today - datetime.timedelta(days = 90)
    campaign_ids = get_campaigns_ids(adwords_client)
    changes_selector = {
      'dateTimeRange': {
          'min': yesterday.strftime('%Y%m%d %H%M%S'),
          'max': today.strftime('%Y%m%d %H%M%S')
      },
      'campaignIds': campaign_ids
    }

    account_changes = customer_sync_service.get(changes_selector)
    changes = []
    if account_changes:
        if account_changes['changedCampaigns']:
            for data in account_changes['changedCampaigns']:
                changes.append('Campaign with id "%s" has change status "%s".'
                   % (data['campaignId'], data['campaignChangeStatus']))
                if (data['campaignChangeStatus'] != 'NEW' and
                        data['campaignChangeStatus'] != 'FIELDS_UNCHANGED'):
                    if 'addedCampaignCriteria' in data:
                        changes.append('  Added campaign criteria: %s' %
                               data['addedCampaignCriteria'])
                    if 'removedCampaignCriteria' in data:
                        changes.append('  Removed campaign criteria: %s' %
                               data['removedCampaignCriteria'])
                    if 'changedAdGroups' in data:
                        for ad_group_data in data['changedAdGroups']:
                            changes.append('  Ad group with id "%s" has change status "%s".'
                                 % (ad_group_data['adGroupId'],
                                    ad_group_data['adGroupChangeStatus']))
                            if ad_group_data['adGroupChangeStatus'] != 'NEW':
                                if 'changedAds' in ad_group_data:
                                    changes.append ('    Changed ads: %s' % ad_group_data['changedAds'])
                            if 'changedCriteria' in ad_group_data:
                                changes.append('    Changed criteria: %s' %
                                 ad_group_data['changedCriteria'])
                            if 'removedCriteria' in ad_group_data:
                                changes.append('    Removed criteria: %s' %
                                 ad_group_data['removedCriteria'])
    if len(changes) < CHANGE_LIMIT:
        res['flag'] = 'red'
    else:
        res['flag'] = 'green'
    if app.debug:
        for row in changes[:4]:
            print(row)
    if list:
        return res
    return flask.jsonify(res)

def has_more3_ads(adwords_client, item, list=None):
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']
    report_downloader = adwords_client.GetReportDownloader(version='v201809')

    # Create report query.
    report_query = (adwords.ReportQueryBuilder()
                  .Select('Id', 'CampaignId', 'AdGroupId', 'AdGroupName', 'AdGroupStatus', 'CampaignStatus', 'Status', 'CampaignName')
                  .From('AD_PERFORMANCE_REPORT')
                  .Where('Status').EqualTo('ENABLED')
                  .Where('AdGroupStatus').EqualTo('ENABLED')
                  .Where('CampaignStatus').EqualTo('ENABLED')
                  .During('LAST_MONTH')
                  .Build())
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True)

    reader = csv.reader(stream_data.split('\n'))
    structure = {}
    header = ['Id', 'CampaignId', 'AdGroupId', 'AdGroupName', 'AdGroupStatus', 'CampaignStatus', 'Status', 'CampaignName']
    for row in reader:
        if row != []:
            if not structure.get(row[header.index('AdGroupId')]):
                structure[row[header.index('AdGroupId')]] = {}
                structure[row[header.index('AdGroupId')]]['CampaignName'] = row[header.index('CampaignName')]
                structure[row[header.index('AdGroupId')]]['AdGroupName'] = row[header.index('AdGroupName')]
                structure[row[header.index('AdGroupId')]]['NumberAds'] = 0
            structure[row[header.index('AdGroupId')]]['NumberAds']  += 1
    filtered_list = [[AdGroupObj['CampaignName'], AdGroupObj['AdGroupName'], AdGroupObj['NumberAds']] for AdGroupId, AdGroupObj in structure.items() if AdGroupObj['NumberAds'] < 3]

    if len(filtered_list) > 0:
        res['flag'] = 'amber'
        if  app.debug:
            for row in filtered_list[:5]:
                print (row)
    else:
        res['flag'] = 'green'
    if list:
        return res

    return flask.jsonify(res)

def search_ctr(adwords_client, item, list=None):
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']
    report_downloader = adwords_client.GetReportDownloader(version='v201809')

    # Create report query.
    report_query = (adwords.ReportQueryBuilder()
                  .Select('AdNetworkType1', 'CustomerDescriptiveName', 'Clicks', 'Ctr')
                  .From('ACCOUNT_PERFORMANCE_REPORT')
                  .Where('AdNetworkType1').EqualTo('SEARCH')
                  .During(**DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['AdNetworkType1', 'CustomerDescriptiveName', 'Clicks', 'Ctr']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    rows = get_reports_rows(stream_data)
    if rows != []:
        row = rows[0]
        if app.debug:
            print (row)
        ctr = float(row[header.index('Ctr')].strip('%') ) / 100
        res['flag'] = 'green'
        if (ctr < 0.03):
            res['flag'] = 'amber'
        if ctr < 0.02:
            res['flag'] = 'red'
    else:
        res['flag'] = 'other'
    if list:
        return res

    return flask.jsonify(res)

def ave_position(adwords_client, item, list=None):
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']
    report_downloader = adwords_client.GetReportDownloader(version='v201809')

    # Create report query.
    report_query = (adwords.ReportQueryBuilder()
                  .Select('AdNetworkType1', 'CustomerDescriptiveName', 'Clicks', 'AveragePosition')
                  .From('ACCOUNT_PERFORMANCE_REPORT')
                  .Where('AdNetworkType1').EqualTo('SEARCH')
                  .During(**DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['AdNetworkType1', 'CustomerDescriptiveName', 'Clicks', 'AveragePosition']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    rows = get_reports_rows(stream_data)
    if rows == []:
        res['flag'] = 'other'
        return flask.jsonify(res)
    row = rows[0]
    if app.debug:
        print (row)
    av_position = float(row[header.index('AveragePosition')])
    res['flag'] = 'green'
    if (av_position > 2.1):
        res['flag'] = 'amber'
    if list:
        return res

    return flask.jsonify(res)

def have_trials(adwords_client, item, list=None):
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']
    report_downloader = adwords_client.GetReportDownloader(version='v201809')

    # Create report query.
    report_query = (adwords.ReportQueryBuilder()
                  .Select('AdNetworkType1', 'CustomerDescriptiveName', 'Clicks', 'Cost')
                  .From('ACCOUNT_PERFORMANCE_REPORT')
                  .Where('AdNetworkType1').EqualTo('SEARCH')
                  .During(**DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['AdNetworkType1', 'CustomerDescriptiveName', 'Clicks', 'Cost']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    rows = get_reports_rows(stream_data)
    if rows == []:
        res['flag'] = 'other'
        return flask.jsonify(res)
    row = rows[0]
    if app.debug:
        print (row)
    res['flag'] = 'amber'
    cost = float(row[header.index('Cost')]) / 1000000
    if cost > 3000:
        if app.debug:
            print ("checking trials, budget {cost} is over 3000")
        # campaignTrialType == TRIAL
        campaign_service = adwords_client.GetService('CampaignService', version='v201809')
        offset = 0
        selector = {
            'fields': ['CampaignTrialType', 'Id', 'CampaignName'],
            'paging': {
                'startIndex': str(offset),
                'numberResults': str(PAGE_SIZE)
            },
            'predicates': [
                {
                  'field': 'CampaignTrialType',
                  'operator': 'EQUALS',
                  'values': 'TRIAL'
                },
                {
                  'field': 'Status',
                  'operator': 'EQUALS',
                  'values': 'ENABLED'
                },
                {
                  'field': 'ServingStatus',
                  'operator': 'NOT_EQUALS',
                  'values': 'ENDED'
                }
            ]
        }
        trials = campaign_service.get(selector)
        if trials.totalNumEntries > 0:
            if app.debug:
                for entry in trials['entries'][:3]:
                    print (entry['CampaignName'])
            res['flag'] = 'green'
    if list:
        return res

    return flask.jsonify(res)

def has_modifiers(adwords_client, item, list=None):
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']
    # GET CPC campaigns:
    campaigns_service = adwords_client.GetService(
        'CampaignService', version='v201809')

    offset = 0
    selector = {
        'fields': ['Name', 'Id', 'BiddingStrategyType'],
        'paging': {
            'startIndex': str(offset),
            'numberResults': str(PAGE_SIZE)
        },
        'predicates': [
            {
              'field': 'BiddingStrategyType', #campaigns_service
              'operator': 'EQUALS',
              'values': 'MANUAL_CPC'
            },{
              'field': 'Status', #campaigns_service
              'operator': 'EQUALS',
              'values': 'ENABLED'
            }
        ]
    }
    campaign_entries = get_selector_entries(campaigns_service, selector)

    campaign_ids = [campaign['id'] for campaign in campaign_entries]

    if campaign_ids != []:

        # CustomerSyncService:
        criterion_service = adwords_client.GetService(
            'CampaignCriterionService', version='v201809')
        offset = 0
        selector = {
            'fields': ['CampaignId', 'CampaignCriterionStatus', 'CriteriaType', 'BidModifier'],
            'paging': {
                'startIndex': str(offset),
                'numberResults': str(PAGE_SIZE)
            },
            'predicates': [
                {
                  'field': 'CriteriaType',
                  'operator': 'NOT_EQUALS',
                  'values': 'KEYWORD'
                },
                {
                  'field': 'BidModifier',
                  'operator': 'GREATER_THAN',
                  'values': 0.0
                },
                {
                  'field': 'CampaignId',
                  'operator': 'IN',
                  'values': campaign_ids
                }
            ]
        }
        selector_entries = get_selector_entries(criterion_service, selector)
        affected = []
        header = ['campaignId', 'campaignCriterionStatus', 'criterion', 'bidModifier']
        campaigns_ok = []
        if selector_entries != []:
            for criterion in selector_entries: #page['entries']:
                row = [criterion[key] for key in header]
                row[header.index('criterion')] = row[header.index('criterion')]['type']
                row[header.index('bidModifier')] = row[header.index('bidModifier')] - 1
                campaigns_ok.append(row[header.index('campaignId')])
                # affected.append(row)
                # campaign_dict[row[header.index('campaignId')]]['hasModifiers'] = True
        for campaign in campaign_entries:
            if not campaign['id'] in campaigns_ok:
                affected.append([campaign['name'], 'No Modifiers'])

        affected = [['Campaign name', 'Has Modifiers']] + affected
    else:
        #no campaigns
        affected = [['Campaign name', 'Has Modifiers']] + [['No Enabled Campaigns', 'No Data']]
    if app.debug:
        print (affected[:5])
    if len(affected) > 1:
        res['flag'] = 'red'
    else:
        res['flag'] = 'green'
    if list:
        res['rows'] = affected
        return res
    return flask.jsonify(res)

def has_customizers(adwords_client, item, list=None):
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']
    # GET CPC campaigns:
    feeds_service = adwords_client.GetService(
        'AdCustomizerFeedService', version='v201809')
    offset = 0
    selector = {
            'fields': ['FeedId', 'FeedName'],

            'paging': {
                'startIndex': str(offset),
                'numberResults': str(PAGE_SIZE)
            },
            'predicates': [
                {
                  'field': 'FeedStatus', #campaigns_service
                  'operator': 'EQUALS',
                  'values': 'ENABLED'
                }
            ]
        }
    feed_page = feeds_service.get(selector)
    if app.debug:
        if 'entries' in feed_page:
            for item in feed_page['entries']:
                print(item['feedName'])
    if feed_page['totalNumEntries'] > 0:
        res['flag'] = 'green'
    else:
        res['flag'] = 'red'
    if list:
        return res

    return flask.jsonify(res)

def location_interested(adwords_client, item, list=None):
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']
    # GET CPC campaigns:
    campaigns_service = adwords_client.GetService(
        'CampaignService', version='v201809')
    offset = 0
    selector = {
        'fields': ['CampaignId', 'Name', 'Settings'],
        'paging': {
            'startIndex': str(offset),
            'numberResults': str(PAGE_SIZE)
        },
        'predicates': [
            {
              'field': 'Status', #campaigns_service
              'operator': 'EQUALS',
              'values': 'ENABLED'
            }
        ]
    }
    mapping = {
        'DONT_CARE': 'either AOI or LOP may trigger the ad',
        'AREA_OF_INTEREST': 'the ads are triggered only if the user\'s Area Of Interest matches',
        'LOCATION_OF_PRESENCE': 'ad is triggered only if the user\'s Location Of Presense matches'
    }
    campaign_entries = get_selector_entries(campaigns_service, selector)
    print (campaign_entries[:4])
    affected = [['Camp Id', 'Camp Name', 'Geo Settings']]
    for campaign in campaign_entries:
        geo_setting = next((setting for setting in campaign['settings'] if setting['Setting.Type'] == 'GeoTargetTypeSetting'), None)
        if geo_setting and geo_setting['positiveGeoTargetType']  != 'LOCATION_OF_PRESENCE':

            affected.append([ campaign['id'], campaign['name'], mapping[geo_setting['positiveGeoTargetType']] ]) #campaign['settings']
    if app.debug:
        print (affected[0:5])
    res['flag'] = 'green'
    if len(affected) > 1:
        res['flag'] = 'amber'
    if list:
        res['rows'] = affected
        return res
    return flask.jsonify(res)

def cost_per_conversions(adwords_client, item, list=None):
    res = {'description' : item['description']}
    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    report_query = (adwords.ReportQueryBuilder()
                  .Select('ConversionTypeName', 'Conversions', 'CostPerConversion')
                  .From('ACCOUNT_PERFORMANCE_REPORT')
                  .During(**DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['Action Name', 'Conversions', 'Cost per conversion']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    rows = get_reports_rows(stream_data)
    for row in rows:
        row[2] = float(row[2])/1000000.0

    if app.debug:
        print (rows[0:6])
    if list:
        res['rows'] = [header] + rows
        return res
    res['rows'] = [header] + rows
    return flask.jsonify(res)

def impressions_share(adwords_client, item, list=None):
        res = {'description' : item['description']}
        report_downloader = adwords_client.GetReportDownloader(version='v201809')
        report_query = (adwords.ReportQueryBuilder()
                      .Select('Impressions', 'SearchBudgetLostImpressionShare', 'SearchRankLostImpressionShare')
                      .From('ACCOUNT_PERFORMANCE_REPORT')
                      .Where('AdNetworkType1').EqualTo('SEARCH')
                      .During(**DEFAULT_PERFOMANCE_PERIOD)
                      .Build())
        header = ['Search Impressions Recieved', 'Lost ImpressionShare (Budget)', 'Lost ImpressionShare (Rank)']
        stream_data = report_downloader.DownloadReportAsStringWithAwql(
            report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
        rows = get_reports_rows(stream_data)
        # for row in rows:
        #     row[2] = float(row[2])/1000000.0

        if app.debug:
            print (rows[0:6])
        if list:
            res['rows'] = [header] + rows
            return res
        res['rows'] = [header] + rows
        return flask.jsonify(res)

def bid_strategy(adwords_client, item, list=None):
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']
    value_map = {
    'MANUAL_CPC': 'Manual cpc',
    'MANUAL_CPV': 'Manual cpv',
    'MANUAL_CPM': 'Manual cpm',
    'PAGE_ONE_PROMOTED': 'Target search page location',
    'TARGET_SPEND': 'Maximize clicks',
    'TARGET_CPA': 'Target CPA',
    'TARGET_ROAS': 'Target ROAS',
    'MAXIMIZE_CONVERSIONS': 'Maximize Conversions',
    'MAXIMIZE_CONVERSION_VALUE': 'Maximize Conversion Value',
    'TARGET_OUTRANK_SHARE': 'Target Outranking Share',
    'NONE': 'None',
    'UNKNOWN': 'unknown'
    }
    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    report_query = (adwords.ReportQueryBuilder()
                  .Select('BiddingStrategyType', 'Cost')
                  .From('CAMPAIGN_PERFORMANCE_REPORT')
                  .During(**DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['Bidding Strategy Type', 'Cost']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    rows = get_reports_rows(stream_data)
    stat_object = {}
    total_cost = 0.0
    non_manual = False
    for row in rows:
        row[1] = float(row[1])/1000000.0
        total_cost += row[1]
        if (row[0] not in ['cpc', 'cpv', 'cpm']) and row[1] > 0.0:
            non_manual = True
        if not stat_object.get(row[0], None):
            stat_object[row[0]] = row[1]
        else:
            stat_object[row[0]] += row[1]
    stat_rows = [[key, value] for key, value in stat_object.items()]
    if app.debug:
        print (stat_rows[:6])
    res['flag'] = 'green'
    if total_cost > 3000 and non_manual:
        res['flag'] = 'amber'
    if list:
        res['rows'] = [header] + stat_rows
        return res
    return flask.jsonify(res)

@app.route('/create_sheet/<customerId>')
def build_sheet_id(customerId):
    SCOPES = ['https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets']
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
                SHEET_SECRETS_FILE, SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    #saving to firebasei

    user = flask.session['user']
    # Reference to firebase user document
    lead_ref = db.collection('leads').document(user['gid'])
    adwords_client = get_adwords_client()
    adwords_client.SetClientCustomerId(customerId)
    customer_service = adwords_client.GetService('CustomerService', version='v201809')
    account = customer_service.getCustomers()[0]

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
        print ('user not found')
        lead_data = {
            'id': user['gid'],
            'name' : user['name'],
            'email' : user['email'],
            'checks': { customerId : [] }
        }
        lead_ref.set(lead_data)

    # sheet checks
    checks = [
        {
            'name': 'conversions_check',
            'description' :'Any conversions set up in tools>conversions?',
            'apply': check_convesions_exist
        },
        {
            'name': 'broad_modifiers_check',
            'description' :'Full broad matches (not modifier)',
            'apply': full_broad_exist,
            'listed': True,
            'sheet_name': 'Full broad keywords'
        },
        {
            'name': 'short_modifiers_check',
            'description' :'1 or 2 word Broad Match Modifiers exist',
            'apply': short_broad_exist,
            'listed': True,
            'sheet_name': '1 or 2 word Broad Match Modifiers'
        },
        {
            'name': 'mobile_firendly_pages',
            'description' :'Landing pages are mobile friendly',
            'apply': mobile_firendly_pages, #low_quality_keywords
            'listed': True,
            'sheet_name': 'Non Mobile Friendly Landing Pages'
        },
        {
            'name': 'landing_home_pages',
            'description' :'Landing Page = homepage (no /anything at end of URL)',
            'apply': landing_home_pages, #landing_home_pages
            'listed': True,
            'sheet_name': 'Hompepage Landing Pages'
        },
        {
            'name': 'low_quality_keywords',
            'description' :'No keywords with quality score <5',
            'apply': low_quality_keywords, # has_negatives
            'listed': True,
            'sheet_name': 'Low Quality Keywords'
        },
        {
            'name': 'has_negatives',
            'description' :'Has Negatives',
            'apply': has_negatives # has_changes
        },
        {
            'name': 'has_changes',
            'description' :'Change History Has More Than 10 changes in period (last 90 days)',
            'apply': has_changes # cost_per_conversions
        },
        {
            'name': 'has_more3_ads',
            'description' :'Ad groups have three or more ads',
            'apply': has_more3_ads # search_ctr
        },
        {
            'name': 'search_ctr',
            'description' :'Search CTR is Less Than 3%',
            'apply': search_ctr # ave_position
        },
        {
            'name': 'ave_position',
            'description' :'Average Position is Better than 2.1',
            'apply': ave_position # have_trials
        },
        {
            'name': 'have_trials',
            'description' :'High spending account has experiments happening',
            'apply': have_trials # have_trials
        },
        {
            'name': 'has_modifiers',
            'description' :'CPC campaigns have bid modifiers in place',
            'apply': has_modifiers, #has_modifiers
            'listed': True,
            'sheet_name': 'Bid Modifiers'
        },
        {
            'name': 'has_customizers',
            'description' :'Account has ad customized feeds',
            'apply': has_customizers #location_interested
        },
        {
            'name': 'bid_strategy',
            'description' :'Account has non manual strategies (applicable for spending over 3K)',
            'apply': bid_strategy, #bid_strategy
            'listed': True,
            'sheet_name': 'Bid Strategies'
        },
        {
            'name': 'location_interested',
            'description' :'Location targeting set to "physically in" location, not "interested in" default',
            'apply': location_interested, #location_interested
            'listed': True,
            'sheet_name': 'Location Settings'
        },
        {
            'name': 'cost_per_conversions',
            'description' :'Cost Per Conversions',
            'apply': cost_per_conversions, # impressions_share
            'listed': True,
            'sheet_name': 'Cost Per Conversions'
        },
        {
            'name': 'impressions_share',
            'description' :'Simpression Share',
            'apply': impressions_share, # impressions_share
            'listed': True,
            'sheet_name': 'Impressions Share'
        }

    ]
    results = []
    spreadsheet_body = {
        'properties': {
            'title': 'Clicks in Context Google Ads Health Check : ' + account['descriptiveName']
        },
        "sheets": [
        {
        "properties": {
            "title": 'All Checks'
            }
        }
        ]
    }
    # adding loop support

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    queue = []
    for item in checks:
        item['imagename'] = None
        queue.append(asyncator(loop, item['apply'], adwords_client, item, list=True))
    async_resutls = loop.run_until_complete(asyncio.gather(
    *queue,
    return_exceptions=True
    ))
    loop.close()


    for idx, async_res in enumerate(async_resutls):
        item = checks[idx]
        if not isinstance(async_res, Exception):
            async_res['sheet_name'] = item['sheet_name'] if item.get('sheet_name', False) else None
            async_res['listed'] = item['listed'] if item.get('listed', False) else None
            results.append(async_res)
            # print (async_res)
            # app.logger.info('got async res with keys []%s]',','.join(async_res.keys) )
            if item.get('listed', False):
                app.logger.info('Creating sheet %s', item.get('sheet_name'))
                spreadsheet_body['sheets'].append({ "properties": { "title": item.get('sheet_name')}})
                print ('creating sheet: ' + "'{0}'".format(item.get('sheet_name')))
        else:
            print(f"oops something went wrong {item['name']}")
            app.logger.warning(f"bad result from {item['name']}")
            app.logger.exception(async_res)

    sheets_service = googleapiclient.discovery.build('sheets', 'v4', credentials=creds)
    drive_service = googleapiclient.discovery.build('drive', 'v3', credentials=creds)

    request = sheets_service.spreadsheets().create(body=spreadsheet_body)
    response = request.execute()

    file_id = response.get('spreadsheetId')
    folder_id = FOLDER_ID
    file = drive_service.files().get(fileId=file_id,
                                    fields='parents').execute();
    previous_parents = ",".join(file.get('parents'))
    # Move the file to the new folder
    file = drive_service.files().update(fileId=file_id,
                                        addParents=folder_id,
                                        removeParents=previous_parents,
                                        fields='id, parents').execute()
    



    sheet_url = response.get('spreadsheetUrl')
    lead_checks = lead_data['checks']
    lead_checks[customerId] = lead_checks.get(customerId, [])
    lead_checks[customerId].append({
        'date': datetime.datetime.today().strftime('%Y-%m-%d'),
        'account_name': account['descriptiveName'],
        'sheetUrl': sheet_url
    })
    lead_ref.set({
        'checks' : lead_checks
    }, merge=True)
    sheet_id = response.get('spreadsheetId')
    main_results = []
    for index, item in enumerate(results):
        try:
            item.get('description', False)
        except:
            print (checks[index])
        if item.get('description', False) and item.get('flag', False):
            main_results.append([item['description'], item['flag']])
        if results[index].get('listed', False):
            body = {
                'values': item.get('rows', [[]])
            }
            if app.debug:
                print (item.get('rows'))
            result = sheets_service.spreadsheets().values().update(
            spreadsheetId=sheet_id, range="'{0}'".format(results[index]['sheet_name']),
            valueInputOption='RAW', body=body).execute()
    body = {
        'values': main_results
    }
    result = sheets_service.spreadsheets().values().update(
    spreadsheetId=sheet_id, range='All Checks',
    valueInputOption='RAW', body=body).execute()

    drive_service = googleapiclient.discovery.build('drive', 'v3', credentials=creds)
    drive_request = drive_service.files().get(fileId=sheet_id)
    new_file = drive_request.execute()
    email = flask.session['user'].get('email', 'postman31@gmail.com')
    user_permission = {
        'type': 'user',
        'role': 'writer',
        'emailAddress': email
    }
    if app.debug:
        print (email)
    req = drive_service.permissions().create(
                fileId=sheet_id,
                body=user_permission,
                fields="id"
            )
    req.execute()
    return flask.jsonify({'url':sheet_url})

@app.route('/dummy_sheet/')
def build_sheet():
    SCOPES = ['https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets']
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
                SHEET_SECRETS_FILE, SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    sheets_service = googleapiclient.discovery.build('sheets', 'v4', credentials=creds)
    spreadsheet_body = {
        'properties': {
            'title': 'Ads Audit Results'
        },
        "sheets": [
           {
             "properties": {
             "title": 'All Checks'
             },
           },
           {
             "properties": {
                  "title": 'Second Sheet'
             },
           }
         ]
        # TODO: Add desired entries to the request body.
    }

    request = sheets_service.spreadsheets().create(body=spreadsheet_body)
    response = request.execute()
    # for key in dir(response):
    #     print (key)
    sheet_url = response.get('spreadsheetUrl')
    sheet_id = response.get('spreadsheetId')
    print (sheet_id)
     # build('drive', 'v3'
    drive_service = googleapiclient.discovery.build('drive', 'v3', credentials=creds)
    drive_request = drive_service.files().get(fileId=sheet_id)
    new_file = drive_request.execute()
    user_permission = {
        'type': 'user',
        'role': 'writer',
        'emailAddress': 'postman31@gmail.com'
    }
    req = drive_service.permissions().create(
                fileId=sheet_id,
                body=user_permission,
                fields="id"
            )
    req.execute()

    return flask.jsonify({'unl':sheet_url})


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

def get_campaigns_ids(client):
    # returns all campaigns IDs
    campaign_service = client.GetService('CampaignService', version='v201809')
    campaigns_selector = {
      'fields': ['Id', 'Name', 'Status']
    }
    campaigns = campaign_service.get(campaigns_selector)
    campaign_ids = []
    if 'entries' in campaigns:
        print ("fonud {campaigns.totalNumEntries}")
        for campaign in campaigns['entries']:
            campaign_ids.append(campaign['id'])
            # print(f"campaign name {campaign.name}")
    else:
        print ('No campaigns were found.')
    return campaign_ids

def get_selector_entries(service, selector):
    offset = int(selector['paging']['startIndex'])
    more_pages = True
    all_entries = []
    while more_pages:
        page = service.get(selector)
        if 'entries' in page:
            all_entries = all_entries + page['entries']
        offset += PAGE_SIZE
        selector['paging']['startIndex'] = str(offset)
        more_pages = offset < int(page['totalNumEntries'])
    return all_entries

def get_search_campaigns_ids(client):
    # returns all campaigns IDs
    campaign_service = client.GetService('CampaignService', version='v201809')
    campaigns_selector = {
      'fields': ['Id', 'Name', 'Status'],
      'predicates': [
          {
            'field': 'AdvertisingChannelType',
            'operator': 'IN',
            'values': ['SEARCH', 'MULTI_CHANNEL']
          },
          {
            'field': 'Status',
            'operator': 'EQUALS',
            'values': 'ENABLED'         }
      ]
    }
    campaigns = campaign_service.get(campaigns_selector)
    campaign_ids = []
    if 'entries' in campaigns:
        print ("fonud {campaigns.totalNumEntries}")
        for campaign in campaigns['entries']:
            campaign_ids.append(campaign['id'])
            # print(f"campaign name {campaign.name}")
    else:
        print ('No campaigns were found.')
    return campaign_ids

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
