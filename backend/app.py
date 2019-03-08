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



CLIENT_SECRETS_FILE = 'creds/client-creds.json'
SHEET_SECRETS_FILE = 'creds/sheet_creds.json'
developer_token = 'kRqQZviBtOhZFDatpjTLmw'
sheets_token = '1/M2iYW2N8Evip-ReUMK7Xix6jM6JYmVwZMPLUWSIxUFLbD9jO06plCyRUTZcjCdmx'
PAGE_SIZE = 100
CHANGE_LIMIT = 10
DEFAULT_PERFOMANCE_PERIOD = 'LAST_30_DAYS'


app = flask.Flask(__name__,
static_folder = "../dist/static",
template_folder = "../dist")

app.secret_key = 'cqoOyBUDkUpVsxIilDZRUcEV'

@app.route('/')
def hello_world():
    # return 'Hello, World!' + flask.url_for('authorize', _external=True)
    return flask.render_template('index.html')

@app.route('/authorize')
def authorize():
    flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE, scopes=[oauth2.GetAPIScope('adwords'),
        'https://www.googleapis.com/auth/userinfo.email',
        'https://www.googleapis.com/auth/userinfo.profile'])

    # change this!!
    flow.redirect_uri = flask.url_for('oauth2callback', _external=True)
    if app.debug:
        flow.redirect_uri = 'http://localhost:8080/oauth-callback'  # flask.url_for('oauth2callback')

    authorization_url, state = flow.authorization_url(
          access_type='offline',
          include_granted_scopes='true',  prompt='consent')
    # Store the state so the callback can verify the auth server response.
    flask.session['state'] = state
    print (authorization_url)
    # return 'some authorize <a href="' +  authorization_url + '"> click here to authorize</a>'
    return flask.jsonify({'authorization_url': authorization_url})

@app.route('/oauth2callback')
def oauth2callback():
    if app.debug:
        print ('hello from oauth2callback. setting oauth_insecure to 1')
        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    # Specify the state when creating the flow in the callback so that it can
    # verified in the authorization server response.

    #state = flask.session['state']
    # user_id = flask.session['userId']

    flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
      CLIENT_SECRETS_FILE, scopes=[oauth2.GetAPIScope('adwords'),
      'https://www.googleapis.com/auth/userinfo.email',
      'https://www.googleapis.com/auth/userinfo.profile'], state=flask.request.args.get('state', ''))
    # flow.redirect_uri = flask.url_for('oauth2callback', _external=True)
    if app.debug:
        flow.redirect_uri = 'http://localhost:8080/oauth-callback'

    # Use the authorization server's response to fetch the OAuth 2.0 tokens.
    try:
        # authorization_response = flask.request.url
        # print (authorization_response)
        print (flask.request.args)
        flow.fetch_token(code=flask.request.args.get('code', ''))
        # pass
    except:
        print("Unexpected error:", sys.exc_info()[0])
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

    # return flask.jsonify({'user': 'authorized'})
    if app.debug:
        # redirect to dev.accounts
        return  flask.jsonify({
            'gid': info['id'],
            'name': info['name'],
            'email': info['email']
            })
    else:
        # set coockie
        return 'authorized'

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
    accounts = customer_service.getCustomers(selector)

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
            # adwords_managed_client = adwords.AdWordsClient(
            # developer_token, oauth2_client, user_agent, client_customer_id=customer['customerId'])  #client_customer_id=client_customer_id
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

      # ACTION ITEM: In a production app, you likely want to save these
      #              credentials in a persistent database instead.
    # flask.session['credentials'] = credentials_to_dict(credentials)
    return flask.jsonify(render_list)

@app.route('/check_account/<customerId>/<check_service>')
def check_account(customerId, check_service):
    adwords_client = get_adwords_client()
    adwords_client.SetClientCustomerId(customerId)
    checks = [{
        'name': 'conversions_check',
        'description' :'Any conversions set up in tools>conversions?',
        'apply': check_convesions_exist
    },
    {
        'name': 'broad_modifiers_check',
        'description' :'Full broad matches (not modifier)',
        'apply': full_broad_exist,
        'listed': True
    },
    {
        'name': 'mobile_firendly_pages',
        'description' :'Landing pages are mobile firendly',
        'apply': mobile_firendly_pages #low_quality_keywords
    },
    {
        'name': 'low_quality_keywords',
        'description' :'Has Kewqords with Quality score less then 5',
        'apply': low_quality_keywords # has_negatives
    },
    {
        'name': 'has_negatives',
        'description' :'Has Negatives',
        'apply': has_negatives # has_changes
    },
    {
        'name': 'has_changes',
        'description' :'Change History Has More Than 10 changes in period (last 90 days)',
        'apply': has_changes # has_more3_ads
    },
    {
        'name': 'has_more3_ads',
        'description' :'Ad Groups Have Three Or Move Ads',
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
        'description' :'High Spending Account has active Trial Campaigns',
        'apply': have_trials # have_trials
    }
    ]
    callee = next((item for item in checks if item['name'] == check_service), None)
    if callee['apply']:
            return callee['apply'](adwords_client, callee)
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
    if conversions.totalNumEntries > 0:
        res['flag'] = 'green'
    else:
        res['flag'] = 'red'
    if list:
        return res
    return flask.jsonify(res)

def full_broad_exist(adwords_client, item, list=None):
    criteria_service = adwords_client.GetService('AdGroupCriterionService', version='v201809')
    offset = 0
    selector = {
        'fields': ['KeywordText', 'KeywordMatchType', 'AdGroupId'],
        'paging': {
            'startIndex': str(offset),
            'numberResults': str(PAGE_SIZE)
        },
        'predicates': [
            {
              'field': 'IsNegative',
              'operator': 'EQUALS',
              'values': False
            },
            {
              'field': 'KeywordText',
              'operator': 'DOES_NOT_CONTAIN',
              'values': '+'
            }
        ]
    }
    res = {}
    res['description'] = item['description']
    keywords = criteria_service.get(selector)
    if keywords.totalNumEntries > 0:
        res['flag'] = 'red'
    else:
        res['flag'] = 'green'
    if list:
        rows =[['KeywordText', 'AdGroupId']] + [[keyword['criterion']['text'], keyword['adGroupId']] for keyword in keywords['entries']]
        print (rows[:5])
        res['rows'] = rows
        return res
    return flask.jsonify(res)

def mobile_firendly_pages(adwords_client, item):
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
            print(row)
    res = {}
    res['description'] = item['description']
    if len(affected) > 0:
        res['flag'] = 'amber'
    else:
        res['flag'] = 'green'
    return flask.jsonify(res)

def low_quality_keywords(adwords_client, item):
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
            print(row)
    res = {}
    res['description'] = item['description']
    if len(affected) > 0:
        res['flag'] = 'amber'
    else:
        res['flag'] = 'green'
    return flask.jsonify(res)

def has_negatives(adwords_client, item):
    res = {}
    res['description'] = item['description']

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
    if 'entries' in negative_keywords_lists:
        for entry in negative_keywords_lists['entries'][:3]:
            print (entry['sharedSetId'])
            print (entry['name'])

    if (not 'entries' in negative_keywords_lists) or (len(rows) == 0):
        res['flag'] = 'red'
    else:
        res['flag'] = 'green'
    return flask.jsonify(res)

def has_changes(adwords_client, item):
    res = {}
    res['description'] = item['description']
    # CustomerSyncService:
    customer_sync_service = adwords_client.GetService(
        'CustomerSyncService', version='v201809')

    # Construct selector and get all changes.
    today = datetime.datetime.today()
    yesterday = today - datetime.timedelta(days = 90)
    campaign_ids = get_campain_ids(adwords_client)
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
    return flask.jsonify(res)

def has_more3_ads(adwords_client, item):
    res = {}
    res['description'] = item['description']
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
    return flask.jsonify(res)

def search_ctr(adwords_client, item):
    res = {}
    res['description'] = item['description']
    report_downloader = adwords_client.GetReportDownloader(version='v201809')

    # Create report query.
    report_query = (adwords.ReportQueryBuilder()
                  .Select('AdNetworkType1', 'CustomerDescriptiveName', 'Clicks', 'Ctr')
                  .From('ACCOUNT_PERFORMANCE_REPORT')
                  .Where('AdNetworkType1').EqualTo('SEARCH')
                  .During(DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['AdNetworkType1', 'CustomerDescriptiveName', 'Clicks', 'Ctr']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    row = get_reports_rows(stream_data)[0]
    if app.debug:
        print (row)
    ctr = float(row[header.index('Ctr')].strip('%') ) / 100
    res['flag'] = 'green'
    if (ctr < 0.03):
        res['flag'] = 'amber'
    if ctr < 0.02:
        res['flag'] = 'red'
    return flask.jsonify(res)

def ave_position(adwords_client, item):
    res = {}
    res['description'] = item['description']

    report_downloader = adwords_client.GetReportDownloader(version='v201809')

    # Create report query.
    report_query = (adwords.ReportQueryBuilder()
                  .Select('AdNetworkType1', 'CustomerDescriptiveName', 'Clicks', 'AveragePosition')
                  .From('ACCOUNT_PERFORMANCE_REPORT')
                  .Where('AdNetworkType1').EqualTo('SEARCH')
                  .During(DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['AdNetworkType1', 'CustomerDescriptiveName', 'Clicks', 'AveragePosition']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    row = get_reports_rows(stream_data)[0]
    if app.debug:
        print (row)
    av_position = float(row[header.index('AveragePosition')])
    res['flag'] = 'green'
    if (av_position > 2.1):
        res['flag'] = 'amber'
    return flask.jsonify(res)

def have_trials(adwords_client, item):
    res = {}
    res['description'] = item['description']

    report_downloader = adwords_client.GetReportDownloader(version='v201809')

    # Create report query.
    report_query = (adwords.ReportQueryBuilder()
                  .Select('AdNetworkType1', 'CustomerDescriptiveName', 'Clicks', 'Cost')
                  .From('ACCOUNT_PERFORMANCE_REPORT')
                  .Where('AdNetworkType1').EqualTo('SEARCH')
                  .During(DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['AdNetworkType1', 'CustomerDescriptiveName', 'Clicks', 'Cost']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    row = get_reports_rows(stream_data)[0]
    if app.debug:
        print (row)
    res['flag'] = 'amber'
    cost = float(row[header.index('Cost')]) / 1000000
    if cost > 3000:
        if app.debug:
            print (f"checking trials, budget {cost} is over 3000")
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

    adwords_client = get_adwords_client()
    adwords_client.SetClientCustomerId(customerId)
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
            'listed': True
        }
    ]
    results = []
    spreadsheet_body = {
        'properties': {
            'title': 'Dummy Sheet'
        },
        "sheets": [
        {
        "properties": {
            "title": 'All Ckecks'
            }
        }
        # }
        # {
        #   "properties": {
        #        "title": 'Second Sheet'
        #   },
        # }
        ]
    }
    for item in checks:
        results.append(item['apply'](adwords_client, item, list=True))
        if item.get('listed', False):
            spreadsheet_body['sheets'].append({ "properties": { "title": item['name']}})
    sheets_service = googleapiclient.discovery.build('sheets', 'v4', credentials=creds)

    request = sheets_service.spreadsheets().create(body=spreadsheet_body)
    response = request.execute()
    sheet_url = response.get('spreadsheetUrl')
    sheet_id = response.get('spreadsheetId')
    main_results = []
    for index, item in enumerate(results):
        main_results.append([item['description'], item['flag']])
        if checks[index].get('listed', False):
            body = {
                'values': item.get('rows', [[]])
            }

            result = sheets_service.spreadsheets().values().update(
            spreadsheetId=sheet_id, range=checks[index]['name'],
            valueInputOption='RAW', body=body).execute()
    body = {
        'values': main_results
    }
    result = sheets_service.spreadsheets().values().update(
    spreadsheetId=sheet_id, range='All Ckecks',
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
            'title': 'Dummy Sheet'
        },
        "sheets": [
           {
             "properties": {
             "title": 'All Ckecks'
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

def get_campain_ids(client):
    # returns all campaigns IDs
    campaign_service = client.GetService('CampaignService', version='v201809')
    campaigns_selector = {
      'fields': ['Id', 'Name', 'Status']
    }
    campaigns = campaign_service.get(campaigns_selector)
    campaign_ids = []
    if 'entries' in campaigns:
        print (f"fonud {campaigns.totalNumEntries}")
        for campaign in campaigns['entries']:
            campaign_ids.append(campaign['id'])
            print(f"campaign name {campaign.name}")
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
