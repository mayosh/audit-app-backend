from flask import jsonify
import json
from googleads import adwords
import csv
import re
import logging
import redis
import datetime

PAGE_SIZE = 100
CHANGE_LIMIT = 10

def defauls_period():
    day1 = datetime.datetime.today() - datetime.timedelta(days = 1)
    date90 = day1 - datetime.timedelta(days = 89)
    return {
        'end_date': day1.strftime('%Y%m%d'),
        'start_date': date90.strftime('%Y%m%d')
    }

DEFAULT_PERFOMANCE_PERIOD = defauls_period()

# check_log = logging.getLogger()

cache = redis.Redis(decode_responses=True)

def check_wrapper(func):
    def list_wrapper(*args, **kwargs):
        item = args[1] if len(args) > 0 else None
        res = func(*args, **kwargs)
        res.update({key: item.get(key) for key in ['name', 'description', 'type'] })
        if hasattr(res, 'rows') and len(res['rows']) < 2:
            res['rows'] = []
        if item.get('score', False) and res.get('flag', False) and res.get('flag', False) != 'other':
            res['scored'] = item.get('score').get(res['flag'])
        if kwargs.get('return_list', None):
            return jsonify(res)
        else:
            return res
    return list_wrapper


"""
HELPER FUNCTIONS
"""
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
        # for campaign in campaigns['entries']:
        campaign_ids.extend([ campaign['id'] for campaign in campaigns['entries'] ])
            # print(f"campaign name {campaign.name}")
    else:
        print ('No campaigns were found.')
    return campaign_ids

def get_search_campaigns_ids(client):
    # returns all campaigns IDs
    """
    ch.lrange( "test_list22", 0, -1 ) == []
    False
    ch.lrange( "test_lis342", 0, -1 ) == []
    True

    """
    hash_name = client.client_customer_id + ':serch_ids'
    if cache.lrange(hash_name, 0, -1) != []:
        print('retrieved ids from cache')
        return cache.lrange(hash_name, 0, -1)
    else:

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
                    'values': 'ENABLED'}
            ]
        }
        campaigns = campaign_service.get(campaigns_selector)
        campaign_ids = []
        if 'entries' in campaigns:
            for campaign in campaigns['entries']:
                campaign_ids.append(campaign['id'])
        cache.rpush(hash_name, *campaign_ids)
        cache.expire(hash_name, 50)
        return campaign_ids
def p2f(x):
    return float(x.strip('%'))/100
 

"""
CHECK FUNCTIONS DEFINITONS
"""
@check_wrapper
def full_broad_exist(adwords_client, item, list=None):
    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    ids = get_search_campaigns_ids(adwords_client)
    res = {}
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

        reader = csv.reader(stream_data.split('\n'))
        for row in reader:
            if row != []:
                affected.append(row)
        res['rows'] = affected

        if len(affected) > 1:
            res['flag'] = 'red'
        else:
            res['flag'] = 'green'
    return res


@check_wrapper
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

    # , dialect='excel') .split('\n')
    reader = csv.reader(stream_data.split('\n'))
    affected = []
    for row in reader:
        if row != []:
            affected.append(row)
    res = {'rows': affected}
    if len(affected) > 1:
        res['flag'] = 'amber'
    else:
        res['flag'] = 'green'
    return res

@check_wrapper
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
    if conversions.totalNumEntries > 0:
        res['flag'] = 'green'
    else:
        res['flag'] = 'red'
    return res


@check_wrapper
def short_broad_exist(adwords_client, item, list=None):
    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    ids = get_search_campaigns_ids(adwords_client)
    res = {}
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

        # , dialect='excel') .split('\n')
        reader = csv.reader(stream_data.split('\n'))
        for row in reader:
            if row != [] and len(row[0].split()) < 3:
                affected.append(row)
        res = {}
        if len(affected) > 1:
            res['flag'] = 'red'
        else:
            res['flag'] = 'green'
    res['rows'] = affected
    return res

@check_wrapper
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

    res = {'rows': affected}
    if len(affected) > 1:
        res['flag'] = 'amber'
    else:
        res['flag'] = 'green'
    return res

@check_wrapper
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
    res = { 'rows': affected }

    if len(affected) > 1:
        res['flag'] = 'amber'
    else:
        res['flag'] = 'green'
    # app.logger.info('%s complete with flag %s', inspect.currentframe().f_code.co_name, res['flag'])

    return res

@check_wrapper
def has_negatives(adwords_client, item, list=None):
    res = {}

    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    report_query = (adwords.ReportQueryBuilder()
                  .Select('CampaignName', 'Criteria')
                  .From('CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT')
                  .Build())
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True)
    rows = get_reports_rows(stream_data)

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

    if (not 'entries' in negative_keywords_lists) or (len(rows) == 0):
        res['flag'] = 'red'
    else:
        res['flag'] = 'green'
    return res

@check_wrapper
def account_url_template(adwords_client, item, list=None):
    res = {}
    res['description'] = item['description']
    res['imagename'] = item['imagename']

    customer_service = adwords_client.GetService(
        'CustomerService', version='v201809')
    offset = 0
    selector = {
        'fields': ['customerId', 'trackingUrlTemplate'],
        'paging': {
            'startIndex': str(offset),
            'numberResults': str(PAGE_SIZE)
        }
    }

    accounts = customer_service.getCustomers(selector)

    listed_account = {
        'id': accounts[0]['customerId'],
        'url': accounts[0]['trackingUrlTemplate']
    }
    #  Got None if no URL template provided
    res = {'flag': 'green' if listed_account.get('url', False) else 'red', 'urls': listed_account }

    return res

@check_wrapper
def has_changes(adwords_client, item, list=None):

    res = {}
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
            allChanges = []
            for data in account_changes['changedCampaigns'][:10]:
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
    
    res['rows'] = changes[:5]

    return res

@check_wrapper
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
    filtered_list.insert(0, ['CampaignName', 'AdGroupName', 'NumberAds'])

    res['rows'] = filtered_list[:10]
    if len(filtered_list) > 1:
        res['flag'] = 'amber'
    else:
        res['flag'] = 'green'
    return res

@check_wrapper
def search_ctr(adwords_client, item, list=None):
    res = {}
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
        ctr = float(row[header.index('Ctr')].strip('%') ) / 100
        res['flag'] = 'green'
        if (ctr < 0.03):
            res['flag'] = 'amber'
        if ctr < 0.02:
            res['flag'] = 'red'
    else:
        res['flag'] = 'other'
    return res

@check_wrapper
def ave_position(adwords_client, item, list=None):
    res = {}
    report_downloader = adwords_client.GetReportDownloader(version='v201809')

    # Create report query.
    report_query = (adwords.ReportQueryBuilder()
                  .Select('AdNetworkType1', 'CustomerDescriptiveName', 'Clicks', 'AveragePosition', 'TopImpressionPercentage')
                  .From('ACCOUNT_PERFORMANCE_REPORT')
                  .Where('AdNetworkType1').EqualTo('SEARCH')
                  .During(**DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['AdNetworkType1', 'CustomerDescriptiveName', 'Clicks', 'AveragePosition', 'TopImpressionPercentage']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    rows = get_reports_rows(stream_data)
    if rows == []:
        res['flag'] = 'other'
        return res

    av_position = float(rows[0][header.index('TopImpressionPercentage')])
    
    res['flag'] = 'green'
    if (av_position < 0.3):
        res['flag'] = 'amber'
    return res

@check_wrapper
def have_trials(adwords_client, item, list=None):
    res = {}
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

    res['flag'] = 'amber'
    cost = float(row[header.index('Cost')]) / 1000000
    if cost > 3000:

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
            res['flag'] = 'green'
    return res

@check_wrapper
def has_modifiers(adwords_client, item, list=None):
    res = {}
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

    if len(affected) > 1:
        res['flag'] = 'red'
    else:
        res['flag'] = 'green'
    res['rows'] = affected
    return res

@check_wrapper
def has_customizers(adwords_client, item, list=None):
    res = {}
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

    if feed_page['totalNumEntries'] > 0:
        res['flag'] = 'green'
    else:
        res['flag'] = 'red'
    return res

@check_wrapper
def bid_strategy(adwords_client, item, list=None):
    res = {}
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

    res['flag'] = 'green'
    if total_cost > 3000 and non_manual:
        res['flag'] = 'amber'

    res['rows'] = [header] + stat_rows
    return res

@check_wrapper
def has_customizers(adwords_client, item, list=None):
    res = {}

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

    if feed_page['totalNumEntries'] > 0:
        res['flag'] = 'green'
    else:
        res['flag'] = 'red'

    return res  

@check_wrapper
def location_interested(adwords_client, item, list=None):
    res = {}
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
    affected = [['Camp Id', 'Camp Name', 'Geo Settings']]
    for campaign in campaign_entries:
        geo_setting = next((setting for setting in campaign['settings'] if setting['Setting.Type'] == 'GeoTargetTypeSetting'), None)
        if geo_setting and geo_setting['positiveGeoTargetType']  != 'LOCATION_OF_PRESENCE':

            affected.append([ campaign['id'], campaign['name'], mapping[geo_setting['positiveGeoTargetType']] ]) #campaign['settings']

    res['flag'] = 'green'
    if len(affected) > 1:
        res['flag'] = 'amber'
    res['rows'] = affected
    return res

@check_wrapper
def extensions_in_use(adwords_client, item, list=None):
    res = {}
    name_mapping = {
        "1":"Sitelink",
        "2":"Call",
        "3":"App",
        "7":"Location",
        "30":"Affiliate location",
        "17":"Callout",
        "24":"Structured snippet",
        "31":"Message",
        "35":"Price",
        "38":"Promotion",
        }
    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    report_query = (adwords.ReportQueryBuilder()
                  .Select('ExtensionPlaceholderType','Impressions')
                  .From('PLACEHOLDER_REPORT')
                  .Where('ExtensionPlaceholderType').In(*name_mapping.keys()) #.In(*ids)
                  .Where('Impressions').GreaterThan(0) 
                  .During(**DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['ExtensionPlaceholderType','Impressions']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    rows = get_reports_rows(stream_data)
    # print(rows)
    agg = {}
    for row in rows:
        name = name_mapping.get(row[0], row[0])
        if not agg.get(name, False):
            agg[name] = 0
        agg[name] += int(row[1])


    res['flag'] = 'red'
    if len(agg.keys()) > 0:
        res['flag'] = 'amber'
    if len(agg.keys()) > 2:
        res['flag'] = 'green'
    res['rows'] = [header] + [ [key, value] for key, value in agg.items() ]
    # print (agg)
    return res

@check_wrapper
def channel_type_list(adwords_client, item, list=None):
    # AdvertisingChannelType
    res = {}
    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    report_query = (adwords.ReportQueryBuilder()
                  .Select('AdvertisingChannelType','Clicks')
                  .From('CAMPAIGN_PERFORMANCE_REPORT')
                  .Where('Clicks').GreaterThan(0) 
                  .During(**DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['AdvertisingChannelType','Clicks']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    rows = get_reports_rows(stream_data)
    agg = {}
    for row in rows:
        name = row[0]
        if not agg.get(name, False):
            agg[name] = 0
        agg[name] += int(row[1])
    res['flag'] = 'other'
    res['rows'] = [header] + [ [key, value] for key, value in agg.items() ]
    return res



@check_wrapper
def top_placements(adwords_client, item, list=None):
    # AdvertisingChannelType
    res = {}
    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    report_query = (adwords.ReportQueryBuilder()
                  .Select('Criteria','Impressions')
                  .From('PLACEMENT_PERFORMANCE_REPORT')
                  .Where('Impressions').GreaterThan(0) 
                  .During(**DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['Criteria','Impressions']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    rows = get_reports_rows(stream_data)
    rows = sorted(rows, key=lambda k:int(k[1]), reverse=True)[:10]
    # agg = {}
    # for row in rows:
    #     name = row[0]
    #     if not agg.get(name, False):
    #         agg[name] = 0
    #     agg[name] += int(row[1])
    res['flag'] = 'other'
    res['rows'] = [header] + rows
    return res

@check_wrapper
def ctr15_placements(adwords_client, item, list=None):
    # AdvertisingChannelType
    res = {}
    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    report_query = (adwords.ReportQueryBuilder()
                  .Select('Criteria','Clicks', 'Impressions')
                  .From('PLACEMENT_PERFORMANCE_REPORT')
                  .Where('Impressions').GreaterThan(100)
                #   .Where('Ctr').GreaterThan(100)
                  .During(**DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['Criteria','Clicks', 'Impressions', 'Ctr']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    rows = get_reports_rows(stream_data)

    # rows = sorted(rows, key=lambda k:int(k[2]), reverse=True)[:10]
    agg = {}
    for row in rows:
        name = row[0]
        if not agg.get(name, False):
            agg[name] = {'Clicks': 0.0, 'Impressions': 0.0}
        agg[name]['Clicks'] += int(row[1])
        agg[name]['Impressions'] += int(row[2])
    rows = [[placement_name, placement_data['Clicks'], placement_data['Impressions'], placement_data['Clicks']/placement_data['Impressions'] ] \
        for placement_name, placement_data in agg.items() \
        if  placement_data['Clicks']/placement_data['Impressions'] > 0.15]
    res['flag'] = 'other'
    res['rows'] = [header] + rows if len(rows) > 1 else rows
    return res

@check_wrapper
def cr15_placements(adwords_client, item, list=None):
    # AdvertisingChannelType
    res = {}
    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    report_query = (adwords.ReportQueryBuilder()
                  .Select('Criteria','Conversions', 'Clicks')
                  .From('PLACEMENT_PERFORMANCE_REPORT')
                  .Where('Impressions').GreaterThan(100)
                #   .Where('Ctr').GreaterThan(100)
                  .During(**DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['Criteria','Clicks', 'Impressions', 'Ctr']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    rows = get_reports_rows(stream_data)

    # rows = sorted(rows, key=lambda k:int(k[2]), reverse=True)[:10]
    agg = {}
    for row in rows:
        name = row[0]
        if not agg.get(name, False):
            agg[name] = {'Conversions': 0.0, 'Clicks': 0.0}
        agg[name]['Conversions'] += float(row[1])
        agg[name]['Clicks'] += int(row[2])
    rows = [[placement_name, placement_data['Conversions'], placement_data['Clicks'], placement_data['Conversions']/placement_data['Clicks'] ] \
        for placement_name, placement_data in agg.items() \
        if  placement_data['Conversions']/placement_data['Clicks'] > 0.15]
    res['flag'] = 'other'
    res['rows'] = [header] + rows if len(rows) > 1 else rows

    return res

@check_wrapper
def display_ks_targeting(adwords_client, item, list=None):
    # AdvertisingChannelType
    res = {}
    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    report_query = (adwords.ReportQueryBuilder()
                  .Select('Criteria','Clicks')
                  .From('DISPLAY_KEYWORD_PERFORMANCE_REPORT')
                  .Where('Impressions').GreaterThan(0) 
                  .During(**DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['Criteria','Clicks']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    rows = get_reports_rows(stream_data)
    agg = {}
    for row in rows:
        name = row[0]
        if not agg.get(name, False):
            agg[name] = 0
        agg[name] += int(row[1])
    res['flag'] = 'green' if len(rows) > 0 else 'red'
    res['rows'] = [header] + [ [key, value] for key, value in agg.items() ]
    return res

@check_wrapper
def display_topics_targeting(adwords_client, item, list=None):
    # AdvertisingChannelType
    res = {}
    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    report_query = (adwords.ReportQueryBuilder()
                  .Select('Criteria','Clicks')
                  .From('DISPLAY_TOPICS_PERFORMANCE_REPORT')
                  .Where('Impressions').GreaterThan(0) 
                  .During(**DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['Criteria','Clicks']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    rows = get_reports_rows(stream_data)
    agg = {}
    for row in rows:
        name = row[0]
        if not agg.get(name, False):
            agg[name] = 0
        agg[name] += int(row[1])
    res['flag'] = 'green' if len(rows) > 0 else 'red'
    res['rows'] = [header] + [ [key, value] for key, value in agg.items() ]
    return res

@check_wrapper
def display_audience_targeting(adwords_client, item, list=None):
    # AdvertisingChannelType
    res = {}
    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    report_query = (adwords.ReportQueryBuilder()
                  .Select('UserListName', 'Criteria','Clicks')
                  .From('AUDIENCE_PERFORMANCE_REPORT')
                  .Where('Impressions').GreaterThan(0)
                  .Where('AdNetworkType1').EqualTo('CONTENT')
                  .During(**DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['Criteria',' Clicks']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    rows = get_reports_rows(stream_data)
    agg = {}
    for row in rows:
        name = row[0] + '::' + row[1]
        if not agg.get(name, False):
            agg[name] = 0
        agg[name] += int(row[2])
    res['flag'] = 'green' if len(rows) > 0 else 'red'
    res['rows'] = [header] + [ [key, value] for key, value in agg.items() ]
    return res

@check_wrapper
def display_placement_targeting(adwords_client, item, list=None):
    # AdvertisingChannelType
    res = {}
    report_downloader = adwords_client.GetReportDownloader(version='v201809')
    report_query = (adwords.ReportQueryBuilder()
                  .Select('Criteria',' Clicks')
                  .From('PLACEMENT_PERFORMANCE_REPORT')
                  .Where('Impressions').GreaterThan(0)
                  .Where('Id').GreaterThan(0)
                  .During(**DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    header = ['Criteria',' Clicks']
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=True, skip_column_header=True)
    rows = get_reports_rows(stream_data)
    agg = {}
    for row in rows:
        name = row[0]
        if not agg.get(name, False):
            agg[name] = 0
        agg[name] += int(row[1])
    res['flag'] = 'green' if len(rows) > 0 else 'red'
    res['rows'] = [header] + [ [key, value] for key, value in agg.items() ] if len(agg.items())>0 else []
    return res

@check_wrapper
def account_stats(adwords_client, item, list=None):
    report_downloader = adwords_client.GetReportDownloader(version='v201809')

    # Create report query.
    report_query = (adwords.ReportQueryBuilder()
                  .Select('AccountCurrencyCode', 'AdNetworkType1', 'Cost', 'Conversions', 'CostPerConversion', 'Ctr',  \
                      'AbsoluteTopImpressionPercentage', 'TopImpressionPercentage')
                  .From('ACCOUNT_PERFORMANCE_REPORT')
                  .During(**DEFAULT_PERFOMANCE_PERIOD)
                  .Build())
    stream_data = report_downloader.DownloadReportAsStringWithAwql(
        report_query, 'CSV', use_raw_enum_values=True, skip_report_header=True, skip_report_summary=False, skip_column_header=False)

    rows = get_reports_rows(stream_data)
    structure = {}
    header = ['AccountCurrencyCode', 'AdNetworkType1', 'Cost', 'Conversions', 'CostPerConversion', 'Ctr']
    for row in rows[1:]:
        row[2] = int(row[2])/1000000
        row[4] = int(row[4])/1000000
    # rows = [header] + rows
    rows[-1][1] = "TOTAL"
    res = {}
    res['rows'] = rows
    return res


check_list = [
    {
        'name': 'conversions_check',
        'description': 'Any conversions set up in tools>conversions?',
        'apply': check_convesions_exist,
        'type':'account-wide',
        'score': {
            'green': 10,
            'amber': 0,
            'red':-20
        }
    },
    {
        'name': 'broad_modifiers_check',
        'description': 'Full broad matches (not modifier)',
        'apply': full_broad_exist,
        'type':'search',
        'score': {
            'green': 10,
            'amber': -10,
            'red':-20
        }
    },
    {
        'name': 'short_modifiers_check',
        'description': '1 or 2 word Broad Match Modifiers',
        'apply': short_broad_exist,
        'type':'search',
        'score': {
            'green': 10,
            'amber': -5,
            'red':-20
        }

    },
    {
        'name': 'mobile_firendly_pages',
        'description': 'Landing pages are mobile friendly',
        'apply': mobile_firendly_pages,
        'type':'account-wide',
        'score': {
            'green': 5,
            'amber': -5,
            'red':-15
        }
    },
    {
        'name': 'landing_home_pages',
        'description' :'Landing Page = homepage (no /anything at end of URL)',
        'apply': landing_home_pages,
        'type':'account-wide',
        'score': {
            'green': 5,
            'amber': 0,
            'red':-5
        }
    },
    {
        'name': 'low_quality_keywords',
        'description' :'No keywords with quality score <5',
        'apply': low_quality_keywords,
        'type':'search',
        'score': {
            'green': 10,
            'amber': 5,
            'red':-15
        }
    },
    {
        'name': 'has_negatives',
        'description' :'Has Negatives',
        'apply': has_negatives,
        'type':'search',
        'score': {
            'green': 5,
            'amber': 0,
            'red':-10
        }
    },
    {
        'name': 'account_url_template',
        'description' :'Account has an URL Template',
        'apply': account_url_template,
        'type':'account-wide',
        'score': {
            'green': 10,
            'amber': 0,
            'red':-10
        }
    },
    # {
    #     'name': 'has_changes',
    #     'description' :'Change History Has More Than 10 changes in period (last 90 days)',
    #     'apply': has_changes, # cost_per_conversions
    #     'type':'account-wide',
    #     'score': {
    #         'green': 10,
    #         'amber': 0,
    #         'red':-10
    #     }
    # },
    {
        'name': 'has_more3_ads',
        'description' :'Ad groups have three or more ads',
        'apply': has_more3_ads,
        'type':'search',
        'score': {
            'green': 10,
            'amber': 0,
            'red':-10
        }
    },
    {
        'name': 'search_ctr',
        'description' :'Search CTR is Less Than 3%',
        'apply': search_ctr, # ave_position
        'type':'search',
        'score': {
            'green': 10,
            'amber': 3,
            'red':-10
        }
    },
    {
        'name': 'ave_position',
        'description' :'Search Top Impression rate is Better than 30%',
        'apply': ave_position, # have_trials
        'type':'search',
        'score': {
            'green': 5,
            'amber': 0,
            'red':-5
        }
    },
    {
        'name': 'have_trials',
        'description' :'High spending account has experiments happening',
        'apply': have_trials, # have_trials
        'type': 'account-wide',
        'score': {
            'green': 5,
            'amber': 0,
            'red':-10
        }
    },
    {
        'name': 'has_modifiers',
        'description' :'CPC campaigns have bid modifiers in place',
        'apply': has_modifiers,
        'type':'account-wide',
        'score': {
            'green': 10,
            'amber': 0,
            'red':-10
        }
    },
    {
        'name': 'has_customizers',
        'description' :'Account has ad customized feeds',
        'apply': has_customizers, #location_interested
        'imagename': '14.png',
        'type': 'search',
        'score': {
            'green': 10,
            'amber': 0,
            'red': -10
        }
    },
    {
        'name': 'bid_strategy',
        'description' :'Account has non manual strategies (applicable for spending over 3K)',
        'apply': bid_strategy, #bid_strategy
        'imagename': '15.png',
        'listed': True,
        'type': 'account-wide',
        'score': {
            'green': 10,
            'amber': 0,
            'red': -10
        }
    },
    {
        'name': 'has_customizers',
        'description' :'Account has ad customized feeds',
        'apply': has_customizers, #location_interested
        'imagename': '14.png',
        'type': 'search',
        'score': {
            'green': 10,
            'amber': 0,
            'red': -10
        }
    },
    {
        'name': 'location_interested',
        'description' :'Location targeting set to "physically in" location, not "interested in" default',
        'apply': location_interested, #extensions_in_use
        'imagename': '16.png',
        'listed': True,
        'type': 'account-wide',
        'score': {
            'green': 10,
            'amber': 0,
            'red': -10
        }
    },
    {
        'name': 'extensions_in_use',
        'description' :'Extensions in use (min 3x types)',
        'apply': extensions_in_use, #channel_type_list
        'imagename': '16.png',
        'listed': True,
        'type': 'search',
        'score': {
            'green': 10,
            'amber': 0,
            'red': -10
        }
    },
    {
        'name': 'channel_type_list',
        'description' :'Display + Video + Shopping - which are set up/had traffic',
        'apply': channel_type_list, #top_placements
        'listed': True,
        'type': 'account-wide',
        'score': {
            'green': 10,
            'amber': 0,
            'red': -10
        }
    },
    {
        'name': 'top_placements',
        'description' :'Top Placements',
        'apply': top_placements, #top_placements
        'listed': True,
        'type': 'display',
    },
    {
        'name': 'ctr15_placements',
        'description' :'Placements with 15%+ CTR (probably fraud)',
        'apply': ctr15_placements, #top_placements
        'listed': True,
        'type': 'display',
    },
    {
        'name': 'cr15_placements',
        'description' :'Placements with 15%+ Conversion (probably not real conversions)',
        'apply': cr15_placements, #display_ks_targeting
        'listed': True,
        'type': 'display',
    },
    {
        'name': 'display_ks_targeting',
        'description' :'Keyword targeting exists',
        'apply': display_ks_targeting, #display_ks_targeting
        'listed': True,
        'type': 'display',
        'score': {
            'green': 10,
            'amber': 0,
            'red': -10
        }
    },
        {
        'name': 'display_topics_targeting',
        'description' :'Topic targeting exists',
        'apply': display_topics_targeting, #display_audience_targeting
        'listed': True,
        'type': 'display',
        'score': {
            'green': 10,
            'amber': 0,
            'red': -10
        }
    },
    {
        'name': 'display_audience_targeting',
        'description' :'Audience targeting exists',
        'apply': display_audience_targeting, #display_placement_targeting
        'listed': True,
        'type': 'display',
        'score': {
            'green': 10,
            'amber': 0,
            'red': -10
        }
    },
    {
        'name': 'display_placement_targeting',
        'description' :'Placement targeting exists',
        'apply': display_placement_targeting, #account_stats
        'listed': True,
        'type': 'display',
        'score': {
            'green': 10,
            'amber': 0,
            'red': -10
        }
    },
        {
        'name': 'account_stats',
        'description' :'Overall account stats',
        'apply': account_stats, #account_stats
        'listed': True,
        'type': 'account-wide',
        'score': {
            'green': 10,
            'amber': 0,
            'red': -10
        }
    }

    
]
