import argparse
import csv
from collections import defaultdict
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
import googleapiclient
from googleapiclient.discovery import build
import pandas as pd
from os import path as pth
import datetime
import time
from urllib.parse import urlparse, parse_qsl, unquote_plus

class Url(object):
    '''A url object that can be compared with other url orbjects
    without regard to the vagaries of encoding, escaping, and ordering
    of parameters in query strings.'''

    def __init__(self, url):
        parts = urlparse(url)
        _query = frozenset(parse_qsl(parts.query))
        _path = unquote_plus(parts.path)
        parts = parts._replace(query=_query, path=_path)
        self.parts = parts

    def __eq__(self, other):
        return self.parts == other.parts

    def __hash__(self):
        return hash(self.parts)


def CheckMatch(url, match_term):
    return Url(url) == Url(match_term)

def execute_request(service, property_uri, request):
    return service.searchanalytics().query(siteUrl=property_uri, body=request).execute()

def GetClientSecrets():
    path = input("\nEnter client secrets path: ")
    if pth.exists(path):
        return pth.abspath(path)
    return False


def MakeRequest(start_row, url):
    print('Numrows at the start of loop: %i' % start_row)
    try:
        request = {
                'startDate': start_date,
                'endDate': end_date,
                'dimensions': ['page'],
                'rowLimit': maxRows,
                'aggregationType' : 'byPage',
                'startRow': start_row
            }
        response = execute_request(webmasters_service, url, request)
        try:
            #https?:\/\/(.+?)(\/.*)
            for row in response['rows']:
                if CheckMatch(url,row['keys'][0]):
                    scDict['page'].append(row['keys'][0] or 0)
                    scDict['clicks'].append(row['clicks'] or 0)
                    scDict['impressions'].append(row['impressions'] or 0)
                    scDict['position'].append(row['position'] or 0)

            # Increment the 'start_row'
            start_row = start_row + len(response['rows'])
        except KeyError:
            return -1  # reached end of request
        print('Numrows at the end of loop: %i' % start_row)
        if start_row % maxRows != 0:  # If numRows not divisible by 25k...
            return -1
    except googleapiclient.errors.HttpError:
        print("Retrying...")
        time.sleep((10))
        MakeRequest(start_row)
    return start_row


def ValiDate(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        print("Incorrect data format, should be YYYY-MM-DD")
        return False
    return date_text


def ValiDateEndDate(start, end):
    try:
        end_date = datetime.datetime.strptime(end, '%Y-%m-%d')
        if (end_date < datetime.datetime.strptime(start, '%Y-%m-%d')):
            print("End date must be greater or equal to start date.")
            return False
    except ValueError:
        print("Incorrect data format, should be YYYY-MM-DD")
        return False
    return end

urls_path = input("Enter path to urls: ")
client_secrets_path = None
while(not client_secrets_path):
    client_secrets_path = GetClientSecrets()
start_date, end_date = None, None
while (not start_date):
    start_date = input("Enter start date (YYYY-MM-DD): ")
    start_date = ValiDate(start_date)
while (not end_date):
    end_date = input("Enter end date (YYYY-MM-DD): ")
    end_date = ValiDateEndDate(start_date, end_date)


if (not site_url or not country_urls or not client_secrets_path
        or not start_date or not end_date):
    print("Invalid values.")
else:
    SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
    DISCOVERY_URI = ('https://www.googleapis.com/discovery/v1/apis/customsearch/v1/rest')
    scDict = defaultdict(list)

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[tools.argparser])
    flags = parser.parse_args([])

    flow = client.flow_from_clientsecrets(
        client_secrets_path, scope=SCOPES,
        message=tools.message_if_missing(client_secrets_path))

    storage = file.Storage('searchconsolereporting.dat')
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)
    http = credentials.authorize(http=httplib2.Http())

    webmasters_service = build('webmasters', 'v3', http=http)

    site_list = webmasters_service.sites().list().execute()

    maxRows = 25000
    numRows = 0
    urls = pd.read_csv(urls_path)['input'].tolist()
    for url in urls:
        while (numRows >= 0):
            numRows = MakeRequest(numRows, url)
        if scDict:
            df = pd.DataFrame(data=scDict)
            df.drop_duplicates(keep = False, inplace = True) # Insurance measure...
            output_name = site_url[-3:-1].upper() + "_" + start_date + "_" + end_date
            df.to_csv(output_name + ".csv", header=True, index=False)
        else:
            print("Blank query!")