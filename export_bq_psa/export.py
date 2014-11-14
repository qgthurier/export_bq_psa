# install new lib : pip install httplib2 -t /home/qgthurier/eclipse/export_bq_psa

#!/usr/bin/env python
# coding: utf8 

import codecs
import httplib2
import logging
import os
import urllib
import webapp2
import cgi 
import csv
import json
import cloudstorage 
from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.appengine import AppAssertionCredentials
from google.appengine.api import mail
from datetime import date, timedelta, datetime

BUCKET = "/havas-bigquery-export"
FILTERS = BUCKET + "/filters.csv"
LOOKUP = BUCKET + "/lookup.csv"
JOB_IDS = BUCKET + "/jobs.csv"
INFORMED = BUCKET + "/informed.csv"
BILLING_PROJECT_ID = "282649517306"
HAVAS_PROJECT_ID = "1059654567191"
PREFIX = "ga_sessions_"

SCOPE = 'https://www.googleapis.com/auth/bigquery'
HTTP = AppAssertionCredentials(scope=SCOPE).authorize(httplib2.Http())

class Launch(webapp2.RequestHandler):
    
    def initialization(self):
        self.bq_service = build('bigquery', 'v2', http=HTTP)
        self.args = self.parse_get_parameters()    
        self.filters = self.read_filters_file()
        self.views = self.read_lookup_file()
    
    def read_filters_file(self):
        file = cloudstorage.open(FILTERS, 'r')
        csv_reader = csv.reader(iter(file.readline, ''), delimiter=',', quotechar='"')
        next(csv_reader)  # skip the headers
        filters = {}
        for row in csv_reader:
            key = row[0] + row[1]
            if key in filters.keys():
                filters[key] += [row[2].replace("http://","").rstrip('/')]
            else:
                filters[key] = [row[2].replace("http://","").rstrip('/')]
        return filters
    
    def read_lookup_file(self):
        file = cloudstorage.open(LOOKUP, 'r')
        csv_reader = csv.reader(iter(file.readline, ''), delimiter=',', quotechar='"')
        next(csv_reader)  # skip the headers
        views = {}
        for row in csv_reader:
            key = row[0] + row[1]
            views[key] = row[2]
        return views
    
    def parse_get_parameters(self):
        get = cgi.FieldStorage()
        try:
          date = get['date'].value
        except:
          yesterday =  datetime.today() - timedelta(1)
          date = yesterday.strftime('%Y%m%d')
        return {'date': date}
        
    def make_query_config(self, query, key):
        return {"configuration": {
                  "query": {
                    "query": query,
                    "destinationTable": {
                      "projectId": HAVAS_PROJECT_ID,
                      "datasetId": self.views[key],
                      "tableId": PREFIX + self.args['date']
                    },
                    "useQueryCache": False,
                    "allowLargeResults": True,
                    "flattenResults": False,
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_EMPTY",
                    #"priority": "BATCH"
                  }
                }}
            
    def make_where_clause(self, key):
        sep = "' or hits.page.hostname contains '"
        return "hits.page.hostname contains '" + sep.join(self.filters[key]) + "'"
    
    def make_from_clause(self, key):
        return "flatten([" + self.views[key] + "." + PREFIX + self.args['date'] + "], customDimensions)" 
    
    def get(self):         
        # initialize parameters 
        self.initialization()
        # check if a table exist : https://github.com/tylertreat/BigQuery-Python/blob/master/bigquery/client.py
        file = cloudstorage.open(JOB_IDS, 'w', content_type='text/csv')
        file.write("table,job_id")
        for k, v in self.views.iteritems():
            where_clause = self.make_where_clause(k)
            from_clause = self.make_from_clause(k)
            query = "select * from %s where %s" % (from_clause, where_clause)
            job = self.bq_service.jobs().insert(projectId=BILLING_PROJECT_ID, body=self.make_query_config(query, k)).execute()
            table = self.views[k] + "." + PREFIX + self.args['date']
            id = job['jobReference']['jobId']
            file.write("\n" + table.encode('utf-8') + "," + id.encode('utf-8'))
        file.close()
        self.response.out.write("extracts have been launched")



class Check(webapp2.RequestHandler):
    
    def initialization(self):
        self.bq_service = build('bigquery', 'v2', http=HTTP)    
        self.jobs = self.read_jobs_file()
        self.emails = self.read_informed_file()
    
    def read_jobs_file(self):
        file = cloudstorage.open(JOB_IDS, 'r')
        csv_reader = csv.reader(iter(file.readline, ''), delimiter=',', quotechar='"')
        next(csv_reader)  # skip the headers
        jobs = {}
        for row in csv_reader:
            key = row[0]
            jobs[key] = row[1]
        return jobs
    
    def read_informed_file(self):
        file = cloudstorage.open(INFORMED, 'r')
        csv_reader = csv.reader(iter(file.readline, ''), delimiter=',', quotechar='"')
        next(csv_reader)  # skip the headers
        emails = []
        for row in csv_reader:
            emails.append(row[0])
        return emails
    
    def get(self):         
        self.initialization()
        message = mail.EmailMessage(sender="<qgthurier@netbooster.com>", subject="Status for Havas daily extract")
        message.to = "<" + ">,<".join(self.emails) + ">"
        message.html = "<html><head></head><body><b>Extract report:</b>\n<ul>"
        message.body = "Extract report:\n"
        for k, v in self.jobs.iteritems():
            res = self.bq_service.jobs().get(projectId=BILLING_PROJECT_ID, jobId=v).execute()
            if "errors" in res['status'].keys():
                message.html += "\n<li>" + "job " + v + " for table " + k + " has failed</li>"
                message.body += "\n" + "job " + v + " for table " + k + " has failed "
            else:
                try:
                    ds_id = k.split(".")[0]
                    table_id = k.split(".")[1]
                    table = self.bq_service.tables().get(projectId=HAVAS_PROJECT_ID, datasetId=ds_id, tableId=table_id).execute()
                except HttpError:
                    table = None
                if not table:
                    message.html += "\n" + "<li>job " + v + " for table " + k + " is " + res['status']['state'] + " & extract is unavailable</li>"
                    message.body += "\n" + "job " + v + " for table " + k + " is " + res['status']['state'] + " & extract is unavailable"
                else:
                    message.html += "\n" + "<li>job " + v + " for table " + k + " is " + res['status']['state'] + " & extract is available</li>"
                    message.body += "\n" + "job " + v + " for table " + k + " is " + res['status']['state'] + " & extract is available"
        message.html += "\n</ul></body></html>"
        message.send()
        self.response.out.write("email has been sent")    

         
app = webapp2.WSGIApplication(
    [
     ('/export/launch', Launch),
     ('/export/check', Check)
    ],
    debug=True)