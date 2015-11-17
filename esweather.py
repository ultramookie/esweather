#!/usr/bin/env python

import json
import urllib
import urllib2
import time
from elasticsearch import Elasticsearch

# Using openweathermap.org.
# Info and API key here:
# http://openweathermap.org/api 
APPID = ''
lat = ''
lon = ''
# can be "metric" or "imperial"
units = 'imperial'
elastichost='localhost:9200'

base_url = 'http://api.openweathermap.org'
url = "/data/2.5/weather?lat=" + str(lat) + "&lon=" + str(lon) + "&units=" + units + "&APPID=" + APPID

es = Elasticsearch(elastichost)

req = urllib2.Request('%s%s' % (base_url, url), None)
r = urllib2.urlopen(req)
weather = json.loads(r.read())
r.close()

month = time.strftime('%m', time.localtime(weather.get('dt')))
year = time.strftime('%Y', time.localtime(weather.get('dt')))
index_id = weather.get('id')
id = weather.get('dt')
index = 'weather-' + str(index_id) + "-" + str(year) + "-" + str(month)

es.index(index=index, doc_type="weather", id=id, body=weather)
