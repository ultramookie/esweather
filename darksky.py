#!/usr/bin/env python

import json
import urllib
import urllib2
import time
import re
from elasticsearch import Elasticsearch

# Using Dark Sky
# Info and API key here:
# https://developer.forecast.io
APIKEY = ''

# lat and lon of where you want to get weather from
lat = ''
lon = ''

# A short all lowercase name for location
# example: "sanjoseca"
index_id = ''

# Your Elasticsearch cluster and port
# Usually localhost and port 9200
elastichost='localhost:9200'

# Build URL for getting information
base_url = 'https://api.forecast.io'
url = "/forecast/" + APIKEY  + "/" + str(lat) + "," + str(lon) + "?exclude=[minutely,hourly,daily,alerts,flags]"

# Connect to Elasticsearch cluster
es = Elasticsearch(elastichost)

# Get sh*t
req = urllib2.Request('%s%s' % (base_url, url), None)
r = urllib2.urlopen(req)
weather = json.loads(r.read())
r.close()

# Get month and year for later index name building
month = time.strftime('%m', time.localtime(weather.get('currently.time')))
year = time.strftime('%Y', time.localtime(weather.get('currently.time')))

# Create coordinates object 
latitude = weather.get('latitude')
longitude = weather.get('longitude')
coord_latlng = '{"lat": ' + str(latitude) + "," + '"lon": ' + str(longitude) + '}'
coord_json = json.loads(coord_latlng)
weather['coord'] = coord_json

# Create date 
cur_dt = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(weather.get('currently.time')))
weather['dt'] = cur_dt

# Create an index name based on user input, year and month for partitioning
index = 'darksky-' + index_id + "-" + str(year) + "-" + str(month)

# Put doc into Elasticsearch
es.index(index=index, doc_type="weather", body=weather)
