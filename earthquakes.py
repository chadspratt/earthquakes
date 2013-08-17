# -*- coding: utf-8 -*-
"""
Created on Thu Aug 15 23:06:56 2013

@author: chad
"""
import csv
import json
import re
import urllib
import zipfile

# extends sqlite with spatial abilities
from pyspatialite import dbapi2 as db

cityurl = 'http://download.geonames.org/export/dump/cities15000.zip'
cityfilename = 'cities15000.txt'
cityfilefields = ['geonameid', 'name', 'asciiname', 'alternatenames',
                  'latitude', 'longitude', 'feature class', 'feature code',
                  'country code', 'cc2', 'admin1 code', 'admin2 code',
                  'admin3 code', 'admin4 code', 'population', 'elevation',
                  'dem', 'timezone', 'modification date']
earthquakeurl = 'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson'
earthquakefilename = 'all_week.geojson'


# load the cities into a spatialite table
# relatively quick, but this is only needed once or rarely anyways
def importcities(filename, minpopulation):
    """Load all cities with a given minimum population into a database."""
    with open(filename, 'rb') as inputfile:
        nonulls = (line.replace('\0', '') for line in inputfile)
        reader = csv.DictReader(nonulls, fieldnames=cityfilefields,
                                delimiter='\t')
        conn = db.connect('temp.db')
        cur = conn.cursor()
        # create the table for storing the cities
        cur.execute('CREATE TABLE cities(name TEXT NOT NULL)')
        # add a geometry column (table, field, coord system, type, dimensions)
        # 4326 is the EPSG SRID for WGS 84
        cur.execute("SELECT AddGeometryColumn('cities', 'geom', 4326, " +
                                             "'POINT', 'XY')")
        for row in reader:
            citysize = int(row['population'])
            if citysize < minpopulation:
                continue
            cityname = row['asciiname']
            # escape apostrophes in city names
            if re.search("'", cityname):
                cityname = re.sub("'", "''", cityname)
            citygeom = "GeomFromText('POINT("
            citygeom += str(row['latitude']) + " " + str(row['longitude'])
            citygeom += ")', 4326)"
            query = "INSERT INTO cities(name, geom) "
            query += "VALUES ('" + cityname + "', " + citygeom + ")"
            cur.execute(query)
        conn.commit()
        conn.close()


def importearthquakes(filename, minmagnitude):
    """Load all earthquakes with a given minimum magnitude into a database ."""
    conn = db.connect('temp.db')
    cur = conn.cursor()
    # create the table for storing the cities
    cur.execute('CREATE TABLE earthquakes(id TEXT NOT NULL, mag INTEGER)')
    # add a geometry column (table, field, coord system, type, dimensions)
    # 4326 is the EPSG SRID for WGS 84
    cur.execute("SELECT AddGeometryColumn('earthquakes', 'geom', 4326, " +
                                         "'POINT', 'XY')")
    earthquakefile = open(filename)
    parsedfile = json.load(earthquakefile)
    earthquakefile.close()
    for feature in parsedfile['features']:
        magnitude = feature['properties']['mag']
        if magnitude < minmagnitude:
            continue
        eqid = feature['id']
        # eqcoords = [lat, long]
        eqcoords = feature['geometry']['coordinates']
        eqgeom = "GeomFromText('POINT("
        eqgeom += str(eqcoords[1]) + " " + str(eqcoords[0])
        eqgeom += ")', 4326)"
        query = "INSERT INTO earthquakes(id, mag, geom) "
        query += "VALUES ('" + eqid + "', " + str(magnitude) + ", " + eqgeom + ")"
        cur.execute(query)
    conn.commit()
    conn.close()


def getearthquakes():
    """Get records for all earthquakes, which consist of id and magnitude."""
    conn = db.connect('temp.db')
    cur = conn.cursor()
    cur.execute("SELECT * FROM earthquakes")
    outputearthquakes(cur)
    conn.close()


def outputearthquakes(queryresult):
    """Output the earthquakes with their magnitude to a csv file."""
    fieldnames = ['earthquake id', 'magnitude']
    with open('result_earthquakes.csv', 'w') as outputfile:
        outputfile.truncate(0)
        # might be marginally faster to just output it manually
        # outputfile.write(','.join(record) + '\n')
        writer = csv.DictWriter(outputfile, fieldnames, delimiter='\t')
        writer.writeheader()
        for record in queryresult:
            outputrow = {'earthquake id': record[0],
                         'magnitude': record[1]}
            writer.writerow(outputrow)


# perform the query and output the result to a csv file
def getcitiesnearearthquakes(kmdist):
    """Find all cities within a given km distance of an earthquake."""
    dist = kmdist * 1000
    query = "SELECT e.id, c.name, ST_Distance(e.geom, c.geom) "
    query += "FROM earthquakes AS e, cities AS c "
    # Distance and PtDistWithin don't seem to use the same method of measuring
    # distance
#    query += "WHERE PtDistWithin(e.geom, c.geom, " + str(dist) + ", 1)"
    query += "WHERE PtDistWithin(e.geom, c.geom, " + str(dist) + ")"
    conn = db.connect('temp.db')
    cur = conn.cursor()
    cur.execute(query)
    outputcities(cur)
    conn.close()


def outputcities(queryresult):
    """Write the result of querying cities near earthquakes to a csv file."""
    fieldnames = ['earthquake id', 'city name', 'distance']
    with open('result_cities.csv', 'w') as outputfile:
        outputfile.truncate(0)
        # might be marginally faster to just output it manually
        # outputfile.write('\t'.join(record) + '\n')
        writer = csv.DictWriter(outputfile, fieldnames, delimiter='\t')
        writer.writeheader()
        for record in queryresult:
            outputrow = {'earthquake id': record[0],
                         'city name': record[1],
                         # convert from degrees to kilometers
                         'distance': record[2] * 111.12}
            #PtDistWithin gives some questionable results, so clamp output here
            if outputrow['distance'] > 200:
                continue
            # unescape apostrophes
            if re.search("''", outputrow['city name']):
                outputrow['city name'] = re.sub("''", "'", outputrow['city name'])
            writer.writerow(outputrow)


def downloadearthquakes(url, filename):
    urllib.urlretrieve(url, filename)


def downloadcities(url, filename):
    urllib.urlretrieve(url, filename + '.zip')
    with zipfile.ZipFile(filename + '.zip') as cityfile:
        cityfile.extractall()

if __name__ == '__main__':
    # clear the temp db
    dbfile = open('temp.db', 'w')
    dbfile.truncate(0)
    dbfile.close()
    conn = db.connect('temp.db')
    cur = conn.cursor()
    # special function to add spatial support to the db
    cur.execute('SELECT InitSpatialMetadata()')
    conn.commit()
    conn.close()
    downloadearthquakes(earthquakeurl, earthquakefilename)
    downloadcities(cityurl, cityfilename)
    # import cities with more than 100k population
    importcities(cityfilename, 100000)
    # load all earthquakes with greater than 4.5 magnitude
    importearthquakes(earthquakefilename, 4.5)
    # get and output to file all earthquakes
    getearthquakes()
    # get and output to file all cities within 200km of an earthquake
    getcitiesnearearthquakes(200)
