# -*- coding: utf-8 -*-
"""
Created on Thu Aug 15 23:06:56 2013

@author: chad
"""
import csv
import json
import re

# extends sqlite with spatial abilities
from pyspatialite import dbapi2 as db

cityfilename = 'cities15000.txt'
cityfilefields = ['geonameid', 'name', 'asciiname', 'alternatenames',
                  'latitude', 'longitude', 'feature class', 'feature code',
                  'country code', 'cc2', 'admin1 code', 'admin2 code',
                  'admin3 code', 'admin4 code', 'population', 'elevation',
                  'dem', 'timezone', 'modification date']
earthquakefilename = 'all_week.geojson'


# load the cities into a spatialite table
# relatively quick, but this is only needed once or rarely anyways
def importcities(filename, minpopulation):
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
            citysize = row['population']
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
    conn = db.connect('temp.db')
    cur = conn.cursor()
    # create the table for storing the cities
    cur.execute('CREATE TABLE earthquakes(id TEXT NOT NULL)')
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
        eqgeom += str(eqcoords[0]) + " " + str(eqcoords[1])
        eqgeom += ")', 4326)"
        query = "INSERT INTO earthquakes(id, geom) "
        query += "VALUES ('" + eqid + "', " + eqgeom + ")"
        cur.execute(query)
    conn.commit()
    conn.close()


# perform the query and output the result to a csv file
def performselection(kmdist):
    dist = kmdist * 1000
    query = "SELECT e.id, c.name, Distance(e.geom, c.geom) "
    query += "FROM earthquakes AS e, cities as c "
    query += "WHERE PtDistWithin(e.geom, c.geom, " + str(dist) + ")"
    conn = db.connect('temp.db')
    cur = conn.cursor()
    cur.execute(query)
    outputtocsv(cur)
    conn.close()


def outputtocsv(queryresult):
    print 'starting output'
    fieldnames = ['earthquake id', 'city name', 'distance']
    with open('result.csv', 'w') as outputfile:
        outputfile.truncate(0)
        # might be marginally faster to just output it manually
        # outputfile.write(','.join(record) + '\n')
        writer = csv.DictWriter(outputfile, fieldnames)
        writer.writeheader()
        for record in queryresult:
            outputrow = {'earthquake id': record[0],
                         'city name': record[1],
                         'distance': record[2]}
            writer.writerow(outputrow)


if __name__ == '__main__':
    # clear the temp db
    dbfile = open('temp.db', 'w')
    dbfile.truncate(0)
    dbfile.close()
    conn = db.connect('temp.db')
    cur = conn.cursor()
    cur.execute('SELECT InitSpatialMetadata()')
    conn.commit()
    conn.close()
    importcities(cityfilename, 100000)
    importearthquakes(earthquakefilename, 4.5)
    performselection(200)
