from influxdb import InfluxDBClient
from influxdb import SeriesHelper
import os
import time

# InfluxDB connections settings
host = 'localhost'
port = 8086
user = 'root'
password = 'root'
dbname = 'test_irise'

myclient = InfluxDBClient(host, port, user, password, dbname)

# Uncomment the following code if the database is not yet created
# myclient.create_database(dbname)
# myclient.create_retention_policy('awesome_policy', '3d', 3, default=True)


class MySeriesHelper(SeriesHelper):
    # Meta class stores time series helper configuration.
    class Meta:
        # The client should be an instance of InfluxDBClient.
        client = myclient
        # The series name must be a string. Add dependent fields/tags in curly brackets.
        series_name = '{household}.{appliance}'
        # Defines all the fields in this time series.
        fields = ['time', 'state', 'energy']
        # Defines all the tags for the series.
        tags = ['household', 'appliance']
        # Defines the number of data points to store prior to writing on the wire.
        bulk_size = 5
        # autocommit must be set to True when using bulk_size
        autocommit = True

folder = '/home/pierrick/Energie/IRISE/data/'
for fn in os.listdir(folder):
        print(fn)
        with open(folder+fn) as f:
            index = 0
            props = {}
            measures = []
            for line in f:
                if index < 3:
                    splitted_line = line.splitlines()[0].split(' : ')
                    props[splitted_line[0]] = splitted_line[1];

                if index > 5:
                    values = line.splitlines()[0].split('\t')
                    MySeriesHelper(household=props['HOUSEHOLD'], appliance=props['APPLIANCE'], time=time.strftime('%Y-%m-%dT%H%M%SZ',time.strptime(values[0]+'H'+values[1], '%d/%m/%yH%H:%M')), state=int(values[2]), energy=int(values[3]))

                index = index + 1
