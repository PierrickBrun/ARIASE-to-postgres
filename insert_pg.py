from pgdb import connect
import os
import time
import datetime
t1 = datetime.datetime.now()
con = connect(dbname='pierrick', host='localhost', port=5432, user='pierrick', password='pierrick')

cursor = con.cursor();
folder = '/home/pierrick/Energie/IRISE/data/'
for fn in os.listdir(folder):
    print(fn)
#fn = '1000080-2000900-3009900.txt'
    ids = fn.rpartition('.')[0].split('-')
    with open(folder+fn) as f:
        old_values = [0, 0, 0, 0]
        index = 0
        props = {}
        measures = []
        for line in f:
            if index < 3:
                splitted_line = line.splitlines()[0].split(' : ')
                props[splitted_line[0]] = splitted_line[1];
            if index == 4:
                print(props['APPLIANCE'])
                id_app = ids[2]
                id_house = ids[1]
                id_neighbor = ids[0]
                cursor.execute("INSERT INTO appliance (id, name, id_household, id_neighborhood) VALUES ("+id_app+", '"+props['APPLIANCE']+"', "+id_house+", "+id_neighbor+")")
            if index > 5:
                values = line.splitlines()[0].split('\t')
                if (int(values[2]) != int(old_values[2]) or int(values[3]) != int(old_values[3])):
                    print(values)
                    old_values = values
                    cursor.execute("INSERT INTO measure (id_appliance, time_stamp, energy, state) VALUES ("+id_app+", TIMESTAMP '"+time.strftime('%Y-%m-%dT%H%M%SZ',time.strptime(values[0]+'H'+values[1], '%d/%m/%yH%H:%M'))+"', "+values[2]+", "+values[3]+")")

            index = index + 1

con.commit()
con.close()
t2 = datetime.datetime.now()
diff = t2 - t1
print  diff
