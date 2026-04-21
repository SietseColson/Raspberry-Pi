Uhmmmm, yeah so these are the files that should run on the Raspberry Pi.
I tried to keep things organized.

The cv_monitor.py script determines the egg- and chickencount, and uploads them to the right table in the DB

The sensor_station.py script: 
- reads all the sensorvalues
- classifies these values into shiiii like 'normal' or 'critical'
- calculates feeder and drinker levels
- calculates mold_risk
- uploads all of this to the right table in the DB

db_utils.py establishes connection to the database, and contains the functions called by the other 2 scripts to write or read in a table in the DB. E.g. the funciton insert_sensor_readings() is defined in db_utils, it is imported into sensor_station.py so that this script can upload the sensor readings with 1 line of code!

In the systemctl_services directory, you'll find the two current .service files. These are needed to make the RP automatically start the cv_monitor and sensor_station scripts upon powering the Raspberry Pi.

*The heatmap generation and crowding assesment are not yet included in the cv_monitor script i think, but they should. Every 2 days, a crowding assesment is perfomed, and i think it is best if this is written to the device_control table in the DB. We'll add one column, e.g. crowding_assesment, and each time the RP does the assemsent, the value is written to this cell. This means we don't keep a history of the crowding, but only know the most recent result, but I think that is fine.

*I think automation should be part of the main loop of sensor_station.py, but maybe it is easier if we keep it appart... Anyways, we'll have to add some things to db_utils.py for updating and reading device_control table in the DB.
