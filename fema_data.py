
from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
import json
import requests
from datetime import datetime, timedelta


FEMA_API_ENDPOINT = "http://www.fema.gov/api/open/v1/DisasterDeclarationsSummaries"
# reference: 
# 
# http://www.fema.gov/openfema-api-documentation
# http://www.fema.gov/openfema-dataset-disaster-declarations-summaries-v1

# fields to retrieve

FIELDS = ['disasterNumber',
					'state',
					'declarationDate',
					'incidentType',
					'title',
					'incidentBeginDate',
					'incidentEndDate',
					'declaredCountyArea',
					'lastRefresh']

# lookback date, in days
LOOKBACK_DAYS = 365 * 75

# number of records per page
PAGE_LENGTH = 1000

# create SQLAlchemy db engine
db = SQLAlchemy()

# data model
class Disaster(db.Model):
	"""Store data about FEMA disasters"""

	__tablename__ = 'disasters'

	id = db.Column(
				db.Integer,
        primary_key=True,
        autoincrement=True,)

	disaster_number = db.Column(
				db.Integer,
				nullable=False,) 

	state = db.Column(
				db.String(2),)

	declaration_date = db.Column(
				db.Date,) 

	incident_type = db.Column(
				db.String(20),)

	title = db.Column(
				db.String(100),) 

	incident_begin_date = db.Column(
				db.Date,) 

	incident_end_date = db.Column(
				db.Date,) 

	county = db.Column(
				db.String(100),) 

	last_refresh = db.Column(
				db.Date,) 

	fema_id = db.Column(
				db.String(30),) 


	def __repr__(self):
		"""representation of a disaster"""

		return "<Disaster id={} type={} title={} state={} date={}>".format(
			self.id, self.incident_type, self.title, self.state, self.declaration_date)

def connect_to_db(app):
    """Connect to database."""

    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql:///fema'
    # app.config['SQLALCHEMY_ECHO'] = True
    db.app = app
    db.init_app(app)


def load_data_to_db(json_data):
	"""Load API results (json) into database"""

	for disaster in json_data:
		
		# clean up county
		if disaster['declaredCountyArea'][-9:] == ' (County)':
			county = disaster['declaredCountyArea'][:-9]
		else:
			county = None

		# check if this one's already in our db; if so, update
		exist = Disaster.query.filter_by(fema_id = disaster['id']).all()
		if exist:
				print ("deleting id ".format(disaster['id']))
				Disaster.query.filter_by(fema_id = disaster['id']).delete()

		# create a Disaster object
		disaster_row = Disaster(
			disaster_number = disaster['disasterNumber'],
			state = disaster['state'],
			declaration_date = disaster['declarationDate'],
			incident_type = disaster['incidentType'],
			title = disaster['title'],
			incident_begin_date = disaster['incidentBeginDate'],
			incident_end_date = disaster['incidentEndDate'],
			county = county,
			last_refresh = disaster['lastRefresh'],
			fema_id = disaster['id'])

		db.session.add(disaster_row)
	
	db.session.commit()

def get_data():
	"""Use the FEMA API to get disaster data"""

	# examples:
	# http://www.fema.gov/api/open/v1/DisasterDeclarationsSummaries?$select=disasterNumber,state,disasterType - returns only the disasterNumber,state,disasterType and _id fields. If no value is specified, all of the fields are returned.
	# http://www.fema.gov/api/open/v1/DisasterDeclarationsSummaries?$select=disasterNumber,state,incidentBeginDate,incidentEndDate - returns only the disasterNumber, state, incidentBeginDate, incidentEndDate and _id fields. If no value is specified, all of the fields are returned.
	# /DisasterDeclarationsSummaries?$filter=declarationDate gt '2013-01-01T04:00:00.000z'

	finished = False
	iterations = 0

	while(not finished):

		start_date = datetime.now() - timedelta(days=LOOKBACK_DAYS)
		start_date_param = "incidentEndDate gt '{}'".format(
														datetime.strftime(start_date, "%Y-%m-%d"))

		payload = {'$select': ','.join(FIELDS),
							 '$top' : PAGE_LENGTH,
							 '$skip' : iterations * PAGE_LENGTH,
							 '$filter': start_date_param}

		r = requests.get(
	    FEMA_API_ENDPOINT,
	    params=payload)

		# try:
		data = r.json()['DisasterDeclarationsSummaries']
		# except:
		# 	print "Error retrieving data: " + r.json()
		# 	exit()

		load_data_to_db(data)

		if len(data) < 1000:
			finished = True
			print "got only {} records. We're done here after {} iterations.".format(
								len(data), iterations)
		else:
			print "{} records".format((iterations + 1) * PAGE_LENGTH)
			iterations += 1

# set up the db connection
app = Flask(__name__)
app.secret_key = "SECRET!"

connect_to_db(app)
db.create_all()
Disaster.query.delete()
json_results = get_data()
