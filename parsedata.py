
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
LOOKBACK_DAYS = 3650

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
				nullable=False,
        unique=True,) 

	state = db.Column(
				db.String(2),)

	declaration_date = db.Column(
				db.Date,) 

	incident_type = db.Column(
				db.String(20),)

	title = db.Column(
				db.String(50),) 

	incident_begin_date = db.Column(
				db.Date,) 

	incident_end_date = db.Column(
				db.Date,) 

	declared_county_area = db.Column(
				db.String(50),) 

	last_refresh = db.Column(
				db.Date,) 

	def __repr__(self):
		"""representation of a disaster"""

		return "<Disaster id={} type={} title={} state={} date={}>".format(
			self.id, self.incident_type, self.title, self.state, self.declaration_date)

def connect_to_db(app):
    """Connect to database."""

    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql:///fema'
    app.config['SQLALCHEMY_ECHO'] = True
    db.app = app
    db.init_app(app)


def get_data():
	"""Use the FEMA API to get disaster data"""

	# examples:
	# http://www.fema.gov/api/open/v1/DisasterDeclarationsSummaries?$select=disasterNumber,state,disasterType - returns only the disasterNumber,state,disasterType and _id fields. If no value is specified, all of the fields are returned.
	# http://www.fema.gov/api/open/v1/DisasterDeclarationsSummaries?$select=disasterNumber,state,incidentBeginDate,incidentEndDate - returns only the disasterNumber, state, incidentBeginDate, incidentEndDate and _id fields. If no value is specified, all of the fields are returned.
	# /DisasterDeclarationsSummaries?$filter=declarationDate gt '2013-01-01T04:00:00.000z'

	start_date = datetime.now() - timedelta(days=LOOKBACK_DAYS)
	start_date_param = "incidentEndDate gt '{}'".format(
													datetime.strftime(start_date, "%Y-%m-%d"))

	payload = {'$select': ','.join(FIELDS),
						 '$filter': start_date_param}

	r = requests.get(
    FEMA_API_ENDPOINT,
    params=payload)

	return r.json()

def load_data_to_db(json_data):
	"""Load API results (json) into database"""

	for disaster in json_data['DisasterDeclarationsSummaries']:
		
		# create a Disaster object
		disaster_row = Disaster(
			disaster['disaster_number'],
			disaster['state'],
			disaster['declaration_date'],
			disaster['incident_type'],
			disaster['title'],
			disaster['incident_begin_date'],
			disaster['incident_end_date'],
			disaster['declared_county_area'],
			disaster['last_refresh'])

		db.session.add(disaster_row)
		db.commit()

# set up the db connection
app = Flask(__name__)
app.secret_key = "SECRET!"

connect_to_db(app)
db.create_all()
json_results = get_data()
load_data_to_db(json_results)


