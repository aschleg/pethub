from dotenv import load_dotenv
import json
import os
import petpy
import datetime
import boto3
import psycopg2


load_dotenv('../../../.env')


class PetfinderETL(object):

    def __init__(self):
        self.petfinder_access_key = os.environ.get('PETFINDER_PETHUB_KEY')
        self.petfinder_secret_key = os.environ.get('PETFINDER_PETHUB_SECRET_KEY')
        self.pf = petpy.Petfinder(key=self.petfinder_access_key,
                                  secret=self.petfinder_secret_key)

    def get_organizations(self):
        orgs = self.pf.organizations(location='WA', country='US', results_per_page=100, pages=100)['organizations']

        with open('organizations_{date}.json'.format(date=datetime.datetime.now().strftime('%Y-%m-%d')), 'w') as f:
            json.dump(orgs['organizations'], f)
