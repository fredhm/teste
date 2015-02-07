from google.appengine.api import taskqueue
from google.appengine.ext import db
import webapp2
from datetime import date, timedelta
from model import CitizenData, MUDamage
from google.appengine.api import urlfetch
from bs4 import BeautifulSoup
import logging, json, urllib
import json
import data

class CitizenQueueDate(db.Model):
    date = db.DateProperty()

class CitizenQueue(db.Model):
    citizen_id = db.IntegerProperty()
    citizen_mu_id = db.IntegerProperty(required = True)

class CitizenQueueHandler(webapp2.RequestHandler):
    def get(self):               
        taskqueue.add(url='/tasks/citizen_queue')

class CitizenQueueTask(webapp2.RequestHandler):
    def post(self): 
        today = date.today()
        dates = db.GqlQuery("SELECT * FROM CitizenQueueDate WHERE date = :1", today).fetch(1)      
        if (len(dates) == 0):
            logging.info("Creating queue")
            citizens = db.GqlQuery("SELECT * FROM CitizenQueue")
            for citizen in citizens:
                citizen.delete()            
            citizens = db.GqlQuery("SELECT * FROM Citizen")
            for citizen in citizens:
                citizen_queue = CitizenQueue(citizen_id = citizen.citizen_id, citizen_mu_id = citizen.citizen_mu_id)
                citizen_queue.put()                
            citizen_queue_date = CitizenQueueDate(date = today)
            citizen_queue_date.put()
            last_dates = db.GqlQuery("SELECT * FROM CitizenQueueDate WHERE date < :1", today)
            for last_date in last_dates:
                last_date.delete()  

class CitizenDataLoadHandler(webapp2.RequestHandler):
    def get(self):
        citizens = db.GqlQuery("SELECT * FROM CitizenQueue ORDER BY citizen_id").fetch(100)        
        for citizen in citizens:
            taskqueue.add(url='/tasks/citizen_data_load', params={'citizen_id': citizen.citizen_id, "citizen_mu_id" : citizen.citizen_mu_id}, queue_name = "citizen-data-load")
            citizen.delete()

class CitizenDataLoadTask(webapp2.RequestHandler):
    def post(self):
        today = date.today()
        citizen_id = int(self.request.get('citizen_id'))
        citizen_mu_id = int(self.request.get('citizen_mu_id'))
        result = urlfetch.fetch("http://www.erepublik.com/en/citizen/profile/" + str(citizen_id))
        if result.status_code == 200:
            soup = BeautifulSoup(result.content)
            rank_text = soup.find_all("div", "citizen_military")[1].find("div", "stat").strong.text
            rank = int(rank_text.split("/")[0].replace(",", ""))
            citizen_data = CitizenData(citizen_id = citizen_id, rank = rank, date = today, citizen_mu_id = citizen_mu_id)
            citizen_data.put()
            
class MUDamageCountHandler(webapp2.RequestHandler):
    def get(self):
        taskqueue.add(url='/tasks/mu_damage_count')

class MUDamageCountTask(webapp2.RequestHandler):
    def post(self):
        today = date.today()
        previous_day = today - timedelta(days = 1)
        today_data = db.GqlQuery("SELECT * FROM CitizenData WHERE date = :1", today)
        data_map = {}
        for data in today_data:
            data_map[data.citizen_id] = (data.citizen_mu_id, [data.rank])
        previuos_data = db.GqlQuery("SELECT * FROM CitizenData WHERE date = :1", previous_day)
        for data in previuos_data:
            if data.citizen_id in data_map:
                data_map[data.citizen_id][1].append(data.rank)
        mu_data = {}
        for citizen_id in data_map.keys():
            if len(data_map[citizen_id][1]) == 2:
                mu_id = data_map[citizen_id][0]
                if mu_id not in mu_data:
                    mu_data[mu_id] = 0
                mu_data[mu_id] += data_map[citizen_id][1][0] - data_map[citizen_id][1][1] 
        for mu_id in mu_data.keys():
            damage = MUDamage(mu_id = mu_id, date = previous_day, damage = mu_data[mu_id] * 10)
            damage.put()
        #clear citizen data
        clear_data = db.GqlQuery("SELECT * FROM CitizenData WHERE date < :1", previous_day)
        for data in clear_data:
            data.delete()
        
class MUDamageMemcacheHandler(webapp2.RequestHandler):
    def get(self):
        taskqueue.add(url='/tasks/mu_damage_memcache_count')

class MUDamageMemcacheCountTask(webapp2.RequestHandler):
    def post(self):
        data.count_10_days_mu_data()

app = webapp2.WSGIApplication([
    ('/tasks/citizen_queue_handler', CitizenQueueHandler),
    ('/tasks/citizen_queue', CitizenQueueTask),
    ('/tasks/citizen_data_load_handler', CitizenDataLoadHandler),
    ('/tasks/citizen_data_load', CitizenDataLoadTask),
    ('/tasks/mu_damage_handler', MUDamageCountHandler),
    ('/tasks/mu_damage_count', MUDamageCountTask),
    ('/tasks/mu_damage_memcache_handler', MUDamageMemcacheHandler),
    ('/tasks/mu_damage_memcache_count', MUDamageMemcacheCountTask),
], debug=True)
