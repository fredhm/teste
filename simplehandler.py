import webapp2
import os
from google.appengine.ext.webapp import template

class SimpleHandler(webapp2.RequestHandler):
	def render(self, filename, template_values = None):
		path = os.path.join(os.path.dirname(__file__), 'templates', filename)
		self.response.out.write(template.render(path, template_values))
