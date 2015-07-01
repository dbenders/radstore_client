import requests
from itertools import imap
import urllib
import json
import datetime

class Config(object):
	pass
config = Config()


def encode(obj):
	if isinstance(obj, datetime.datetime):
		return obj.isoformat()
	else:
		return json.default(obj)


def _post(url, data, **kwargs):
	resp = requests.post(url, data=json.dumps(data, default=encode), 
		headers={'Content-Type':'application/json'})
	if not resp.ok:
		raise Exception("error: %d: %s", resp.status_code, resp.reason)
	data = resp.json()
	if data['status'] != 'ok': 
		raise Exception("error: %s: %s", data['status'], data.get('message',''))
	return data['data']

def _post_binary(url, data):
	resp = requests.post(url, data=data, 
		headers={'Content-Type':'application/octet-stream'})
	if not resp.ok:
		raise Exception("error: %d: %s", resp.status_code, resp.reason)
	data = resp.json()
	if data['status'] != 'ok': 
		raise Exception("error: %s: %s", data['status'], data.get('message',''))
	return data['data']


def _get(*args, **kwargs):
	resp = requests.get(*args, **kwargs)
	if not resp.ok:
		raise Exception("error: %d: %s", resp.status_code, resp.reason)
	data = resp.json()
	if data['status'] != 'ok': 
		raise Exception("error: %s: %s", data['status'], data.get('message',''))
	return data['data']

def _get_binary(*args, **kwargs):
	resp = requests.get(*args, **kwargs)
	if not resp.ok:
		raise Exception("error: %d: %s", resp.status_code, resp.reason)
	return resp

class Query(object):
	def __init__(self, cls):
		self.params = {}
		self._offset = None
		self._limit = None
		self.cls = cls

	def filter(self, **kwargs):
		self.params.update(kwargs)
		return self

	def offset(self, ofs):
		self._offset = ofs
		return self

	def limit(self, lim):
		self._limit = lim
		return self

	def _get_list(self):
		url = '%s/%s' % (config.base_url, self.cls.endpoint)
		par = self.params.copy()
		if self._limit is not None: par['limit'] = self._limit
		if self._offset is not None: par['offset'] = self._offset		
		return _get(url, params=par)

	def all(self):
		return map(self.cls, self._get_list()[self.cls.class_name_plural])

	def first(self):
		return self.cls(self.limit(1)._get_list()[self.cls.class_name_plural][0])

	def count(self):
		return self._get_list()['count']


class Resource(object):
	normal_attributes = ['_id','metadata']

	def __init__(self, params={}):
		self._id = None
		self.metadata = {}
		for k,v in params.items():
			setattr(self, k, v)

	def __getattr__(self, name):
		return self.metadata[name]

	def __setattr__(self, name, value):
		if name in self.normal_attributes: super(Resource, self).__setattr__(name, value)
		else: self.metadata[name] = value

	def copy(self):
		outp = self.__class__()
		outp.metadata = self.metadata.copy()
		return outp

	@classmethod
	def query(cls):
		return Query(cls)

	@classmethod
	def get(cls, id):
		return Query(cls).filter(_id=id).first()

	def save(self):
		if self._id is None:			
			resp = _post('%s/%s' % (config.base_url, self.endpoint), data=self.metadata)
			self._id = resp[self.class_name_singular]['_id']


class Product(Resource):
	endpoint = 'products'
	class_name_singular = 'product'
	class_name_plural = 'products'

	normal_attributes = ['_id','metadata', 'content', '_content']
	
	def __init__(self, params={}):
		super(Product, self).__init__(params)

	def save_content(self):
		if self._id is None:
			raise Exception("save instance first")
		_post_binary('%s/%s/%s/content' % (config.base_url, self.endpoint, self._id), data=self.content)

	@property
	def content(self):
		if not hasattr(self, '_content'):
			self.__dict__['_content'] = _get_binary('%s/%s/%s/content' % (config.base_url, self.endpoint, self._id)).content
		return self._content

	@content.setter
	def content(self, value):
		self._content = value


class Transformation(Resource):
	endpoint = 'transformations'
	class_name_singular = 'transformation'
	class_name_plural = 'transformations'

	def add_input(self, prod):
		if 'inputs' not in self.metadata: 
			self.metadata['inputs'] = []
		self.metadata['inputs'].append({'_id': prod._id})
		return self

	def add_output(self, prod):
		if 'outputs' not in self.metadata: 
			self.metadata['outputs'] = []
		self.metadata['outputs'].append({'_id': prod._id})
		return self


def _parse_arg(arg):
	i = arg.index('=')
	return arg[:i],arg[i+1:]

def parse_cmdline(params):
	cmd = None
	if len(params)>1:
		cmd = params[1]
	args = {}
	if len(params)>2:
		args = dict(map(_parse_arg, params[2:]))
	return cmd, args


# for testing
if __name__ == '__main__':
	config.base_url = 'http://protopalenque.agrodatos.info:3003/api/v1'
	q = Product.query()
	q.filter(variable='dBZ')
	print "count: %d" % q.count()
	for prod in q.all():
		print prod._id, prod.name

	prod = Product.get(Product.query().first()._id)
	print prod._id, prod.type, prod.name