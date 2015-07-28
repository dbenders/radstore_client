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

def _put(url, data, **kwargs):
	resp = requests.put(url, data=json.dumps(data, default=encode),
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

	def _parse_params(self, prefix, par):
		if not isinstance(par,dict): return {'.'.join(prefix): par}
		ans = {}
		for k,v in par.items():
			ans.update(self._parse_params(prefix+[k], v))
		return ans

	def _get_list(self):
		url = '%s/%s' % (config.base_url, self.cls.endpoint)
		par = self._parse_params([],self.params)
		if self._limit is not None: par['limit'] = self._limit
		if self._offset is not None: par['offset'] = self._offset
		return _get(url, params=par)

	def all(self):
		return map(self.cls, self._get_list()[self.cls.class_name_plural])

	def first(self):
		return self.cls(self.limit(1)._get_list()[self.cls.class_name_plural][0])

	def count(self):
		return self._get_list()['count']

	def exists(self):
		return self._get_list()['count'] > 0


class Resource(object):
	def __init__(self, params={}):
		self._id = None
		self._metadata = {}
		for k,v in params.items():
			setattr(self, k, v)

	def __getattr__(self, name):
		if name[0] == '_': return super(Resource, self).__getattr__(self, name)
		else: return self._metadata[name]

	def __setattr__(self, name, value):
		if name[0] == '_': super(Resource, self).__setattr__(name, value)
		else: self._metadata[name] = value

	def copy(self):
		outp = self.__class__()
		outp._metadata = self._metadata.copy()
		return outp

	@classmethod
	def query(cls):
		return Query(cls)

	@classmethod
	def get(cls, id):
		return Query(cls).filter(_id=id).first()

	def save(self):
		if self._id is None:
			resp = _post('%s/%s' % (config.base_url, self.endpoint), data=self._metadata)
			self._id = resp[self.class_name_singular]['_id']
		else:
			resp = _put('%s/%s/%s' % (config.base_url, self.endpoint, self._id), data=self._metadata)

class Product(Resource):
	endpoint = 'products'
	class_name_singular = 'product'
	class_name_plural = 'products'

	def __init__(self, params={}):
		super(Product, self).__init__(params)
		self._content_dirty = False

	def save(self):
		super(Product, self).save()
		if self._content_dirty:
			_post_binary('%s/%s/%s/content' % (config.base_url, self.endpoint, self._id), data=self.content)
			self._content_dirty = False

	def __setattr__(self, name, value):
		if name == 'content': self.set_content(value)
		else: super(Product, self).__setattr__(name, value)

	@property
	def content(self):
		if not hasattr(self, '_content'):
			self._content = _get_binary('%s/%s/%s/content' % (config.base_url, self.endpoint, self._id)).content
			self._content_dirty = False
		return self._content

	def set_content(self, value):
		self._content = value
		self._content_dirty = True

class Transformation(Resource):
	endpoint = 'transformations'
	class_name_singular = 'transformation'
	class_name_plural = 'transformations'

	def add_input(self, prod):
		if 'inputs' not in self._metadata:
			self._metadata['inputs'] = []
		self._metadata['inputs'].append({'_id': prod._id})
		return self

	def add_output(self, prod):
		if 'outputs' not in self._metadata:
			self._metadata['outputs'] = []
		self._metadata['outputs'].append({'_id': prod._id})
		return self


def _parse_arg(arg):
	if '=' in arg:
		i = arg.index('=')
		return arg[:i],arg[i+1:]

	if arg.startswith('--'):
		return arg[2:],None

	return arg,None

def parse_cmdline(params, cmd=True):
	_cmd = None
	if cmd and len(params)>1:
		_cmd = params[1]
		params = params[1:]
	args = {}
	if len(params)>1:
		args = dict(map(_parse_arg, params[1:]))
	if cmd: return _cmd, args
	else: return args


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
