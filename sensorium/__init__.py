import time
import os
import socket
import logging
import logging.config
import sys
import threading
import Queue
import gzip
from cStringIO import StringIO
import contextlib
import argparse
import imp
import requests
import requests.auth
import random


logger = logging.getLogger(__name__)


one_of_sensors_crashed = False

class Sensor (object):
	# def wrapped_work (self, metrics):
	# 	global one_of_sensors_crashed
	# 	try:
	# 		return self.work(metrics)
	# 	except:
	# 		one_of_sensors_crashed = True
	# 		raise
	
	def reset (self):
		pass

from sensorium import sensors  #avoid circular imports

def forward_data (que, settings):
	auth = requests.auth.HTTPBasicAuth(settings['user'], settings['password']) if settings['user'] else None
	with contextlib.closing(StringIO()) as buf:
		while True:
			msg = que.get()
			buf.seek(0)
			buf.truncate(0)
			with gzip.GzipFile(fileobj = buf, compresslevel = 6, mode = 'wb') as gzf:
				gzf.write(msg)
			out = buf.getvalue()

			while True:
				try:
					r = requests.post(settings['url'] + '/write?db=%s&precision=ms&consistency=all&rp=default' % settings['db'], data = out,
						timeout = (10, 20), stream = False, allow_redirects = False, headers = {'Content-Encoding': 'gzip'}, auth = auth)
					if r.status_code != 204:
						logger.error("failed to send to influxdb, will retry soon, resp: %s, %s, %s", r.status_code, r.reason, r.content)
						time.sleep(random.uniform(1, 3))
					else:
						break
				except requests.RequestException:
					logger.exception('Exception while sending data, will retry soon')
					time.sleep(random.uniform(1, 3))

DEFAULT_LOGGING_CONFIG = {
	'version': 1,
	'disable_existing_loggers': False,
	'root': {
		'level': 'INFO',
		'handlers': ['stderr'],
	},
	'handlers': {
		'stderr': {
			'class': 'logging.StreamHandler',
			'stream': 'ext://sys.stderr',
			'formatter': 'default',
		},
	},
	'formatters': {
		'default': {'format': '%(asctime)s %(levelname)-5.5s %(filename)s:%(funcName)s:%(lineno)d  %(message)s'},
	},
}

def main ():
	parser = argparse.ArgumentParser()
	parser.add_argument('--hostname', default = socket.gethostname())
	parser.add_argument('--url', default = 'http://localhost:8086', help = "influxdb url")
	parser.add_argument('--db', default = 'test')
	parser.add_argument('--user')
	parser.add_argument('--password')
	parser.add_argument('-e', '--enable-sensors', help = 'use "," as separator')
	parser.add_argument('-d', '--disable-sensors', help = '')
	parser.add_argument('-c', '--config', help = "config file path")
	args = parser.parse_args()

	settings = dict(vars(args))
	if args.config:
		#runpy.run_path doesn't play well with closures (e.g. http://stackoverflow.com/a/7092730/1183239)
		m = imp.load_source('sensorium_config', args.config)  #TODO for py3 see http://stackoverflow.com/a/67692/1183239
		settings.update(m.__dict__)

	logging.getLogger("requests").setLevel(logging.WARNING)
	logging_config = settings.get('logging_config', DEFAULT_LOGGING_CONFIG)
	if 'update_logging_config' in settings:
		settings['update_logging_config'](logging_config)
	logging.config.dictConfig(logging_config)

	logger.info("starting; using influxdb '%s', db '%s', user %s, hostname '%s'", settings['url'], settings['db'], repr(settings['user']), settings['hostname'])

	#distributed = 
	default_enabled = [sensors.ProcStat, sensors.ProcMeminfo, sensors.ProcDiskstats, sensors.Statvfs, sensors.ProcPid, sensors.NetDev]
	default_disabled = [sensors.NginxStatus]
	if args.disable_sensors:
		assert args.disable_sensors == '*', "value not supported yet"
		default_disabled += list(default_enabled)
		default_enabled = []
	#WARNING don't forget there can be multiple instances of extra-sensor class
	#TODO these are instances - maybe it should be done lazier for disabling declaratively
	enabled_extra_sensors = settings.get('extra_samplers', [])
	names_to_enable = args.enable_sensors.split(',') if args.enable_sensors else []
	if names_to_enable:
		for name in names_to_enable:
			assert name in set(cls.__name__ for cls in (default_enabled + default_disabled + [inst.__class__ for inst in enabled_extra_sensors])), name
	enabled_sensors = default_enabled + [cls for cls in default_disabled if cls.__name__  in set(names_to_enable)]
	
	sensor_inst = [cls(settings) for cls in enabled_sensors] + enabled_extra_sensors
	logger.info("using sensors: %s", ', '.join(s.__class__.__name__ for s in sensor_inst))

	forward_que = Queue.Queue()
	sending_thr = threading.Thread(target = forward_data, args = [forward_que, settings], name = 'forward_data')
	sending_thr.daemon = True #TODO probably better non-daemon or atexit sync
	sending_thr.start()

	mainthr_samplers = []
	for sampler in sensor_inst:
		if getattr(sampler, 'requires_thread', False): #TODO rename run_in_thread
			logger.info("starting a thread for %s", sampler.__class__.__name__)
			thr = threading.Thread(target = managed_loop, args = [(sampler,), float(getattr(sampler, 'interval', 1)), settings, forward_que, None],
				name = 'sensor_' + sampler.__class__.__name__)
			thr.daemon = True
			thr.start()
		else:
			mainthr_samplers.append(sampler)

	sample_interval = 1.0
	# sample_interval = 0.3
	loop(mainthr_samplers, sample_interval, settings, forward_que, sending_thr)

def managed_loop (*args, **kwargs):
	global one_of_sensors_crashed
	while True:
		try:
			loop(*args, **kwargs)
		except:
			one_of_sensors_crashed = True
			raise

def loop (sensors, work_interval, settings, forward_que, thr):
	consec_exc_num = 0
	consec_exc_timeout = 30
	metrics = []
	while True:
		t_start = time.time()

		try:
			for sensor in sensors:
				sensor.work(metrics)
		except:
			consec_exc_num += 1
			if consec_exc_num < (consec_exc_timeout / work_interval):
				logger.exception("Exception during sensor.work, ignoring until timeout %ss (%s)", consec_exc_timeout, consec_exc_num)
			else:
				raise
		else:
			consec_exc_num = 0

		if metrics:
			out_ts = int(t_start * 1000)
			batch = '\n'.join(serialize_metrics(metrics, out_ts, settings['hostname']))
			forward_que.put(batch)
			del metrics[:]

		if thr is not None:
			if one_of_sensors_crashed:
				logger.error('one of sensors crashed, terminating')
				sys.exit(1)
			if not thr.is_alive(): #TODO use e.g. one_of_sensors_crashed?
				logger.error('sending thread is dead, terminating')
				sys.exit(1)

		lat = time.time() - t_start
		rem = work_interval - lat
		if rem > 0.0:
			time.sleep(rem)
		else:
			logger.warning("sensors' latency %.2fs higher than interval %ss, resetting sensors", lat, work_interval)

			for sensor in sensors:
				sensor.reset()

def serialize_value (v, fields):
	if isinstance(v, (int, long)):
		return str(v) + 'i'
	elif isinstance(v, float):
		return str(v)
	elif isinstance(v, basestring):
		return '"%s"' % v.replace('"', '\\"')
	else:
		raise Exception('%s %s %s' % (type(v), repr(v), fields))

def serialize_metrics (metrics, out_ts, hostname):
	for name, tags, fields in metrics:
		yield '%s,%s %s %s' % (
			name,
			','.join('%s=%s' % (tn, tv) for tn, tv in (tags + (('host', hostname),))), #TODO not all metrics need host
			','.join('%s=%s' % (n, serialize_value(v, fields)) for n, v in fields),
			out_ts,
		)


if __name__ == '__main__':
	main()
