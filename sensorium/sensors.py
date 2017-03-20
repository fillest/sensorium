import os
import subprocess
import logging
import operator
import errno
import requests
import requests.exceptions
from sensorium import Sensor


logger = logging.getLogger(__name__)


def reread (fd, bufmax = 1024 * 4): #typical actual length is about 1kb
	os.lseek(fd, 0, os.SEEK_SET)
	buf = os.read(fd, bufmax)
	if not (len(buf) < bufmax):
		raise Exception(len(buf))
	return buf

class ProcMeminfo (Sensor):
	def __init__ (self, settings):
		#http://manpages.ubuntu.com/manpages/trusty/man5/proc.5.html
		self.fd = os.open('/proc/meminfo', os.O_RDONLY)
		#TODO os.close(fd)

	def work (self, metrics):
		buf = reread(self.fd)

		info = {}
		for line in buf.splitlines():
			parts = line.split()
			name = parts[0][:-1]
			value = int(parts[1])
			# if len(parts) > 2:
			# 	assert parts[2] == 'kB'
			info[name] = value
		#TODO overcommiting metrics?
		#int-division is enough for that
		tot_1perc = info['MemTotal'] / 100
		metrics.append(('mem_avail', (), (
			('free', info['MemFree'] / tot_1perc),
			('cached', info['Cached'] / tot_1perc),
			('buffers', info['Buffers'] / tot_1perc), #http://unix.stackexchange.com/a/87909
		)))

class ProcDiskstats (Sensor):
	def __init__ (self, settings):
		self.names = set()
		for line in subprocess.check_output('lsblk --nodeps --list --noheadings --output TYPE,NAME,PHY-SEC,LOG-SEC'.split()).splitlines():
			dtype, name, pssz, lssz = line.split()
			if dtype == 'disk':
				# assert pssz == lssz == '512', (pssz, lssz) #TODO not needed? see 512-related comment with docs link
				self.names.add(name)
			else:
				logger.info('skipping non-disk: %s', line)

		#https://www.kernel.org/doc/Documentation/iostats.txt
		#https://www.kernel.org/doc/Documentation/block/stat.txt
		self.fd = os.open('/proc/diskstats', os.O_RDONLY)

		self.reset()

	def reset (self):
		self.prev_counters = {}
		self.is_first_sample = True
	
	def work (self, metrics):
		buf = reread(self.fd)
		for line in buf.splitlines():
			parts = line.split()
			name = parts[2]
			if name in self.names:
				cur_counters = map(int, parts[3:])
				(reads_completed, reads_merged, sectors_read, ms_spent_reading, writes_completed, writes_merged, sectors_written,
					ms_spent_writing, reqs_in_progress, ms_spent_doing_ios, weighted_ms_spent_doing_ios) = cur_counters
				if not self.is_first_sample:
					diff_since_prev = map(operator.sub, cur_counters, self.prev_counters[name])
					(reads_completed, reads_merged, sectors_read, ms_spent_reading, writes_completed, writes_merged, sectors_written,
						ms_spent_writing, _, ms_spent_doing_ios, weighted_ms_spent_doing_ios) = diff_since_prev
					metrics.append(('disk_io_stats', (('name', name),), (
						('reqs_in_progress_sample', reqs_in_progress),
						('reads_completed', reads_completed),
						('writes_completed', writes_completed),
						('reads_merged', reads_merged),
						('writes_merged', writes_merged),
						#https://www.kernel.org/doc/Documentation/block/stat.txt
						#  >The "sectors" in question are the standard UNIX 512-byte sectors, not any device- or filesystem-specific block size. 
						('mib_read', (sectors_read * 512) / (1024 * 1024)),  #matched with iostat output
						('mib_written', (sectors_written * 512) / (1024 * 1024)),
						('que_active_time', ms_spent_doing_ios / ((1000 * 1) / 100)),
						('ms_reading_sum', ms_spent_reading),
						('ms_writing_sum', ms_spent_writing),
						('time_in_queue', weighted_ms_spent_doing_ios),
					)))
				self.prev_counters[name] = cur_counters
		self.is_first_sample = False

def get_SC_CLK_TCK ():
	return os.sysconf(os.sysconf_names['SC_CLK_TCK']) #sysconf(_SC_CLK_TCK) #https://gitlab.com/procps-ng/procps/blob/master/proc/sysinfo.c

#http://manpages.ubuntu.com/manpages/trusty/man5/proc.5.html
class ProcStat (Sensor):
	def __init__ (self, settings):
		SC_CLK_TCK = get_SC_CLK_TCK()
		if SC_CLK_TCK != 100:
			raise Exception(SC_CLK_TCK)
		self.fd = os.open('/proc/stat', os.O_RDONLY)

		self.reset()

	def reset (self):
		self.prev_counters = {}
		self.is_first_sample = True
		# debugbuf = []
	
	def work (self, metrics):
		buf = reread(self.fd, 1024 * 4 * 2)
		# debugbuf.append((t_start, buf))

		busiest_core_util = None
		v_busy_max = 0
		for line in buf.splitlines():
			parts = line.split()
			metric_type = parts[0]
			if metric_type != 'cpu' and metric_type.startswith('cpu'):
				cpu_name = metric_type
				cur_counters = map(int, parts[1:])
				if not self.is_first_sample:
					util_during_sec = map(operator.sub, cur_counters, self.prev_counters[cpu_name])
					v_user, v_nice, v_system, _v_idle, v_iowait, v_irq, v_softirq, v_steal, v_guest, v_guest_nice = util_during_sec
					v_busy = v_user + v_nice + v_system + v_irq + v_softirq + v_steal + v_guest + v_guest_nice
					if v_busy > v_busy_max:
						v_busy_max = v_busy
						busiest_core_util = util_during_sec
					metrics.append(('cpu_total_usage', (('cpu', cpu_name),), (
						('total_busy', v_busy),
					)))
				self.prev_counters[cpu_name] = cur_counters
			elif metric_type == 'intr':
				cur_count = parts[1]
				pass #TODO
			# elif metric_type == 'softirq':
			# 	cur_count = parts[1]
			# 	pass

		if not self.is_first_sample:
			# if not v_busy_max:
			# 	for t, b in debugbuf:
			# 		print t, b
			# 		print '-'*30
			# 	print '-'*30
			# 	raise Exception("no cores info in /proc/stat?\n%s" % buf)
			if v_busy_max:
				v_user, v_nice, v_system, _v_idle, v_iowait, v_irq, v_softirq, v_steal, v_guest, v_guest_nice = busiest_core_util
			else:
				#TODO we dont have iowait and idle here
				v_user = v_nice = v_system = v_iowait = v_irq = v_softirq = v_steal = v_guest = v_guest_nice = 0
			metrics.append(('cpu_busiest_core', (), (
				('value.user', v_user), #################TODO rename without value
				('value.nice', v_nice),
				('value.system', v_system),
				('value.iowait', v_iowait),
				('value.irq', v_irq),
				('value.softirq', v_softirq),
				('value.steal', v_steal),
				('value.guest', v_guest),
				('value.guest_nice', v_guest_nice),
			)))

		self.is_first_sample = False

SC_PAGESIZE = os.sysconf(os.sysconf_names['SC_PAGESIZE'])
MiB = 1024 * 1024
key_op = operator.itemgetter(0)
br = '<br/>'  # '\n' doesn't work because it gets rendered as html in grafana
index_offset = 1 + 2  #1 is because manual is 1-based and 2 is because 'rest' starts from 3rd column
#TODO check atop >It shows the resource consumption by all processes that were active during the interval, so also the resource consumption by those processes that have finished during the interval.
#TODO check out https://www.kernel.org/doc/Documentation/accounting/taskstats.txt + https://lwn.net/Articles/414875/ + https://github.com/uber-common/cpustat (!) + https://github.com/facebook/gnlpy
#http://manpages.ubuntu.com/manpages/trusty/man5/proc.5.html
class ProcPid (Sensor):
	def __init__ (self, settings):
		SC_CLK_TCK = get_SC_CLK_TCK()
		if SC_CLK_TCK != 100:
			raise Exception(SC_CLK_TCK)
		self.top_limit = 10

		self.reset()

	def reset (self):
		self.prev_counters = {}

	def work (self, metrics):
		top_ps_cpu = []
		top_ps_mem = []

		dead_pids = set(self.prev_counters.viewkeys())
		for pid in os.listdir('/proc/'):
			if pid.isdigit():
				try:
					fd = os.open('/proc/' + pid + '/stat', os.O_RDONLY)
				except OSError as e:
					if e.errno == errno.ENOENT or e.errno == errno.ESRCH:
						continue
					raise
				try:
					raw = os.read(fd, 1024)
				except OSError as e:
					if e.errno == errno.ESRCH:
						continue
					raise
				finally:
					os.close(fd)
				h, t = raw.split(') ', 1)  #second column may have spaces
				title = h.split('(', 1)[1]
				rest = t.split(' ')  #starts from 3rd column
				if len(rest) != 50:
					raise Exception(rest)
				starttime = rest[22 - index_offset]

				v_utime = int(rest[14 - index_offset])
				v_stime = int(rest[15 - index_offset])
				v_cpu = v_utime + v_stime
				# v_majflt = int(rest[12 - index_offset])

				prev = self.prev_counters.get(pid)
				if prev is not None and starttime == prev[0]:
					dead_pids.remove(pid)
					diff_cpu = v_cpu - prev[1]
					if diff_cpu:
						top_ps_cpu.append((diff_cpu, pid))
					prev[1] = v_cpu
				else:
					try:
						fd = os.open('/proc/' + pid + '/cmdline', os.O_RDONLY)
					except OSError as e:
						if e.errno == errno.ENOENT or e.errno == errno.ESRCH:
							continue
						raise
					try:
						cmdline = os.read(fd, 1024)
					except OSError as e:
						if e.errno == errno.ESRCH:
							continue
						raise
					finally:
						os.close(fd)
					v_title = cmdline.split('\x00')[0] or title
					if not v_title:
						try:
							exepath = os.readlink('/proc/' + pid + '/exe')
						except OSError as e:
							if e.errno == errno.EACCES:
								exepath = '?'
							elif e.errno == errno.ENOENT or e.errno == errno.ESRCH:
								continue
							else:
								raise
						v_title = exepath

					self.prev_counters[pid] = [starttime, v_cpu, v_title]
					if prev is not None:
						dead_pids.remove(pid)

				v_rss = int(rest[24 - index_offset])
				top_ps_mem.append((v_rss, pid))

		top_ps_cpu.sort(key = key_op, reverse = True)
		top_ps_mem.sort(key = key_op, reverse = True)
		
		fields = [
			('top.mem.rss', br.join('%s %s %s' % ((v_rss * SC_PAGESIZE) / MiB, pid, self.prev_counters[pid][2]) for v_rss, pid in top_ps_mem[:self.top_limit])),
		]
		if top_ps_cpu:
			fields.append(('top.cpu.total', br.join('%s %s %s' % (v_cpu, pid, self.prev_counters[pid][2]) for v_cpu, pid in top_ps_cpu[:self.top_limit])))
		metrics.append(('top_processes', (), fields))

		for pid in dead_pids:
			del self.prev_counters[pid]

class Statvfs (Sensor):
	def __init__ (self, settings):
		paths_to_check = ['/', '/persistent', '/mnt', '/static'] #TODO configurable
		self.paths = []
		for path in paths_to_check:
			if os.path.exists(path):
				s = os.statvfs(path)
				total_1perc = float(s.f_blocks * s.f_frsize) / 100.0
				self.paths.append((path, total_1perc))
			else:
				logger.warning('"%s" does not exist, skipping', path)

	def work (self, metrics):
		for path, total_1perc in self.paths:
			s = os.statvfs(path)
			metrics.append(('disk_free_space', (('mount_path', path),), (
				('free', int(round(float(s.f_bavail * s.f_bsize) / total_1perc))),
			)))

#http://manpages.ubuntu.com/manpages/trusty/man5/proc.5.html
#/proc/net/dev src https://github.com/torvalds/linux/blob/master/net/core/net-procfs.c - ctrl f "dev_seq_printf_stats"
#/sys/class/net/ src https://github.com/torvalds/linux/blob/master/net/core/net-sysfs.c - ctrl f "netstat_show"
#looks like they use same source - dev_get_stats()
#description for counters https://www.kernel.org/doc/Documentation/ABI/testing/sysfs-class-net-statistics
#TODO see also https://www.kernel.org/doc/Documentation/ABI/testing/sysfs-class-net-queues
#	https://github.com/OpenTSDB/tcollector/blob/master/collectors/0/netstat.py
#TODO check /proc/net/netstat
class NetDev (Sensor):
	def __init__ (self, settings):
		self.fd = os.open('/proc/net/dev', os.O_RDONLY)
		lines = reread(self.fd).splitlines()
		assert ':' not in lines[0], lines[0]
		assert ':' not in lines[1], lines[1]
		assert ':' in lines[2], lines[2]

		self.reset()

	def reset (self):
		self.prev_counters = {}
		self.is_first_sample = True

	def work (self, metrics):
		buf = reread(self.fd)
		lines = buf.splitlines()[2:]
		for l in lines:
			name, cur_counters = l.split(':', 1)
			name = name.strip(' ')
			cur_counters = map(int, cur_counters.split())
			if not self.is_first_sample:
				diff_since_prev = map(operator.sub, cur_counters, self.prev_counters[name])  #TODO interfaces may be created dynamically
				(v_r_bytes, v_r_packets, v_r_errs, v_r_drop, v_r_fifo, v_r_frame, v_r_compressed, v_r_multicast,
					v_t_bytes, v_t_packets, v_t_errs, v_t_drop, v_t_fifo, v_t_colls, v_t_carrier, v_t_compressed) = diff_since_prev
				metrics.append(('net.stat', (('interface', name),), (
					('received.bytes', v_r_bytes),
					('received.packets', v_r_packets),
					('received.errors', v_r_errs),
					('transmitted.bytes', v_t_bytes),
					('transmitted.packets', v_t_packets),
					('transmitted.errors', v_t_errs),
				)))
			self.prev_counters[name] = cur_counters
		self.is_first_sample = False

class NginxStatus (Sensor):
	"""
	see http://nginx.org/en/docs/http/ngx_http_stub_status_module.html

	$ cat /etc/nginx/sites-enabled/nginx_status.conf
server {
    listen 127.0.0.1:39574;
    location /nginx_status {
        stub_status on;
        access_log off;
    }
}
	"""

	requires_thread = True

	def __init__ (self, settings):
		self.url = 'http://127.0.0.1:39574/nginx_status'
		# self.url = 'https://httpbin.org/delay/3'

	def work (self, metrics):
		# try:
		lines = requests.get(self.url, timeout = (0.3, 0.6), stream = False, allow_redirects = False).content.splitlines()
		# except requests.exceptions.Timeout:
		# 	# logger.exception("qwe")
		# 	# return
		# 	raise

		v_accepts, v_handled, v_requests = map(int, lines[2].split())
		v_reading, v_writing, v_waiting = map(int, lines[3].split()[1::2])
		metrics.append(('nginx.status', (), (
			('connections_active', int(lines[0].split()[2])),
			('connections_accepted', v_accepts),
			('connections_handled', v_handled),
			('requests_started', v_requests),  #http://lxr.nginx.org/source/src/http/ngx_http_request.c ctrl f ngx_stat_requests
			('connections_reading', v_reading),
			('connections_writing', v_writing),
			('connections_waiting', v_waiting),
		)))
