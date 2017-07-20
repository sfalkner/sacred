#!/usr/bin/env python
# coding=utf-8
from __future__ import division, print_function, unicode_literals
import json
import os
import tempfile
import gzip
import glob
import pickle
import tinydb

from datetime import datetime
from shutil import copyfile

from sacred.commandline_options import CommandLineOption
from sacred.dependencies import get_digest
from sacred.observers.base import RunObserver
from sacred.utils import FileNotFoundError  # For compatibility with py2
from sacred import optional as opt
from sacred.serializer import flatten

import numpy as np


DEFAULT_FILE_STORAGE_PRIORITY = 20


class HPCFSObserver(RunObserver):
    VERSION = 'HPCFSObserver-0.7.0'

    @classmethod
    def create(cls, basedir, resource_dir=None, source_dir=None,
               priority=DEFAULT_FILE_STORAGE_PRIORITY):
        os.makedirs(basedir, exists_ok=True)
        resource_dir = resource_dir or os.path.join(basedir, '_resources')
        source_dir = source_dir or os.path.join(basedir, '_sources')
        return cls(basedir, resource_dir, source_dir, priority)

    def __init__(self, basedir, resource_dir, source_dir,
                 priority=DEFAULT_FILE_STORAGE_PRIORITY):
        self.basedir = basedir
        self.resource_dir = resource_dir
        self.source_dir = source_dir
        self.priority = priority
        self.dir = None
        self.run_entry = None
        self.info = None
        self.cout = ""

    def queued_event(self, ex_info, command, queue_time, config, meta_info,
                     _id):
        raise NotImplementedError("this observer doesn't support queueing!")

    def save_sources(self, ex_info):
        base_dir = ex_info['base_dir']
        source_info = []
        for s, m in ex_info['sources']:
            abspath = os.path.join(base_dir, s)
            store_path, md5sum = self.find_or_save(abspath, self.source_dir)
            # assert m == md5sum
            source_info.append([s, os.path.relpath(store_path, self.basedir)])
        return source_info

    def started_event(self, ex_info, command, host_info, start_time, config,
                      meta_info, _id):
        if _id is None:
            self.dir = tempfile.mkdtemp(prefix='run_', dir=self.basedir)
        else:
            self.dir = os.path.join(self.basedir, str(_id))
            os.mkdir(self.dir)

        ex_info['sources'] = self.save_sources(ex_info)

        self.run_entry = {
            'experiment': dict(ex_info),
            'command': command,
            'host': dict(host_info),
            'start_time': start_time.isoformat(),
            'meta': meta_info,
            'status': 'RUNNING',
            'resources': [],
            'artifacts': [],
            'heartbeat': None,
            'config': config,
            'ETA': None
        }
        self.info = {}
        self.cout = ""

        self.start_time = start_time
        self.save_json(self.run_entry, 'run.json')
        self.save_cout()

        return os.path.relpath(self.dir, self.basedir)

    def find_or_save(self, filename, store_dir):
        if not os.path.exists(store_dir):
            os.makedirs(store_dir)
        source_name, ext = os.path.splitext(os.path.basename(filename))
        md5sum = get_digest(filename)
        store_name = source_name + '_' + md5sum + ext
        store_path = os.path.join(store_dir, store_name)
        if not os.path.exists(store_path):
            copyfile(filename, store_path)
        return store_path, md5sum

    def save_json(self, obj, filename):
        with open(os.path.join(self.dir, filename), 'w') as f:
            json.dump(flatten(obj), f, sort_keys=True, indent=2)
    
    def save_compressed_pickle(self, obj, filename):
        with gzip.GzipFile(os.path.join(self.dir, filename), 'wb') as f:
            pickle.dump(obj,f)

    def save_file(self, filename, target_name=None):
        target_name = target_name or os.path.basename(filename)
        copyfile(filename, os.path.join(self.dir, target_name))

    def save_cout(self):
        with open(os.path.join(self.dir, 'cout.txt'), 'w') as f:
            f.write(self.cout)

    def heartbeat_event(self, info, captured_out, beat_time, result):
        self.info = info
        self.run_entry['heartbeat'] = beat_time.isoformat()
        self.run_entry['result'] = result
        
       
        try:
			# the result measures the progress of the task:
			#	0 meaning just started and 
			#	1 representing the end of the experiment
			# this enables a linear extrapolation for the remaining time:
            diff = datetime.utcnow() - self.start_time
            self.run_entry['ETA'] = (1./result-1.) * diff.total_seconds()
        except:
            pass
        
        self.cout = captured_out
        self.save_cout()
        self.save_json(self.run_entry, 'run.json')
        if self.info:
            self.save_json(self.info, 'info.json')

    def completed_event(self, stop_time, result):
        self.run_entry['stop_time'] = stop_time.isoformat()
        self.run_entry.pop('ETA')
        assert isinstance(result, dict), 'Your experiments has to return a dictionary!'
        
        for (k,v) in result.items():
            fn = str(k)+'.gzp'
            self.save_compressed_pickle(v, fn)
            self.run_entry['result'][k] = fn

        self.run_entry['status'] = 'COMPLETED'

        self.save_json(self.run_entry, 'run.json')

    def interrupted_event(self, interrupt_time, status):
        self.run_entry['stop_time'] = interrupt_time.isoformat()
        self.run_entry['status'] = status
        self.save_json(self.run_entry, 'run.json')

    def failed_event(self, fail_time, fail_trace):
        self.run_entry['stop_time'] = fail_time.isoformat()
        self.run_entry['status'] = 'FAILED'
        self.run_entry['fail_trace'] = fail_trace
        self.save_json(self.run_entry, 'run.json')

    def resource_event(self, filename):
        store_path, md5sum = self.find_or_save(filename, self.resource_dir)
        self.run_entry['resources'].append([filename, store_path])
        self.save_json(self.run_entry, 'run.json')

    def artifact_event(self, name, filename):
        self.save_file(filename, name)
        self.run_entry['artifacts'].append(name)
        self.save_json(self.run_entry, 'run.json')

    def __eq__(self, other):
        if isinstance(other, FileStorageObserver):
            return self.basedir == other.basedir
        return False

    def __ne__(self, other):
        return not self.__eq__(other)


class HPCFSOption(CommandLineOption):
    """Add a HPCFS observer to the experiment."""

    short_flag = 'H'
    arg = 'BASEDIR'
    arg_description = "Base-directory to write the runs to"

    @classmethod
    def apply(cls, args, run):
        run.observers.append(FileStorageObserver.create(args))




def create_tinyDB(source_dir, destination, overwrite=False, skip_incomplete=True, db_filename='run.json', logger=None):
	""" Function to accumulate all runs in a given directory into a single
		tinyDB file.
	"""
	if os.path.isfile(destination) and not overwrite:
		fn = destination
		raise FileExistsError("Databasefile %s already exists, delete it or use the argument 'overwrite=True' to recreate the DB."%destination)
	
	tmp_filename = destination+'.tmp'
	db = tinydb.TinyDB(tmp_filename)

	# recursively walk through the subdirs
	for cur, dirs, files in os.walk(source_dir):
		try:
			# load the db file (which is written last, so  all other files should exist)
			fn = os.path.join(cur,db_filename)
			if not os.path.isfile(fn): continue
			with open(fn, 'r') as fh:
				datum = json.load(fh)
				# get relative path wrt. to the data root
				rel_path = os.path.relpath(cur, source_dir)
				# update the entries with the relative path
				if datum['status'] != 'COMPLETED' and skip_incomplete:
					continue

				for k,v in datum['result'].items():
					datum['result'][k] = os.path.join(rel_path, v)

				db.insert(datum)

		except Exception as e:
			if logger:
				logger.INFO("Something went wrong loading subdirectory %s. Skipping it."%cur)
				logger.INFO(e)
			else:
				print("Something went wrong loading subdirectory %s. Skipping it."%cur)
				print(e)
			continue

	# replace old db file with current one
	os.rename(tmp_filename, destination)


class tinydb_data(object):
	""" Class to access the data. It's main purpose is to load the stored values
		from disk for a given list of keys.
	"""
	def __init__ (self, db_file, data_root_dir):
		self.db_file=db_file
		self.data_root_dir = data_root_dir

		self.db = tinydb.TinyDB(db_file)
		self.q = tinydb.Query()

	def query(self, query):
		return(self.db.search(query))

	def load_keys(self, datum, keys):
		""" loads numpy arrays from disk for the corresponding keys"""

		return_dict = {}

		for k in keys:
			try:
				with gzip.GzipFile(os.path.join(self.data_root_dir, datum['result'][k]), 'rb') as fh:
					return_dict[k] = pickle.load(fh)
			except Exception as e:
				print(e)
				pass
				#TODO: raise a more meaningful Warning or something
		return(return_dict)


