#!/usr/bin/env python
# coding=utf-8
from __future__ import division, print_function, unicode_literals
import json
import os
import os.path
import tempfile
import gzip,bz2
import pickle

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
        if not os.path.exists(basedir):
            os.makedirs(basedir)
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
