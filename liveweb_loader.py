#!/usr/bin/env python

"""Usage: liveweb_loader.py [options]

Options:
  -h --help         show this help message and exit
  --config=FILE     location of config.yml
  --daemon          daemonize and run in background
"""

import os
import re
import sys
import yaml
import socket
import syslog
import logging
import traceback
from datetime import datetime
from docopt import docopt
from s3_loader import S3_Loader


class Liveweb_Loader(S3_Loader):
    def get_seq_num(self, file):
        m = re.match('\w+-\d+-(\d+).w?arc.gz$', file)
        if m:
            seq_num = m.group(1)
        else:
            self.logger.info('Could not find sequence number in filename (%s), using 00000 instead.' % (file))
            seq_num = '00000'
        return seq_num


    def get_timestamp(self, file):
        m = re.match('\w+-(\d+)-\d+.w?arc.gz$', file)
        if m:
            timestamp = m.group(1)
        else:
            self.logger.info('Could not find timestamp in filename (%s), using ctime instead.' % (file))
            secs = os.path.getctime(os.path.join(self.dir, file))
            dt = datetime.utcfromtimestamp(secs)
            #truncating the six digits of microseconds to three digits
            #is the same as converting to milliseconds (div by 1000)
            timestamp = dt.strftime('%Y%m%d%H%M%S%f')[:17]
        return timestamp


    def make_bucket_name(self, filelist):
        first_seq_num   = self.get_seq_num(filelist[0])
        first_timestamp = self.get_timestamp(filelist[0])
        last_timestamp  = self.get_timestamp(filelist[-1])
        bucket_name = "%s-%s-%s-%s" % (self.upload_prefix, first_timestamp, first_seq_num, last_timestamp)
        return bucket_name


    def format_metadata(self, filelist, upload_size):
        host = socket.gethostname()
        first_timestamp = self.get_timestamp(filelist[0])
        last_timestamp  = self.get_timestamp(filelist[-1])
        start_date = datetime.strptime(first_timestamp[:14], '%Y%m%d%H%M%S').isoformat() + ' UTC'
        end_date   = datetime.strptime(last_timestamp[:14], '%Y%m%d%H%M%S').isoformat() + ' UTC'

        re_host  = re.compile('CRAWLHOST')
        re_start = re.compile('START_DATE')
        re_end   = re.compile('END_DATE')

        headers = {}
        for k, v in self.metadata.iteritems():
            v = re_host.sub(host, v)
            v = re_start.sub(start_date, v)
            v = re_end.sub(end_date, v)

            key = 'x-archive-meta-'+k
            headers[key] = v

        headers['x-archive-size-hint'] = str(upload_size)

        self.logger.debug('metadata dict:')
        self.logger.debug(headers)

        return headers


if __name__ == "__main__":
    import s3_loader.s3_loader

    script_name = sys.argv[0].split('/')[-1]

    #read cli options and config.yml
    options, arguments = docopt(__doc__)
    if options.config is False:
        exit('Must supply path to config.yml via the --config option')
    d = yaml.safe_load(open(options.config))

    if options.daemon is False:
        #logging.basicConfig(level=logging.DEBUG) #uncomment to turn on verbose boto logging
        logger = s3_loader.s3_loader.get_logger(script_name, logging.DEBUG)

        s3_loader = Liveweb_Loader(d['dir'], d['prefix'], d['s3_key'], d['s3_secret'], d['metadata'], logger, maxfiles=2)
        s3_loader.run()
    else:
        logger = s3_loader.s3_loader.get_logger(script_name, logging.INFO, use_syslog=True)
        syslog.openlog(script_name, syslog.LOG_PID, syslog.LOG_DAEMON)
        s3_loader.s3_loader.daemonize()

        try:
            s3_loader = Liveweb_Loader(d['dir'], d['prefix'], d['s3_key'], d['s3_secret'], d['metadata'], logger)
            s3_loader.run()
        except:
            t = traceback.format_exc()
            for l in t.split('\n'):
                syslog.syslog(l)
            time.sleep(61)
            raise
