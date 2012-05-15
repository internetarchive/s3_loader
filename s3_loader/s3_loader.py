#!/usr/bin/env python

import os
import re
import sys
import time
import syslog
import logging
from datetime import datetime

import yaml
import boto
from boto.s3.key import Key

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(threadName)18s %(levelname)5s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")


class S3_Loader():
    def __init__(self, dir, prefix, s3_key, s3_secret, max_files=10, max_size=1073741824):
        self.dir = dir
        assert os.path.exists(dir)

        self.max_files = max_files
        self.max_size  = max_size

        self.s3_key    = s3_key
        self.s3_secret = s3_secret
        self.s3_host   = 's3.us.archive.org'
        self.s3_secure = False

        self.upload_prefix = prefix


    def get_dir_contents(self):
        files = sorted(os.listdir(self.dir))
        sizes = [os.path.getsize(os.path.join(self.dir, f)) for f in files]
        return files, sizes


    def make_filelist(self, files, sizes):
        upload_size = 0
        filelist = []
        for file, size in zip(files, sizes):
            filelist.append(file)

            if len(filelist) == self.max_files:
                break
            if upload_size >= self.max_size:
                #if there is a single file larger than max_size, upload it anyway
                if len(filelist) > 1:
                    a.pop()
                break

        return filelist


    def get_seq_num(self, file):
        m = re.match('\w+-\d+-(\d+).w?arc.gz$', file)
        if m:
            seq_num = m.group(1)
        else:
            logging.info('Could not find sequence number in filename (%s), using 00000 instead.' % (file))
            seq_num = '00000'
        return seq_num


    def get_timestamp(self, file):
        m = re.match('\w+-(\d+)-\d+.w?arc.gz$', file)
        if m:
            timestamp = m.group(1)
        else:
            logging.info('Could not find timestamp in filename (%s), using ctime instead.' % (file))
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


    def s3_get_bucket(self, conn, bucket_name):
        #Maybe we already tried to make a bucket on a previous run, but the
        #catalog was locked up. Let's see how it is looking..
        bucket = conn.lookup(bucket_name)
        if bucket is not None:
            logging.info('Found existing bucket ' + bucket_name)
            return bucket

        logging.info('Creating bucket ' + bucket_name)
        #todo: do we need to add retry?
        bucket = conn.create_bucket(bucket_name)

        #Now we need to block until the item has been created in paired storage
        #so subsequent writes will work
        i=0
        while i<10:
            b = conn.lookup(bucket_name)
            if b is not None:
                break
            logging.debug('Waiting for bucket creation...')
            time.sleep(60)
            i+=1

        return bucket


    def s3_upload_file(self, bucket, filename):
        logging.info('Uploading ' + filename)
        key = Key(bucket)
        key.name = filename
        key.set_contents_from_filename(os.path.join(self.dir, filename))


    def upload_and_delete_files(self, files, sizes):
        filelist    = self.make_filelist(files, sizes)
        bucket_name = self.make_bucket_name(filelist)

        conn = boto.connect_s3(self.s3_key, self.s3_secret, host=self.s3_host, is_secure=self.s3_secure)
        bucket = self.s3_get_bucket(conn, bucket_name)
        for filename in filelist:
            if bucket.get_key(filename) is not None:
                logging.warning('File %s already exists, not deleting from server!' % filename)
                continue
            self.s3_upload_file(bucket, filename)

            logging.info('Deleting local copy of %s' % filename)
            os.unlink(os.path.join(self.dir, filename))


    def run(self):
        while True:
            print("Starting s3 uploader, waiting for files...\n")
            files, sizes = self.get_dir_contents()
            num_files = len(files)
            size = sum(sizes)

            if num_files >= self.max_files:
                logging.info('num_files (%d) >= max_files (%d), uploading!' % (num_files, self.max_files))
                self.upload_and_delete_files(files, sizes)
            elif size >= self.max_size:
                logging.info('size (%d) >= max_size (%d), uploading!' % (size, self.max_size))
                self.upload_and_delete_files(files, sizes)
            else:
                logging.debug('num_files (%d) < max_files (%d) and size (%d) < max_size (%d), waiting for more files.' % (num_files, self.max_files, size, self.max_size))

            logging.debug('Sleeping')
            time.sleep(600)


if __name__ == "__main__":
    d = yaml.safe_load(open('config.yml'))
    s3_loader = S3_Loader(d['dir'], d['prefix'], d['s3_key'], d['s3_secret'])
    s3_loader.run()
