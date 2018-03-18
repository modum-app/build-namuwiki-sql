#!/usr/bin/python
# coding=utf-8

# Converts Namu Wiki MySQL dump to a custom sqlite3 format mainly for iOS offline reader app
# Copyright (C) 2016  Yeonwoon JUNG <flow3r@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import re
import os
import sys
import getopt
import sqlite3
import pylzma
import json
from datetime import datetime

if sys.platform != 'win32':
  import signal
  signal.signal(signal.SIGPIPE, signal.SIG_DFL)

def usage():
  print r'usage: build-namuwiki-sql.py [--no-data] [--force] [--output=path] [--expected=#] [--sample=#]'
  print
  print r'example:'
  print r'  $ 7zcat namuwiki160126.7z | build-namuwiki-sql.py'
  print

class Option:
  NoData = False # True if you want to build index-only dump
  Force = False  # True if you want to overwrite the existing file
  Output = ''    # output filename
  Expected = 547202 # expected (or estimated) number of entries to feed (display progress bar)
  Sample = 0     # generates sample output with specified number of articles

def config():
  try:
    opts,args = getopt.getopt(sys.argv[1:],
                              'hnfo:e:s:',
                              ['help','no-data','force','output=','expected=','sample='])
    for k,v in opts:
      if k in ('-h','--help'):
        usage()
        return False
      elif k in ('-n','--no-data'):
        Option.NoData = True
      elif k in ('-f','--force'):
        Option.Force = True
      elif k in ('-o','--output'):
        Option.Output = v
      elif k in ('-e','--expected'):
        Option.Expected = int(v)
      elif k in ('-s','--sample'):
        Option.Sample = int(v)
    if not Option.Output:
      # default output filename
      date = datetime.now().date().strftime("%y%m%d")
      Option.Output = 'namuwiki-'+date+'.sql'
    return True
  except getopt.GetoptError, err:
    print str(err)
    usage()


class JSONStream:
  def __init__(self, inputStream):
    self.stream = inputStream
    self.buffer = self.read()
    assert self.buffer.startswith('[')
    self.buffer = self.buffer[1:]
    assert self.buffer.startswith('{')

  def __iter__(self):
    return iter(self.next, None)

  def read(self):
    return self.stream.read(8192*2)

  def item(self):
    rear = self.buffer.find('},{"namespace":"')
    rear = self.buffer.find('}]\n') if rear == -1 else rear
    return rear>0

  def move(self):
    rear = self.buffer.find('},{"namespace":"')
    rear = self.buffer.find('}]\n') if rear == -1 else rear
    item = json.loads(self.buffer[:rear+1])
    self.buffer = self.buffer[rear+2:]
    return item

  def next(self):
    while self.item()==False:
      data = self.read()
      if data=='':
        self.buffer += '\n'
        if not self.item():
          print repr(self.buffer)
          assert self.buffer=='' or (len(self.buffer)==1 and self.buffer[0]=='\n')
          return None
      else:
        self.buffer += data
    return self.move()


class SQLWriter:
  """ create sqlite db from mysqldump using standard input
  """
  nsprefix = (u"",u"틀:",u"분류:",u"파일:",u"사용자:",u"#ns5:",u"나무위키:",u"#ns7:",u"#ns8:")
  nsfilter = (0,1,2,6)
  MaxArChunkSize = 1024*1024 # 1M

  def __init__(self,output,force=False,nodata=False,expected=0,sample=0):
    self.fn = output
    self.force = force
    self.nodata = nodata
    self.sample = sample
    self.init_db()

    self.total_num_docs = 0
    self.expected_total = expected

    self.art = 0
    self.init_chunk()

  def init_db(self):
    if os.path.exists(self.fn):
      if self.force:
        os.remove(self.fn)
      else:
        print 'file %s already exists!' % (self.fn,)
        sys.exit(2)

    self.conn = sqlite3.connect(self.fn)
    self.c = self.conn.cursor()
    self.c.execute("""CREATE TABLE doc (name TEXT UNIQUE, art INTEGER, off INTEGER, len INTEGER);""")
    self.c.execute("""CREATE VIRTUAL TABLE idx using fts4(name TEXT);""")
    self.c.execute("""CREATE TABLE art (art INTEGER PRIMARY KEY, data BLOB);""")
    self.c.execute("""CREATE TABLE cat (name TEXT, artn TEXT);""")
    self.c.execute("""CREATE TABLE inc (name TEXT, artn TEXT);""")

  def init_chunk(self):
    self.art += 1
    self.off = 0
    self.buf = u''

  def close_db(self):
    self.conn.commit()
    self.conn.close()

  def read_cats(self,cname,data):
    """read categories from the article and its includes."""
    categories = re.findall(ur'\[\[분류:(.+?)\]\]', data)
    for cat in categories:
      self.c.execute("""INSERT INTO cat(name,artn) VALUES(?,?)""", (cat,cname))

    includes = re.findall(ur'\[include\((.+?)(?:,.+)?\)\]', data)
    for inc in includes:
      #normalize names that have spaces between namespace and title; e.g. '틀: 이름'
      nsname = inc[:inc.find(':')+1]
      if nsname and (nsname in SQLWriter.nsprefix):
        nnc = re.sub(ur'^('+nsname+r') +',ur'\1',inc,1)
        self.c.execute("""INSERT INTO inc(name,artn) VALUES(?,?)""", (nnc,cname))
      else:
        self.c.execute("""INSERT INTO inc(name,artn) VALUES(?,?)""", (inc,cname))

  def on_row(self,row):
    data,ns,contrib,name = row.values()
    datalen = len(buffer(data))
    ns = int(ns)
    if ns in SQLWriter.nsfilter:
      if self.off+datalen > SQLWriter.MaxArChunkSize:
        self.commit_chunk()
      try:
        cname = (SQLWriter.nsprefix[ns]+name)
        self.c.execute("""INSERT INTO doc(name,art,off,len) VALUES(?,?,?,?)""", (cname,self.art,self.off,datalen))
        self.c.execute("""INSERT INTO idx(name) VALUES(?)""", (cname,))
        self.off += datalen;
        self.buf += data

        if not self.nodata:
          self.read_cats(cname,data)

      except Exception as e:
        print 'Err:',repr(name), e.message, data[:80]
        #sys.exit(1)

  def commit_chunk(self):
    if self.off:
      cdata = buffer(u'') if self.nodata else buffer(pylzma.compress(buffer(self.buf)))
      self.c.execute("""INSERT INTO art(art,data) VALUES(?,?)""", (self.art,cdata))
      self.init_chunk()
      self.conn.commit()

  def run(self):
    for item in JSONStream(sys.stdin):
      self.on_row(item)
      self.on_progress(1)

      if self.sample and self.total_num_docs >= self.sample:
        return

  def on_progress(self,num):
    self.total_num_docs += num
    print '\r',' '*60,'\r%08d'%(self.total_num_docs,),'(+% 4d, ~ %02.02f%%)'%(num, self.total_num_docs/float(self.expected_total)*100),': ',
    sys.stdout.flush()

  def done(self):
    self.close_db()
    print 'done'
    print '%d entires were inserted in total'%self.total_num_docs


def main():
  if not config():
    sys.exit(2)

  sqlWriter = SQLWriter(Option.Output,
                        nodata=Option.NoData,
                        force=Option.Force,
                        expected=Option.Expected,
                        sample=Option.Sample)
  try:
    sqlWriter.run()
    sqlWriter.commit_chunk()
  finally:
    sqlWriter.done()


if __name__ == '__main__':
  main()

