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

import os
import sys
import getopt
import sqlite3
import pylzma
from datetime import datetime

import signal
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

def usage():
  print r'usage: build-namuwiki-sql.py [--no-data] [--force] [--output=path] [--sample=#]'
  print
  print r'example:'
  print r'  $ 7zcat namuwiki160126.7z | build-namuwiki-sql.py'
  print

class Option:
  NoData = False # True if you want to build index-only dump
  Force = False  # True if you want to overwrite the existing file
  Output = ''    # output filename
  Sample = 0     # generates sample output with specified number of articles

def config():
  try:
    opts,args = getopt.getopt(sys.argv[1:],
                              'hnfo:s:',
                              ['help','skip-data','force','output=','sample='])
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


class SQLWriter:
  """ create sqlite db from mysqldump using standard input
  """
  nsprefix = ("","틀:","분류:","파일:","사용자:","#ns5:","나무위키:","#ns7:","#ns8:")
  nsfilter = (0,1,2,6)
  MaxArChunkSize = 1024*1024 # 1M

  def __init__(self,output,force=False,nodata=False,sample=0):
    self.fn = output
    self.force = force
    self.nodata = nodata
    self.sample = sample
    self.init_db()

    self.total_num_docs = 0
    self.expected_total = 400000

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

  def init_chunk(self):
    self.art += 1
    self.off = 0
    self.buf = ''

  def close_db(self):
    self.conn.commit()
    self.conn.close()

  def on_row(self,row):
    ns,name,data = row
    if ns in SQLWriter.nsfilter:
      if self.off+len(data) > SQLWriter.MaxArChunkSize:
        self.commit_chunk()
      try:
        cname = (SQLWriter.nsprefix[ns]+name).decode('utf8');
        self.c.execute("""INSERT INTO doc(name,art,off,len) VALUES(?,?,?,?)""", (cname,self.art,self.off,len(data)))
        self.c.execute("""INSERT INTO idx(name) VALUES(?)""", (cname,))
        self.off += len(data)
        self.buf += data
      except Exception as e:
        print repr(name), e.message, data[:80]
        #sys.exit(1)

  def commit_chunk(self):
    if self.off:
      cdata = buffer('') if self.nodata else buffer(pylzma.compress(self.buf))
      self.c.execute("""INSERT INTO art(art,data) VALUES(?,?)""", (self.art,cdata))
      self.init_chunk()
      self.conn.commit()

  def run(self):
    for line in sys.stdin:
      if line.startswith('INSERT INTO'):
        values = line[line.find('('):-2]
        assert values
        assert values[0] == '('
        assert values[-1] == ')'
        rows = eval(values)
        nrows = len(rows)
        self.on_progress(nrows)

        prog=0
        for i,row in enumerate(rows):
          self.on_row(row)
          curr=int(i*20./nrows)
          if prog<curr:
            prog=curr
            sys.stdout.write('.')
            sys.stdout.flush()

        sys.stdout.write('.')
        sys.stdout.flush()

        if self.sample and self.total_num_docs > self.sample:
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
                        sample=Option.Sample)
  try:
    sqlWriter.run()
    sqlWriter.commit_chunk()
  finally:
    sqlWriter.done()


if __name__ == '__main__':
  main()

