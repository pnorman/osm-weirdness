#!/usr/bin/env python

import sys
import xml.etree.cElementTree as ElementTree
import simplejson as json
from datetime import datetime
import time
import math

JXAPI_BASE = 'http://localhost:8080/xapi/'

VERBOSE = False

# A running list of changesets
changesets = {}

import osmdifffetcher
myfetcher = osmdifffetcher.DiffFetcher()

myfetcher.init_latest()

class Changeset:
  def __init__(self, name, attr):
    self.starttime = isoToTimestamp(attr['timestamp'])
    self.lasttime = self.starttime 
    self.user = attr['user']
    self.created={'node':0,'way':0,'relation':0}
    self.deleted={'node':0,'way':0,'relation':0}
    self.modified={'node':0,'way':0,'relation':0}
    
  def parse_create(self, name, attr):
    self.created[name] += 1
    self.starttime = min(self.starttime, isoToTimestamp(attr['timestamp']))
    self.lasttime = max(self.lasttime, isoToTimestamp(attr['timestamp']))
    
  def parse_delete(self, name, attr):
    self.deleted[name] += 1
    self.starttime = min(self.starttime, isoToTimestamp(attr['timestamp']))
    self.lasttime = max(self.lasttime, isoToTimestamp(attr['timestamp']))

  def parse_modify(self, name, attr):
    self.modified[name] += 1
    self.starttime = min(self.starttime, isoToTimestamp(attr['timestamp']))
    self.lasttime = max(self.lasttime, isoToTimestamp(attr['timestamp']))
  
  @property
  def objects(self):
    return sum(self.created.itervalues())+sum(self.deleted.itervalues())+sum(self.modified.itervalues())
  
  @property
  def objects_created(self):
    return sum(self.created.itervalues())
  
  @property
  def objects_deleted(self):
    return sum(self.deleted.itervalues())
  
  @property
  def objects_modified(self):
    return sum(self.modified.itervalues())
  
  @property
  def nodes(self):
    return self.created['node'] + self.deleted['node'] + self.modified['node']

  @property
  def ways(self):
    return self.created['way'] + self.deleted['way'] + self.modified['way']

  @property
  def relations(self):
    return self.created['relation'] + self.deleted['relation'] + self.modified['relation']
  
# Parse the diff and write out a simplified version
class OscHandler():
  def __init__(self):
    self.action = ''
 
  def startElement(self, name, attributes):
    if name in ('modify', 'delete', 'create'):
      self.action = name

    if name in ('node', 'way', 'relation'):
      if not attributes['changeset'] in changesets:
        changesets[attributes['changeset']]=Changeset(name, attributes)
      if self.action == 'modify':
        changesets[attributes['changeset']].parse_modify(name, attributes)
      elif self.action == 'delete':
        changesets[attributes['changeset']].parse_delete(name, attributes)
      elif self.action == 'create':
        changesets[attributes['changeset']].parse_create(name, attributes)
      else:
        raise UserWarning   
        
  def endElement(self, name, attributes):
    if name in ('modify', 'delete', 'create'):
      self.action = ''

def isoToTimestamp(isotime):
  t = datetime.strptime(isotime, "%Y-%m-%dT%H:%M:%SZ")
  return time.mktime(t.timetuple())
  
def distanceBetweenNodes(node1, node2):
  dlat = math.fabs(node1['lat'] - node2['lat'])
  dlon = math.fabs(node1['lon'] - node2['lon'])
  return math.hypot(dlat, dlon)

def angleBetweenNodes(a, b, c):
  d = ((a * a) + (b * b) - (c * c)) / (2.0 * a * b)
  if 1.0 - d < 0.00001:
    d = 1.0
  elif 1.0 + d < 0.00001:
    d = -1.0
  return math.degrees(math.acos(d))

def parseOsm(source, handler):
  for event, elem in ElementTree.iterparse(source, events=('start', 'end')):
    if event == 'start':
      handler.startElement(elem.tag, elem.attrib)
    elif event == 'end':
      handler.endElement(elem.tag, elem.attrib)
    elem.clear()

def minutelyUpdateRun():

  # Read the state.txt

  diff = myfetcher.next_wait()
  parseOsm(diff, OscHandler())
  return True

warned = {}
def warnset(type, number, cs, message=None):
  type = str(type)
  if not type in warned:
    warned[type] = []
  if not number in warned[type]:
    if not message:
      message = type
    print 'CS %s by %s: %s (c%s m%s d%s)' % (number, cs.user, message, cs.objects_created, cs.objects_modified, cs.objects_deleted)
    warned[type].append(number)

    
def detect():
  while True:
    while minutelyUpdateRun():
      for n, cs in changesets.iteritems():
        if cs.objects > 300:
          if cs.objects > 5000:
            warnset('5000', n, cs)
          if cs.objects > 10000:
            warnset('10000', n, cs)
          if cs.objects > 25000:
            warnset('25000', n, cs)
          if cs.objects > 45000:
            warnset('45000', n, cs)
           
          if cs.objects > 1500:
            if cs.objects == cs.created['node']:
              warnset('onlynodes', n, cs)
            if cs.objects == cs.objects_deleted:
              warnset('onlydelete', n, cs)
          
          # shouldn't look at moved nodes, just retagged
          if cs.created['node'] < 0.05*cs.objects and cs.objects_modified > 0.85*cs.objects:
            warnset('mechanical1', n, cs, 'Suspicous (Mechanical edit): Mainly modified objects')
          
          # Wait for a new minutely diff to be generated. Over time the script will slip farther and farther behind until it catches up by processing two diffs at once.
    time.sleep(60.0)
    

if __name__ == "__main__":
  warned = {}
  
  detect()