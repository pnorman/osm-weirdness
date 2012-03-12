#!/usr/bin/env python

import sys
import urllib2
import xml.etree.cElementTree as ElementTree
import simplejson as json
from datetime import datetime
import StringIO
import gzip
import time
import math

JXAPI_BASE = 'http://localhost:8080/xapi/'

VERBOSE = False

# Parse the diff and write out a simplified version
class OscHandler():
  def __init__(self):
    self.changes = {}
    self.nodes = {}
    self.ways = {}
    self.relations = {}
    self.action = ""
    self.primitive = {}
    self.missingNds = set()

  def startElement(self, name, attributes):
    if name in ('modify', 'delete', 'create'):
      self.action = name
    if name in ('node', 'way', 'relation'):
      self.primitive['id'] = int(attributes['id'])
      self.primitive['version'] = int(attributes['version'])
      self.primitive['changeset'] = int(attributes['changeset'])
      self.primitive['user'] = attributes.get('user')
      self.primitive['timestamp'] = isoToTimestamp(attributes['timestamp'])
      self.primitive['tags'] = {}
      self.primitive['action'] = self.action
    if name == 'node':
      self.primitive['lat'] = float(attributes['lat'])
      self.primitive['lon'] = float(attributes['lon'])
    elif name == 'tag':
      key = attributes['k']
      val = attributes['v']
      self.primitive['tags'][key] = val
    elif name == 'way':
      self.primitive['nodes'] = []
    elif name == 'relation':
      self.primitive['members'] = []
    elif name == 'nd':
      ref = int(attributes['ref'])
      self.primitive['nodes'].append(ref)
      if ref not in self.nodes:
        self.missingNds.add(ref)
    elif name == 'member':
      self.primitive['members'].append(
                                    {
                                     'type': attributes['type'],
                                     'role': attributes['role'],
                                     'ref': attributes['ref']
                                    })

  def endElement(self, name):
    if name == 'node':
      self.nodes[self.primitive['id']] = self.primitive
    elif name == 'way':
      self.ways[self.primitive['id']] = self.primitive
    elif name == 'relation':
      self.relations[self.primitive['id']] = self.primitive
    if name in ('node', 'way', 'relation'):
      self.primitive = {}

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

def requestNodes(ndlist):
  url = JXAPI_BASE + "api/0.6/node/%s" % (",".join(map(str, ndlist)))
  content = urllib2.urlopen(url)
  return content

def parseOsm(source, handler):
  for event, elem in ElementTree.iterparse(source, events=('start', 'end')):
    if event == 'start':
      handler.startElement(elem.tag, elem.attrib)
    elif event == 'end':
      handler.endElement(elem.tag)
    elem.clear() 

# from http://stackoverflow.com/questions/2348317/how-to-write-a-pager-for-python-iterators
def grouper( page_size, iterable ):
  page= []
  for item in iterable:
    page.append( item )
    if len(page) == page_size:
      yield page
      page= []
  yield page

# from http://stackoverflow.com/questions/1335392/iteration-over-list-slices/1335466#1335466
def sliceIterator(lst, sliceLen):
  for i in range(len(lst) - sliceLen + 1):
    yield lst[i:i + sliceLen]

def minutelyUpdateRun():

  # Read the state.txt
  sf = open('state.txt', 'r')

  state = {}
  for line in sf:
    if line[0] == '#':
      continue
    (k, v) = line.split('=')
    state[k] = v.strip().replace("\\:", ":")

  minuteNumber = int(isoToTimestamp(state['timestamp'])) / 60
  if VERBOSE:
    print "Minute Number: %s" % (minuteNumber)

  # Grab the sequence number and build a URL out of it
  sqnStr = state['sequenceNumber'].zfill(9)
  url = "http://planet.openstreetmap.org/minute-replicate/%s/%s/%s.osc.gz" % (sqnStr[0:3], sqnStr[3:6], sqnStr[6:9])

  if VERBOSE:
    print "Downloading change file (%s)." % (url)
  content = urllib2.urlopen(url)
  content = StringIO.StringIO(content.read())
  gzipper = gzip.GzipFile(fileobj=content)

  if VERBOSE:
    print "Parsing change file."
  handler = OscHandler()
  parseOsm(gzipper, handler)

  # Fetch from jxapi the nodes that weren't in the changeset
  if VERBOSE:
    print "Filling in %d missing nodes." % (len(handler.missingNds))
  ndchunk = []
  for group in grouper(350, handler.missingNds):
    if group != []:
      parseOsm(requestNodes(group), handler)

  # Now that we have the data in memory, start looking for suspicious-looking changes
  
  print "%s/%s/%s.osc: %d nodes, %d ways, %d relations." % (sqnStr[0:3], sqnStr[3:6], sqnStr[6:9], len(handler.nodes), len(handler.ways), len(handler.relations))
  ## Ways should not have bends > 95deg in them
  for way in handler.ways.itervalues():
    if way['action'] == 'delete':
      # Skip deleted ways for now
      continue

    prevNode = None
    nds = way['nodes']
    ndCount = len(nds)
    if ndCount <= 1:
      print "Action was %s" % (way['action'])
      print "!!! Way %d consists of one or fewer nodes." % (way['id'])
    elif ndCount == 2:
      try:
        node1 = handler.nodes[nds[0]]
        node2 = handler.nodes[nds[1]]
      except KeyError, e:
        print "Node %s was not found in way %d. Skipping." % (e, way['id'])
        continue
      if distanceBetweenNodes(node1, node2) > 0.3:
        print "!!! Way %d consists of two nodes that are far apart." % (way['id'])
    else:
      totalAngle = 0.0
      for ndSlice in sliceIterator(nds, 3):
        nd1 = ndSlice[0]
        nd2 = ndSlice[1]
        nd3 = ndSlice[2]
        try:
          A = handler.nodes[nd1]
          B = handler.nodes[nd3]
          C = handler.nodes[nd2]
        except KeyError, e:
          print "Node %s was not found in way %d. Skipping." % (e, way['id'])
          continue
        a = distanceBetweenNodes(C, B)
        b = distanceBetweenNodes(C, A)
        c = distanceBetweenNodes(A, B)

        if 0.0 in (a, b, c):
          if a == 0.0:
            print "!!! Way %d has nodes %d and %d that overlap." % (way['id'], nd2, nd3)
          if b == 0.0:
            print "!!! Way %d has nodes %d and %d that overlap." % (way['id'], nd1, nd3)
          if c == 0.0:
            print "!!! Way %d has nodes %d and %d that overlap." % (way['id'], nd1, nd2)
          continue
 
        try:
          totalAngle = totalAngle +  angleBetweenNodes(a, b, c)
        except ValueError, e:
          print "a=%f b=%f c=%f" % (a, b, c)
          raise e
      
      averageAngle = totalAngle / (len(nds) - 2)
      #print "Way %d has average angle %f." % (way['id'], averageAngle)
      if averageAngle < 60.0 or averageAngle > 210.0:
          print "!!! Way %d is abnormally shaped with an average angle of %f." % (way['id'], averageAngle)

  #sys.exit(0)
  # Download the next state file
  nextSqn = int(state['sequenceNumber']) + 1
  sqnStr = str(nextSqn).zfill(9)
  url = "http://planet.openstreetmap.org/minute-replicate/%s/%s/%s.state.txt" % (sqnStr[0:3], sqnStr[3:6], sqnStr[6:9])
  try:
    u = urllib2.urlopen(url)
    statefile = open('state.txt', 'w')
    statefile.write(u.read())
    statefile.close()
  except Exception, e:
    print e
    return False
  return True

if __name__ == "__main__":
  while True:
    while minutelyUpdateRun():
	  pass
  time.sleep(60)