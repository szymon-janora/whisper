#!/usr/bin/env python
import operator
import os
import sys
import time
import signal
import optparse
import traceback
from itertools import zip_longest

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

# Ignore SIGPIPE
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

now = int(time.time())

option_parser = optparse.OptionParser(
    usage='''%prog path timePerPoint:timeToStore [timePerPoint:timeToStore]*

timePerPoint and timeToStore specify lengths of time, for example:

60:1440      60 seconds per datapoint, 1440 datapoints = 1 day of retention
15m:8        15 minutes per datapoint, 8 datapoints = 2 hours of retention
1h:7d        1 hour per datapoint, 7 days of retention
12h:2y       12 hours per datapoint, 2 years of retention
''')

option_parser.add_option(
    '--xFilesFactor', default=None,
    type='float', help="Change the xFilesFactor")
option_parser.add_option(
    '--aggregationMethod', default=None,
    type='string', help="Change the aggregation function (%s)" %
    ', '.join(whisper.aggregationMethods))
option_parser.add_option(
    '--force', default=False, action='store_true',
    help="Perform a destructive change")
option_parser.add_option(
    '--newfile', default=None, action='store',
    help="Create a new database file without removing the existing one")
option_parser.add_option(
    '--nobackup', action='store_true',
    help='Delete the .bak file after successful execution')

(options, args) = option_parser.parse_args()

if len(args) < 2:
  option_parser.print_help()
  sys.exit(1)

path = args[0]

if not os.path.exists(path):
  sys.stderr.write("[ERROR] File '%s' does not exist!\n\n" % path)
  option_parser.print_help()
  sys.exit(1)

info = whisper.info(path)

new_archives = [whisper.parseRetentionDef(retentionDef)
                for retentionDef in args[1:]]
# sort by precision, highest to lowest
new_archives.sort(key=operator.itemgetter(0))

old_archives = info['archives']
# sort by precision, highest to lowest
old_archives.sort(key=lambda a: a['secondsPerPoint'])

if [a['secondsPerPoint'] for a in old_archives] == new_archives:
  print("The new retention is equal to the old retention!")
  sys.exit(1)

if options.xFilesFactor is None:
  xff = info['xFilesFactor']
else:
  xff = options.xFilesFactor

if options.aggregationMethod is None:
  aggregationMethod = info['aggregationMethod']
else:
  aggregationMethod = options.aggregationMethod

if options.newfile is None:
  tmpfile = path + '.tmp'
  if os.path.exists(tmpfile):
    print('Removing previous temporary database file: %s' % tmpfile)
    os.unlink(tmpfile)
  newfile = tmpfile
else:
  newfile = options.newfile

print('Creating new whisper database: %s' % newfile)
whisper.create(newfile, new_archives, xFilesFactor=xff, aggregationMethod=aggregationMethod)
size = os.stat(newfile).st_size
print('Created: %s (%d bytes)' % (newfile, size))


def find_new_archive(old_archive, new_archives):
  old_archive_retention = old_archive['retention']
  old_precision = old_archive['secondsPerPoint']
  best_fit_new_archive = None
  for new_archive in list(new_archives):
    new_precision, new_points = new_archive
    new_retention = new_precision * new_points
    if new_precision <= old_precision and (old_precision % new_precision) == 0:
      best_fit_new_archive = new_archive
      if new_retention <= old_archive_retention:
        new_archives.remove(new_archive)
      if new_retention >= old_archive_retention:
        return best_fit_new_archive, None
    elif new_retention >= old_archive_retention and (new_precision % old_precision) == 0:
      if new_retention == old_archive_retention:
        new_archives.remove(new_archive)
      return best_fit_new_archive, new_archive
  return best_fit_new_archive, None


def grouped(n, iterable, fillvalue=None):
    """grouped(2, [0, 1, 2, 3, 4], None) --> (0, 1) (2, 3) (4, None)"""
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)


def write_datapoints(timeinfo, values):
  datapoints = zip(range(*timeinfo), values)
  datapoints = filter(lambda p: p[1] is not None, datapoints)
  whisper.update_many(newfile, datapoints)


print('Migrating all data from the old archives into a new retention')
fromTime = now
for old_archive in old_archives:
  # Retrieve data from the old archive
  untilTime = fromTime
  fromTime = now - old_archive['retention']
  old_timeinfo, old_values = whisper.fetch(path, fromTime, untilTime, now)

  # Find a new archvie for old data
  best_fit_new_archive, fit_new_archive = find_new_archive(old_archive, new_archives)
  old_archive_retention = "%ss:%ss" % (old_archive['secondsPerPoint'], old_archive['retention'])
  if not best_fit_new_archive and not fit_new_archive:
    if options.force:
      print("Migration dropped the old archives from the archive %s!" % old_archive_retention)
      break
    else:
      print("Migration couldn't fit the old archive (%s) into a new retention" % old_archive_retention)
      sys.exit(1)
  elif not fit_new_archive:
    best_retention = best_fit_new_archive[0] * best_fit_new_archive[1]  # best_precision * best_points
    if best_retention < old_archive['retention'] and not options.force:
        print("Migration couldn't fit the old archive (%s) into too small new retention (%ss)" % (old_archive_retention, best_retention))
        sys.exit(1)
    write_datapoints(old_timeinfo, old_values)
    continue
  old_precision = old_archive['secondsPerPoint']
  start, end, step = old_timeinfo

  # Migrate old data into a best fit new archive
  best_precision, best_points = best_fit_new_archive
  best_new_points = int(best_points / (old_precision / best_precision))
  best_new_values = old_values[len(old_values) - best_new_points:]
  best_new_timeinfo = (end - (best_new_points * step), end, step)
  write_datapoints(best_new_timeinfo, best_new_values)

  # Migrate old data into a fit new archive
  fit_precision, fit_points = fit_new_archive
  num_of_aggregation_points = int(fit_precision / old_precision)
  fit_values = old_values[:best_new_points]
  new_values = []
  for aggregation_values in grouped(num_of_aggregation_points, fit_values):
    non_none_values = list(filter(lambda x: x is not None, aggregation_values))
    if non_none_values and 1.0 * len(non_none_values) / len(aggregation_values) >= xff:
      new_values.append(whisper.aggregate(aggregationMethod,
                                          non_none_values, aggregation_values))
    else:
      new_values.append(None)
  new_timeinfo = (start, start + len(new_values) * fit_precision, fit_precision)
  write_datapoints(new_timeinfo, new_values)

if options.newfile is not None:
  sys.exit(0)

backup = path + '.bak'
print('Renaming old database to: %s' % backup)
os.rename(path, backup)

try:
  print('Renaming new database to: %s' % path)
  os.rename(tmpfile, path)
except (OSError):
  traceback.print_exc()
  print('\nOperation failed, restoring backup')
  os.rename(backup, path)
  sys.exit(1)

if options.nobackup:
  print("Unlinking backup: %s" % backup)
  os.unlink(backup)
