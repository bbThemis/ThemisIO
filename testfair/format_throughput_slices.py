#!/usr/bin/env python3

import time, sys

"""
Looks for lines in stdin in the form:
  rw_speed.<tag> time=2021-08-17:21:41:18 ... mbps_1sec_time_slices=(31.0,178.0,...)

Use the timestamp to figure out the start time of each sequence of time
slice entries, and output one column of data for each tag.
"""

def timestampToSeconds(timestamp):
  """
  Given a timestamp in the form '2021-08-17:21:41:18', return the number of
  seconds since epoch. Returns 0 on invalid input.
  """

  fmt = '%Y-%m-%d:%H:%M:%S'
  try:
    time_struct = time.strptime(timestamp, fmt)
  except ValueError:
    return 0

  return int(time.mktime(time_struct))


def parseLine(line):
  """
  Parse an input line, returning (tag, end_time, slices_array)
  """
  
  if not line.startswith('rw_speed.'):
    print('Bad input to parseLine: ' + line)
    return None

  words = line.split()
  tag = words[0][9:]
  start_time = None
  slices_array = None
  for word in words:

    # look for name=value pairs
    eq = word.find('=')
    if eq == -1: continue

    name = word[:eq]
    value = word[eq+1:]
    
    if name == 'time':
      start_time = timestampToSeconds(value)
      
    elif name == 'mbps_1sec_time_slices':
      slices_array = [float(x) for x in value[1:-1].split(',')]

  return (tag, start_time, slices_array)
  

def main(args):
  # job_tag -> (start_time, [t0, t1, ...])
  jobs = {}
  min_time = None
  max_time = None
  
  for line in sys.stdin:
    if line.startswith('rw_speed.'):
      job_name, end_time, slices = parseLine(line)
      start_time = end_time - len(slices) + 1
      jobs[job_name] = (start_time, slices)
      
      if min_time == None or start_time < min_time:
        min_time = start_time

      if max_time == None or end_time > max_time:
        max_time = end_time

  # print('min_start_time = ' + str(min_time))
  # for (tag, (start_time, slices_array)) in jobs.items():
  #   print(f'{tag} : {start_time}, {slices_array}[{len(slices_array)}]')

  tags = list(jobs.keys())
  tags.sort()

  header = ['time_in_seconds'] + [x+'.mbps' for x in tags]
  print('\t'.join(header))
  
  for t in range(min_time, max_time):
    line = [str(t - min_time)]
    # line = [str(t)]
    for tag in tags:
      start_time, slices = jobs[tag]
      slice_no = t - start_time
      if slice_no < 0 or slice_no >= len(slices):
        mbps = 0
      else:
        mbps = slices[slice_no]
      line.append(f'{mbps:.1f}')
    print('\t'.join(line))

    
if __name__ == '__main__':
  sys.exit(main(sys.argv[1:]))


