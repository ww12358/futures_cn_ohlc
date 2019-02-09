#!/usr/bin/python
# encoding: utf-8

"""
Utility to convert strings to start and end datetime tuples

The MIT License (MIT)

Copyright (c) 2013 Marian Steinbach

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import datetime
from calendar import monthrange


def to_dates(param):
    """
    This function takes a date string in various formats
    and converts it to a normalized and validated date range. A list
    with two elements is returned, lower and upper date boundary.

    Valid inputs are, for example:
    2012              => Jan 1 20012 - Dec 31 2012 (whole year)
    201201            => Jan 1 2012  - Jan 31 2012 (whole month)
    2012101           => Jan 1 2012 - Jan 1 2012   (whole day)
    2011-2011         => same as "2011", which means whole year 2012
    2011-2012         => Jan 1 2011 - Dec 31 2012  (two years)
    201104-2012       => Apr 1 2011 - Dec 31 2012
    201104-201203     => Apr 1 2011 - March 31 2012
    20110408-2011     => Apr 8 2011 - Dec 31 2011
    20110408-201105   => Apr 8 2011 - May 31 2011
    20110408-20110507 => Apr 8 2011 - May 07 2011
    2011-             => Jan 1 2012 - Dec 31 9999 (unlimited)
    201104-           => Apr 1 2011 - Dec 31 9999 (unlimited)
    20110408-         => Apr 8 2011 - Dec 31 9999 (unlimited)
    -2011             Jan 1 0000 - Dez 31 2011
    -201104           Jan 1 0000 - Apr 30, 2011
    -20110408         Jan 1 0000 - Apr 8, 2011
    """
    pos = param.find('-')
    lower, upper = (None, None)
    if pos == -1:
        # no seperator given
        lower, upper = (param, param)
    else:
        lower, upper = param.split('-')
    ret = (expand_date_param(lower, 'lower'), expand_date_param(upper, 'upper'))
    return ret


def expand_date_param(param, lower_upper):
    """
    Expands a (possibly) incomplete date string to either the lowest
    or highest possible contained date and returns
    datetime.datetime for that string.

    0753 (lower) => 0753-01-01
    2012 (upper) => 2012-12-31
    2012 (lower) => 2012-01-01
    201208 (upper) => 2012-08-31
    etc.
    """
    year = datetime.MINYEAR
    month = 1
    day = 1
    hour = 0
    minute = 0
    second = 0
    if lower_upper == 'upper':
        year = datetime.MAXYEAR
        month = 12
        day = 31
        hour = 23
        minute = 59
        second = 59
    if len(param) == 0:
        # leave defaults
        pass
    elif len(param) == 4:
        year = int(param)
        if lower_upper == 'lower':
            month = 1
            day = 1
            hour = 0
            minute = 0
            second = 0
        else:
            month = 12
            day = 31
            hour = 23
            minute = 59
            second = 59
    elif len(param) == 6:
        year = int(param[0:4])
        month = int(param[4:6])
        if lower_upper == 'lower':
            day = 1
        else:
            (firstday, dayspermonth) = monthrange(year, month)
            day = dayspermonth
    elif len(param) == 8:
        year = int(param[0:4])
        month = int(param[4:6])
        day = int(param[6:8])
    elif len(param) == 10:
        year = int(param[0:4])
        month = int(param[4:6])
        day = int(param[6:8])
        hour = int(param[8:10])
    elif len(param) == 12:
        year = int(param[0:4])
        month = int(param[4:6])
        day = int(param[6:8])
        hour = int(param[8:10])
        minute = int(param[10:12])
    elif len(param) == 14:
        year = int(param[0:4])
        month = int(param[4:6])
        day = int(param[6:8])
        hour = int(param[8:10])
        minute = int(param[10:12])
        second = int(param[12:14])
    else:
        # wrong input length
        raise ValueError('Bad date string provided. Use YYYY, YYYYMM or YYYYMMDD.')
    # force numbers into valid ranges
    #print (param, lower_upper), [year, month, day, hour, minute, second]
    year = min(datetime.MAXYEAR, max(datetime.MINYEAR, year))
    return datetime.datetime(year=year, month=month, day=day,
        hour=hour, minute=minute, second=second)
