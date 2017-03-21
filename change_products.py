from collections import namedtuple
import datetime as dt

import numpy as np


ChangeModel = namedtuple('ChangeModel', ['start_day', 'end_day', 'break_day',
                                         'qa', 'magnitudes'])


beginning_of_time = dt.date(year=1982, month=1, day=1)


def changedate_val(models, ord_date):
    query_date = dt.date.fromordinal(ord_date)

    ret = 0
    for m in models:
        break_date = dt.date.fromordinal(m.break_day)

        if query_date.year == break_date.year:
            ret = break_date.timetuple().tm_yday
            break

    return ret


def changemag_val(models, ord_date):
    query_date = dt.date.fromordinal(ord_date)

    ret = 0
    for m in models:
        break_date = dt.date.fromordinal(m.break_day)

        if query_date.year == break_date.year:
            ret = np.linalg.norm(m.magnitudes[1:-1])
            break

    return ret


def qa_val(models, ord_date):
    query_date = dt.date.fromordinal(ord_date)

    ret = 0
    for m in models:
        start_date = dt.date.fromordinal(m.start_day)
        end_date = dt.date.fromordinal(m.end_day)

        if start_date < query_date < end_date:
            ret = m.qa
            break

    return ret


def seglength_val(models, ord_date, bot=beginning_of_time):
    query_date = dt.date.fromordinal(ord_date)

    all_dates = [bot]
    for m in models:
        start_date = dt.date.fromordinal(m.start_day)
        end_date = dt.date.fromordinal(m.end_day)

        all_dates.append(start_date)
        all_dates.append(end_date)

    diff = [(query_date - d).days for d in all_dates]

    if not any((i < 0 for i in diff)):
        return 0

    return min(i for i in diff if i > 0)


def lastchange_val(models, ord_date):
    query_date = dt.date.fromordinal(ord_date)

    break_dates = []
    for m in models:
        break_dates.append(dt.date.fromordinal(m.break_day))

    diff = [(query_date - d).days for d in break_dates]

    if not any((i < 0 for i in diff)):
        return 0

    return min(i for i in diff if i > 0)
