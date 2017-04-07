from collections import namedtuple
import datetime as dt

import numpy as np


ChangeModel = namedtuple('ChangeModel', ['start_day', 'end_day', 'break_day',
                                         'qa', 'magnitudes', 'change_prob'])


beginning_of_time = dt.date(year=1982, month=1, day=1).toordinal()


def changedate_val(models, ord_date):
    if ord_date <= 0:
        return 0

    query_date = dt.date.fromordinal(ord_date)

    ret = 0
    for m in models:
        if m.break_day <= 0:
            continue

        break_date = dt.date.fromordinal(m.break_day)

        if query_date.year == break_date.year:
            ret = break_date.timetuple().tm_yday
            break

    return ret


def changemag_val(models, ord_date):
    if ord_date <= 0:
        return 0

    query_date = dt.date.fromordinal(ord_date)

    ret = 0
    for m in models:
        if m.break_day <= 0:
            continue

        break_date = dt.date.fromordinal(m.break_day)

        if query_date.year == break_date.year:
            ret = np.linalg.norm(m.magnitudes[1:-1])
            break

    return ret


def qa_val(models, ord_date):
    if ord_date <= 0:
        return 0

    ret = 0
    for m in models:

        if m.start_day <= ord_date <= m.end_day:
            ret = m.qa
            break

    return ret


def seglength_val(models, ord_date, bot=beginning_of_time):
    if ord_date <= 0:
        return 0

    all_dates = [bot]
    for m in models:
        all_dates.append(m.start_day)
        all_dates.append(m.end_day)

    diff = [(ord_date - d) for d in all_dates if (ord_date - d) > 0]

    if not diff:
        return 0

    return min(diff)


def lastchange_val(models, ord_date):
    if ord_date <= 0:
        return 0

    break_dates = []
    for m in models:
        if m.change_prob == 1:
            break_dates.append(m.break_day)
        else:
            break_dates.append(ord_date)

    diff = [(ord_date - d) for d in break_dates if (ord_date - d) > 0]

    if not diff:
        return 0

    return min(diff)
