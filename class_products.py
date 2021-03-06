from collections import namedtuple
import datetime as dt

import numpy as np


ClassModel = namedtuple('ClassModel', ['start_day', 'end_day',
                                       'class_probs', 'class_vals'])

trans_class = 9


def sort_models(models):
    if len(models) == 1:
        return models

    idxs = np.argsort([m.start_day for m in models])

    return [models[i] for i in idxs]


def class_primary(models, ord_date):
    if ord_date <= 0:
        return 0

    prev_end = 0
    for m in models:
        if m.start_day <= ord_date <= m.end_day:
            return m.class_vals[np.argmax(m.class_probs[0])]
        elif prev_end < ord_date < m.start_day:
            return trans_class

        prev_end = m.end_day

    return 0


def class_secondary(models, ord_date):
    if ord_date <= 0:
        return 0

    prev_end = 0
    for m in models:
        if m.start_day <= ord_date <= m.end_day:
            return m.class_vals[np.argsort(m.class_probs[0])[-2]]
        elif prev_end < ord_date < m.start_day:
            return trans_class

        prev_end = m.end_day

    return 0


def segchange(models, ord_date):
    if ord_date <= 0:
        return 0

    ret = 0
    query_yr = dt.date.fromordinal(ord_date).year

    for idx, m in enumerate(models):
        yr = dt.date.fromordinal(m.end_day).year

        if yr == query_yr:
            class_val = m.class_vals[np.argmax(m.class_probs[0])]

            if idx + 1 < len(models):
                next_val = (models[idx + 1]
                            .class_vals[np.argmax(models[idx + 1]
                                                  .class_probs[0])])
            else:
                next_val = 0

            ret = int('{}{}'.format(class_val, next_val))

            break

    return ret


def conf_primary(models, ord_date):
    if ord_date <= 0:
        return 0

    prev_end = 0
    for m in models:
        if m.start_day <= ord_date <= m.end_day:
            return int(max(m.class_probs[0]) * 100)
        elif prev_end < ord_date < m.start_day:
            return 100

        prev_end = m.end_day

    return 0


def conf_secondary(models, ord_date):
    if ord_date <= 0:
        return 0

    prev_end = 0
    for m in models:
        if m.start_day <= ord_date <= m.end_day:
            return int(m.class_probs[0][np.argsort(m.class_probs[0])[-2]] * 100)
        elif prev_end < ord_date < m.start_day:
            return 100

        prev_end = m.end_day

    return 1
