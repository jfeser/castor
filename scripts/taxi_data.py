#!/usr/bin/env python3

import psycopg2
import datetime
import random

GRID_WIDTH = 100
AVG_TRACE_LEN = 30
TRACE_RES = datetime.timedelta(minutes=1)
TRACE_START = datetime.datetime.today()
NUM_TRACES = 100

DB_NAME = 'sam_analytics_small'

def taxi_trace(trace_id):
    pos = (random.randint(0, GRID_WIDTH-1),
           random.randint(0, GRID_WIDTH-1),
           TRACE_START)
    trace = [pos]
    for step in range(int(random.gauss(AVG_TRACE_LEN, 5))):
        xmove = random.randint(-1, 1)
        ymove = random.randint(-1, 1)
        pos = ((pos[0] + xmove) % GRID_WIDTH,
               (pos[1] + ymove) % GRID_WIDTH,
               pos[2] + TRACE_RES)
        trace += [pos]
    return [(trace_id,) + t for t in trace]

if __name__ == '__main__':
    conn = psycopg2.connect(dbname=DB_NAME)

    with conn.cursor() as c:
        c.execute('drop table if exists taxi')
        c.execute('create table taxi (id int, xpos int, ypos int, time timestamp)')
        for trace_id in range(NUM_TRACES):
            trace = taxi_trace(trace_id)
            for t in trace:
                c.execute('INSERT INTO taxi VALUES (%s, %s, %s, %s)', t)
    conn.commit()
