import argparse
import sys
import sqlite3
from typing import Tuple, List
import functools
import time

import numpy


def parse():
    parser = argparse.ArgumentParser(description='Output movies whose scores exceed a given threshold')
    parser.add_argument('-d','--db', help='Source DB generated by process_rt', required=True)
    parser.add_argument('-c','--critics', help='Consider critic scores', action='store_true', default=False)
    parser.add_argument('-a','--audience', help='Consider audience scores', action='store_true', default=False)
    parser.add_argument('-t','--threshold', help='Numeric threshold', required=True, type=float)
    parser.add_argument('-D','--delta-func', help='Delta function (slope or minmax)', default="slope")

    args = parser.parse_args()

    if args.critics is False and args.audience is False:
        print("Must specify at least one of: --audience, --critics")
        sys.exit(1)
    return args


def get_critic_point(score: str) -> Tuple[float, float]:
    score_ary = score.split(';')
    return float(score_ary[0]), float(score_ary[1])


def get_audience_point(score: str) -> Tuple[float, float]:
    score_ary = score.split(';')
    return float(score_ary[0]), float(score_ary[2])


def add_point(points: Tuple[List[float], List[float]], point: Tuple[float, float]) -> Tuple[List[float], List[float]]:
    points[0].append(point[0])
    points[1].append(point[1])
    return points


def linear_regression_2d(points: Tuple[List[float], List[float]]) -> Tuple[float, float]:
    x = numpy.array(points[0])
    y = numpy.array(points[1])
    A = numpy.vstack([x, numpy.ones(len(x))]).T
    slope, y_int = numpy.linalg.lstsq(A, y, rcond=None)[0]

    return slope, y_int


def formatted_ts(points: Tuple[List[float], List[float]]) -> List[Tuple[str, float]]:
    formatted_ts = map(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x)), points[0])

    return list(zip(formatted_ts, points[1]))


def main():
    args = parse()

    query = '''
    select group_concat(ts || ';' || critic || ';' || numCritic || ';' || audience  || ';' || numAudience, ','), 
           name 
           from rt 
           group by name order by name, ts asc
    '''

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()
    for row in cur.execute(query):
        scores=row[0].split(',')
        scores=sorted(scores, key=lambda x: float(x.split(';')[0]))
        name=row[1]

        if args.critics:
            critic_ts = functools.reduce(lambda points, score: add_point(points, get_critic_point(score)), scores, ([],[]))
            slope, y_int = linear_regression_2d(critic_ts)
            value = max(critic_ts[1]) - min(critic_ts[1])
            if args.delta_func == 'slope':
                if numpy.abs(slope) > args.threshold:
                    print(f'{name} (critics): minmax={value} slope={slope} ts={formatted_ts(critic_ts)}')
            elif args.delta_func == 'minmax':
                if value > args.threshold:
                    print(f'{name} (critics): minmax={value} slope={slope} ts={formatted_ts(critic_ts)}')

        if args.audience:
            audience_ts = functools.reduce(lambda points, score: add_point(points, get_audience_point(score)), scores, ([],[]))
            slope, y_int = linear_regression_2d(audience_ts)
            value = max(audience_ts[1]) - min(audience_ts[1])
            if args.delta_func == 'slope':
                if numpy.abs(slope) > args.threshold:
                    print(f'{name} (audience): minmax={value} slope={slope} ts={formatted_ts(audience_ts)}')
            elif args.delta_func == 'minmax':
                if value > args.threshold:
                    print(f'{name} (audience): minmax={value} slope={slope} ts={formatted_ts(audience_ts)}')


if __name__ == '__main__':
    main()
