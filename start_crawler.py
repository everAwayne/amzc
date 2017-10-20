#!/usr/bin/env python

import os
import argparse
from util import daemon


CRAWLER_LS = ['flow_core', 'callback',
              'bsr', 'proxy_product', 'vps_product', 'bsr_result', 'review', 'qa']


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start optional crawler')
    parser.add_argument('-d', '--daemon', dest='daemon', action='store_true',
                        help='run as daemon')
    parser.add_argument('-c', '--crawler', dest='crawler', choices=CRAWLER_LS,
                        required=True, help='specify which crawler should be started')
    args = parser.parse_args()

    if args.daemon:
        daemon.daemonize(stderr='/tmp/'+args.crawler+'.log')

    if args.crawler == 'bsr':
        from amz_bsr_product import server
    elif args.crawler == 'bsr_result':
        from amz_bsr_result import server
    elif args.crawler == 'proxy_product':
        from amz_product import proxy_server as server
    elif args.crawler == 'vps_product':
        from amz_product import vps_server as server
    elif args.crawler == 'flow_core':
        from flow_core import server
    elif args.crawler == 'review':
        from amz_review import server
    elif args.crawler == 'qa':
        from amz_qa import server
    elif args.crawler == 'callback':
        from http_callback import server
    server.run()
