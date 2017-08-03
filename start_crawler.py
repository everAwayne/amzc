#!/usr/bin/env python

import os
import argparse
from util import daemon


CRAWLER_LS = ['product', 'bsr', 'bsr_product', 'asin_relationship']


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start optional crawler')
    parser.add_argument('-d', '--daemon', dest='daemon', action='store_true',
                        help='run as daemon')
    parser.add_argument('-c', '--crawler', dest='crawler', choices=CRAWLER_LS,
                        required=True, help='specify which crawler should be started')
    args = parser.parse_args()

    if args.daemon:
        daemon.daemonize(stderr='/tmp/'+args.crawler+'.log')

    if args.crawler == 'product':
        from amz_product import server
    elif args.crawler == 'bsr':
        from amz_bsr_product import server
    elif args.crawler == 'bsr_product':
        from amz_product import bsr_product_server as server
    elif args.crawler == 'asin_relationship':
        from amz_asin_relationship import server
    server.run()
