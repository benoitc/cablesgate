#!/usr/bin/env python
# -*- coding: utf-8 -
# Copyright (c) 2010 Benoît Chesneau <benoitc@e-engura.org>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
# 
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.


import copy
import logging
import os
import re
import sys

from BeautifulSoup import BeautifulSoup
from couchdbkit import Database
import nltk

fmt = r"%(asctime)s [%(levelname)s] %(message)s"
datefmt = r"%Y-%m-%d %H:%M:%S"

logging.basicConfig(level=logging.DEBUG, format=fmt, datefmt=datefmt)
log = logging.getLogger("cablegate")


class Extractor(object):

    def __init__(self, cables_path):
        self.cables_path = cables_path
        self.processed = 0

    def __iter__(self):
        return self.process()

    def process(self):
        for root, dirs, files in os.walk(self.cables_path):
            for fname in files:
                if fname.endswith(".html"):
                    self.processed += 1
                    cnt = self.parse(os.path.join(root, fname))
                    if cnt is not None:
                        yield cnt
                    else:
                        log.info("%s not processed" % fname)
                        self.processed -= 1

    def parse(self, fname):
        try:
            with open(fname, "r") as f:
                log.info("Process %s" % fname)
                soup = BeautifulSoup(f.read())
                tbl = soup.find("table", { "class" : "cable" })
                docid = tbl.findAll('tr')[1].\
                        findAll('td')[0].contents[1].contents[0]

                doc = {
                        "_id": docid,
                        "refererence_id": docid,
                        "date_time": tbl.findAll('tr')[1].\
                                findAll('td')[1].contents[1].contents[0],
                        "classification": tbl.findAll('tr')[1].\
                                findAll('td')[2].contents[1].contents[0],
                        "origin": tbl.findAll('tr')[1].\
                                findAll('td')[3].contents[1].contents[0],
                        "header":nltk.clean_html(str(soup.findAll(['pre'])[0])),
                        "body": nltk.clean_html(str(soup.findAll(['pre'])[1]))
                }
                
                return doc

        except OSError:
            log.error("Can't open '%s'" % fname)
            self.processed -= 1

def save_docs(db, docs):
    db.save_docs(docs)

def send(pool, db, docs):
    
    pool.spawn_n(save_docs, db, copy.copy(docs))
    eventlet.sleep(1)


def main(dburi, cables_path):
    db = Database(dburi, create=True)

    log.info("Start processing")

    extractor = Extractor(cables_path)
    
    docs = []
    for doc in extractor:
        if len(docs) == 100:
            log.info("Sending to CouchDB")
            db.save_docs(docs)
            docs = []
        docs.append(doc)

    if docs:
        log.info("Sending to CouchDB")
        db.save_docs(docs)
    log.info("%s cables processed." % extractor.processed)

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) < 2:
        print >>sys.stderr, "command is: convert.py dburi cables_path"
        sys.exit(1)
    
    dburi = args[0]
    cables_path = os.path.normpath(os.path.join(os.getcwd(), args[1]))
    main(dburi, cables_path)


