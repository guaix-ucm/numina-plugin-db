
from __future__ import print_function

import os
import logging
import datetime

from numinadb.dal import Session
from numinadb.model import MyOb, Frame, Base, ObFact
from sqlalchemy import create_engine

from megaradrp.simulation.actions import megara_sequences

_logger = logging.getLogger("simulation.controldb")


class ControlSystem(object):
    """Top level"""
    def __init__(self, factory):
        self._elements = {}
        from megaradrp.simulation.factory import PersistentRunCounter
        self.imagecount = PersistentRunCounter('r00%04d.fits')
        self.mode = 'null'
        self.ins = 'MEGARA'
        self.seqs = megara_sequences()
        self.dbname = 'processing.db'
        uri = 'sqlite:///%s' % self.dbname
        engine = create_engine(uri, echo=False)
        self.conn = None
        self.datadir = 'data'
        Session.configure(bind=engine)
        self.factory = factory
        # FIXME: remove this
        self.register('factory', factory)

    def register(self, name, element):
        self._elements[name] = element

    def get(self, name):
        return self._elements[name]

    def set_mode(self, mode):
        self.mode = mode

    def run(self, exposure, repeat=1):

        if repeat < 1:
            return

        _logger.info('mode is %s', self.mode)
        try:
            thiss = self.seqs[self.mode]
        except KeyError:
            _logger.error('No sequence for mode %s', self.mode)
            raise

        session = Session()
        now = datetime.datetime.now()
        ob = MyOb(instrument=self.ins, mode=self.mode, start_time=now)
        session.add(ob)

        iterf = thiss.run(self, exposure, repeat)
        count = 1
        for final in iterf:
            _logger.info('image %d of %d', count, repeat)
            name = self.imagecount.runstring()
            fitsfile = self.factory.create(final, name, self)
            _logger.info('save image %s', name)
            fitsfile.writeto(os.path.join(self.datadir, name), clobber=True)
            # Insert into DB
            newframe = Frame()
            newframe.name = name
            ob.frames.append(newframe)
            count += 1


        # Update completion time of the OB when its finished
        ob.completion_time = datetime.datetime.now()
        # Facts

        from numina.core.pipeline import DrpSystem

        drps = DrpSystem()

        this_drp = drps.query_by_name('MEGARA')

        tagger = None
        for mode in this_drp.modes:
            if mode.key == self.mode:
                tagger = mode.tagger
                break

        if tagger:
            current = os.getcwd()
            os.chdir(self.datadir)
            master_tags = tagger(ob)
            os.chdir(current)
            for k in master_tags:
                fact = ObFact(key=k, value=master_tags[k])
                ob.facts.append(fact)

        session.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.imagecount.__exit__(exc_type, exc_val, exc_tb)

    def initdb(self):

        uri = 'sqlite:///%s' % self.dbname
        engine = create_engine(uri, echo=False)

        Base.metadata.create_all(engine)