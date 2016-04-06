
from __future__ import print_function

import os
import logging
import datetime

import megaradrp.simulation.control as basecontrol

from numinadb.dal import Session
from numinadb.model import MyOb, Frame, Base, Fact
from sqlalchemy import create_engine


_logger = logging.getLogger("simulation.controldb")


class ControlSystem(basecontrol.ControlSystem):
    """Top level"""
    def __init__(self, factory):
        super(ControlSystem, self).__init__(factory)

        dbname = 'processing.db'
        self.uri = 'sqlite:///%s' % dbname
        engine = create_engine(self.uri, echo=False)

        self.datadir = 'data'
        Session.configure(bind=engine)

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
        session.commit() # So that we have ob.id
        iterf = thiss.run(self, exposure, repeat)
        self.ob_data['repeat'] = repeat
        self.ob_data['name'] = None
        self.ob_data['obsid'] = ob.id
        for count, final in enumerate(iterf, 1):
            _logger.info('image %d of %d', count, repeat)
            self.ob_data['count'] = count
            self.ob_data['name'] = self.imagecount.runstring()
            fitsfile = self.factory.create(final, self.ob_data['name'], self)
            _logger.info('save image %s', self.ob_data['name'])
            fitsfile.writeto(os.path.join(self.datadir, self.ob_data['name']), clobber=True)
            # Insert into DB
            newframe = Frame()
            newframe.name = self.ob_data['name']
            ob.frames.append(newframe)

        # Update completion time of the OB when its finished
        ob.completion_time = datetime.datetime.now()

        # Facts
        self.add_facts(session, ob)

        session.commit()

    def add_facts(self, session, ob):
        from numina.core.pipeline import DrpSystem

        drps = DrpSystem()

        this_drp = drps.query_by_name(self.ins)

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

            for k, v in master_tags.items():
                fact = session.query(Fact).filter_by(key=k, value=v).first()
                if fact is None:
                    fact = Fact(key=k, value=v)
                ob.facts.append(fact)

    def initdb(self):

        engine = create_engine(self.uri, echo=False)

        Base.metadata.create_all(engine)