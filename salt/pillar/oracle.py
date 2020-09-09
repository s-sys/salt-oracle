# -*- coding: utf-8 -*-
'''
Retrieve Pillar data by doing an Oracle query

This module is a concrete implementation of the sql_base ext_pillar for Oracle
Database.

:maintainer: Lucas Sanches (lucas.sanches@ssys.com.br)
:maturity: new
:depends: cx-Oracle
:platform: all

Configuring the oracle ext_pillar
=================================

Use the 'oracle' key under ext_pillar for configuration of queries.

Oracle configuration of the Oracle returner is being used (oracle.sid,
oracle.host, oracle.port, oracle.user, oracle.pass) for database connection
info.

Required python modules: cx-Oracle

Complete example
================

.. code-block:: yaml

    oracle:
      user: 'salt'
      pass: 'super_secret_password'
      host: 'localhost'
      port: 1521
      sid: 'XE'

    ext_pillar:
      - oracle:
          fromdb:
            query: 'SELECT col1,col2,col3,col4,col5,col6,col7
                    FROM some_random_table
                    WHERE minion_pattern LIKE %s'
            depth: 5
            as_list: True
            with_lists: [1,3]
'''
from __future__ import absolute_import, print_function, unicode_literals

# Import python libs
from contextlib import contextmanager
import logging

# Import Salt libs
from salt.pillar.sql_base import SqlBaseExtPillar

# Set up logging
log = logging.getLogger(__name__)

# Import third party libs
try:
    # Trying to import cx-Oracle
    import cx_Oracle
except ImportError:
    cx_Oracle = None


def __virtual__():
    '''
    Confirm that a python oracle client is installed.
    '''
    return bool(cx_Oracle), 'No python oracle client installed.' if cx_Oracle is None else ''


class OracleExtPillar(SqlBaseExtPillar):
    '''
    This class receives and processes the database rows from Oracle.
    '''
    @classmethod
    def _db_name(cls):
        return 'Oracle'

    def _get_options(self):  # pylint: disable=no-self-use
        '''
        Returns options used for the Oracle connection.
        '''
        defaults = {'user': 'salt',
                    'pass': 'salt',
                    'host': 'localhost',
                    'port': 1521,
                    'sid': 'XE',
                    'service': False}
        _options = {}
        _opts = __opts__.get('oracle', {})
        for attr in defaults:
            if attr not in _opts:
                log.debug('Using default for Oracle %s', attr)
                _options[attr] = defaults[attr]
                continue
            _options[attr] = _opts[attr]
        return _options

    @contextmanager
    def _get_cursor(self):
        '''
        Yield an Oracle cursor
        '''
        _options = self._get_options()
        if _options['service']:
            dsn = cx_Oracle.makedsn(_options['host'], _options['port'], service_name=_options['service'])
        else:
            dsn = cx_Oracle.makedsn(_options['host'], _options['port'], sid=_options['sid'])
        conn = cx_Oracle.connect(_options['user'], _options['pass'], dsn)
        cursor = conn.cursor()
        try:
            yield cursor
        except cx_Oracle.DatabaseError as err:
            log.exception('Error in ext_pillar Oracle: %s', err.args)
        finally:
            conn.close()


def ext_pillar(minion_id, pillar, *args, **kwargs):
    '''
    Execute queries against Oracle, merge and return as a dict
    '''
    return OracleExtPillar().fetch(minion_id, pillar, *args, **kwargs)
