# coding=utf-8
#
# Diamond plugin to get metrics from MySQL/MariaDB Performance_Schema
# and send them to graphite
# 
# It will fetch the following metrics:
# - Number of connections per Account (host-user) - Total and current
# - Number of connections per User - Total and current
# - Number of connections per Host - Total and current
# - Number of rows read per index - schema, table, index name, rows read
# - Queries that raised errors/warnings - Query, number of executions, errors, warnings
# - Slow queries - Query, number of executions, execution time (total,max,avg), rows sent (total, avg), scanned rows
#
# To make it work copy the config file to the configuration directory and edit it with
#  the right user and password. Then copy the .py file on the collectors directory
# 
# Author: Isart Montane Mogas <isartmontane@gmail.com>
# License: GPL v3

"""

#### Grants
grant select on performance_schema.* to graphite@localhost identified by 'graphite';

#### Dependencies

 * MySQLdb

"""

import diamond.collector
from diamond.collector import str_to_bool
import re
import time
import MySQLdb
from MySQLdb import MySQLError

class MysqlPSCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(MysqlPSCollector, self).get_default_config_help()
        config_help.update({
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(MysqlPSCollector, self).get_default_config()
        config.update({
            'host':'localhost',
            'db':'performance_schema',
            'port':3306
        })
        return config

    # Connect to MySQL database
    def connect(self):
        try:
            self.log.debug('MySQLPSCollector: Connecting to database.')
            self.db = MySQLdb.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                passwd=self.config['password']
            )
            return True
        except MySQLError, e:
            self.log.error('MySQLPSCollector couldnt connect to database %s', e)
            return False

    def mysql_query(self,query):
        conn = self.db
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(query)
        return cur

    # Close MySQL connection
    def disconnect(self):
        self.db.close()

    # Check if PENFORMANCE_SCHEMA is enabled
    def is_ps_enabled(self):
        result = self.mysql_query('SHOW GLOBAL VARIABLES LIKE "performance_schema"')
        row = result.fetchone()
        if row['Value'] == 'ON':
            return True
        else:
        	  return False
    	
    def clean_string(self,digest):
        clean_digest=digest
        clean_digest=re.sub(r'[^\x00-\x7F]+','_', clean_digest)
       	clean_digest=clean_digest.replace('`', '')
       	clean_digest=clean_digest.replace('?', '')
       	clean_digest=clean_digest.replace(' ', '_')
       	clean_digest=clean_digest.replace(',', '_')
       	clean_digest=clean_digest.replace('(', '_')
       	clean_digest=clean_digest.replace(')', '_')
       	clean_digest=clean_digest.replace('.', '_')
        clean_digest=re.sub(r'(__)', '',clean_digest)
       	clean_digest=re.sub('_$', '',clean_digest)
       	return clean_digest	
    
    # Connections per account
    def fetch_connections_per_account(self):
        queries = {}
       	try:
            result = self.mysql_query("""
                SELECT user,host,current_connections,total_connections
                FROM performance_schema.accounts
                LIMIT {} 
                """.format(self.config['max_rows']))
            for row in result.fetchall():
                # Clean the digest string 
                clean_digest=str(row['user'])+'_'+str(row['host'])
                queries["current_connections_per_host_"+clean_digest] = row['current_connections'] 
                queries["total_connections_per_account_"+clean_digest] = row['total_connections'] 
       
       	except MySQLdb.OperationalError:
       		  return {}
       
       	return queries
    
    # Connections per host
    def fetch_connections_per_host(self):
        queries = {}
      	try:
            result = self.mysql_query("""
      			  	SELECT host,current_connections,total_connections
      				  FROM performance_schema.hosts
      				  LIMIT {} 
                """.format(self.config['max_rows']))
            for row in result.fetchall():
                # Clean the digest string 
                clean_digest=str(row['host'])
                queries["current_connections_per_host_"+clean_digest] = row['current_connections'] 
                queries["total_connections_per_host_"+clean_digest] = row['total_connections'] 
      
      	except MySQLdb.OperationalError:
      		    return {}
      
      	return queries
    
    # Connections per user
    def fetch_connections_per_user(self):
        queries = {}
       	try:
       	    result = self.mysql_query("""
       	   			SELECT user,current_connections,total_connections
       	   			FROM performance_schema.users
       	   			LIMIT {} 
                """.format(self.config['max_rows']))
            for row in result.fetchall():
                # Clean the digest string 
                clean_digest=str(row['user'])
                queries["current_connections_per_user_"+clean_digest] = row['current_connections'] 
                queries["total_connections_per_user_"+clean_digest] = row['total_connections'] 
       
       	except MySQLdb.OperationalError:
       	    return {}
       
       	return queries
    
    # number of reads per index
    def fetch_number_of_reads_per_index(self):
        queries = {}
       	try:
       	    result = self.mysql_query("""
       	   			SELECT 
       	   				object_schema, 
       	   				object_name, 
       	   				index_name, 
       	   				count_read AS rows_read 
       	   			FROM performance_schema.table_io_waits_summary_by_index_usage 
       	   			WHERE index_name IS NOT NULL 
       	   				AND count_read>0
       	   			ORDER BY sum_timer_wait DESC
       	   			LIMIT {} 
                """.format(self.config['max_rows']))
            for row in result.fetchall():
                # Clean the digest string 
                clean_digest=self.clean_string(row['object_schema']+'_'+row['object_name']+'_'+row['index_name'])
                queries["number_of_reads_per_index_"+clean_digest] = row['rows_read'] 
       
       	except MySQLdb.OperationalError:
       		return {}
       
       	return queries
    
    # Queries that raised errors
    def fetch_warning_error_queries(self):
        queries = {}
       	try:
       	    # Get error queries
            result = self.mysql_query("""
            SELECT DIGEST_TEXT AS query,
            COUNT_STAR AS exec_count,
            SUM_ERRORS AS errors,
            SUM_WARNINGS AS warnings
            FROM performance_schema.events_statements_summary_by_digest
            WHERE SUM_ERRORS > 0
            OR SUM_WARNINGS > 0
            ORDER BY SUM_ERRORS DESC, SUM_WARNINGS DESC
            LIMIT {} 
            """.format(self.config['max_rows']))

            for row in result.fetchall():
       	   	    # Clean the digest string
                clean_digest=self.clean_string(row['query'])
                queries["exec_count_"+clean_digest] = row['exec_count']
                queries["errors_"+clean_digest] = row['errors']
                queries["warnings_"+clean_digest] = row['warnings']
       
       	except MySQLdb.OperationalError:
       	    return {}
       
       	return queries
    
    # Slow queries, response time, rows scanned, rows returned 
    def fetch_slow_queries(self):
        slow_queries = {}
       	try:
            # Get the slow queries
            result = self.mysql_query("""
            SELECT DIGEST_TEXT AS query,
            COUNT_STAR AS exec_count,
            round(SUM_TIMER_WAIT/1000000000) AS exec_time_total_ms,
            round(MAX_TIMER_WAIT/1000000000) AS exec_time_max_ms,
            round(AVG_TIMER_WAIT/1000000000) AS exec_time_avg_ms,
            SUM_ROWS_SENT AS rows_sent_sum,
            ROUND(SUM_ROWS_SENT / COUNT_STAR) AS rows_sent_avg,
            SUM_ROWS_EXAMINED AS rows_scanned
            FROM performance_schema.events_statements_summary_by_digest
            ORDER BY SUM_TIMER_WAIT DESC 
            LIMIT {}
            """.format(self.config['max_rows']))


            for row in result.fetchall():
                # Clean the digest string
                clean_digest=self.clean_string(row['query'])
                slow_queries["exec_count_"+clean_digest] = row['exec_count']
                slow_queries["exec_time_total_"+clean_digest] = row['exec_time_total_ms']
                slow_queries["exec_time_max_"+clean_digest] = row['exec_time_max_ms']
                slow_queries["exec_time_avg_ms_"+clean_digest] = row['exec_time_avg_ms']
                slow_queries["rows_sent_sum_"+clean_digest] = row['rows_sent_sum']
                slow_queries["rows_sent_avg_"+clean_digest] = row['rows_sent_avg']
                slow_queries["rows_scanned_"+clean_digest] = row['rows_scanned']
       
       	except MySQLdb.OperationalError:
       	    return {}
       
       	return slow_queries

    def collect(self):
        self.connect()
        # Performance_Schema metrics
        if self.is_ps_enabled():
            queries = self.fetch_slow_queries()
            for key in queries:
            	self.publish('mysql.performance_schema.slow_query.'+key, queries[key])
            
            queries = self.fetch_warning_error_queries()
            for key in queries:
            	self.publish('mysql.performance_schema.warn_err_query.'+key, queries[key])
            
            queries = self.fetch_number_of_reads_per_index()
            for key in queries:
            	self.publish('mysql.performance_schema.numbers_of_reads_per_index.'+key, queries[key])
            
            queries = self.fetch_connections_per_user()
            for key in queries:
            	self.publish('mysql.performance_schema.connections_per_user.'+key, queries[key])
            
            queries = self.fetch_connections_per_host()
            for key in queries:
            	self.publish('mysql.performance_schema.connections_per_host.'+key, queries[key])
            
            queries = self.fetch_connections_per_account()
            for key in queries:
            	self.publish('mysql.performance_schema.connections_per_account.'+key, queries[key])
