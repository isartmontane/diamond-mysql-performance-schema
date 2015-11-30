## diamond-mysql-performance-schema
Get metrics from MySQL/MariaDB Performance_Schema 
### Sends the following metrics from P_S to graphite
- Number of connections per Account (host-user) - Total and current
- Number of connections per User - Total and current
- Number of connections per Host - Total and current
- Number of rows read per index - schema, table, index name, rows read
- Queries that raised errors/warnings - Query, number of executions, errors, warnings
- Slow queries - Query, number of executions, execution time (total,max,avg), rows sent (total, avg), scanned rows
 
### Configuration
 - Edit the .conf file and set the user and password
 - Place the .py file under the collectors_path directory
