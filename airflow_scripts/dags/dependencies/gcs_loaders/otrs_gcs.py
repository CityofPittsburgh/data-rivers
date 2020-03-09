import os
import MySQLdb


OTRScon = MySQLdb.connect(host='10.1.0.97', user=os.environ['OTRS_USER'], passwd=os.environ['OTRS_PW'], db='otrs')
cursor_master = OTRScon.cursor()
