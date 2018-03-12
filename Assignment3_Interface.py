#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
#Assignment3 Interface
#Author - Saiteja Sirikonda
#ASU_id - 1211246826

import psycopg2
import os
import sys
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
'''
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
'''
FIRST_TABLE_NAME = ‘MovieRating’
SECOND_TABLE_NAME = ‘MovieBoxOfficeCollection’
SORT_COLUMN_NAME_FIRST_TABLE = ‘Rating’
SORT_COLUMN_NAME_SECOND_TABLE = ‘Collection’
JOIN_COLUMN_NAME_FIRST_TABLE = ‘MovieID’
JOIN_COLUMN_NAME_SECOND_TABLE = ‘MovieID’
##########################################################################################################


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    #pass #Remove this once you are done with implementation
    cur = openconnection.cursor()
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur.execute("SELECT * from {}". format(InputTable))
    rows = cur.fetchall()
    #Getting the column index of SortingC
    #Creating Range partition tables  
    for i in range(5):
        #to create partition tables. How will I know the schema?
        cur.execute('CREATE TABLE IF NOT EXISTS {} AS TABLE {}'.format(InputTablePart + str(i),InputTable))
        cur.execute('DELETE FROM {}'.format(InputTablePart + str(i)))
    cur.execute('SELECT max({}) FROM {}'.format(SortingColumnName,InputTable))
    max_value  = cur.fetchone()[0]
    cur.execute('SELECT min({}) FROM {}'.format(SortingColumnName,InputTable))
    min_value  = cur.fetchone()[0]
    part_size = (max_value - min_value)/5 #You have the size of each partition
    #Creating list of boundaries
    value = 0
    parti = []
    parti.append(min_value)
    for i in range(1,5):
        value = min_value + part_size + value
        parti.append(value)
    parti.append(max_value)
    threads=[]    
    for i in range(5):
        t = threading.Thread(target = ThreadsWorking1,args = (InputTable,InputTablePart+str(i),SortingColumnName,parti[i],parti[i+1],openconnection,))
        threads.append(t)
        t.start()

    cur.execute('CREATE TABLE IF NOT EXISTS {} AS TABLE {}'.format(OutputTable))
    cur.execute('DELETE FROM {}'.format(OutputTable))
    for i in range(5):
        cur.execute('INSERT INTO {} SELECT * from {}'.format(OutputTable,InputTablePart + str(i)))

    cur.close()

    #Inserting values into those partitions
    #Starting sorting using threads
    #Wait for the all threads to get over
    #Write the sorted values into a table


def ThreadsWorking1 (maintable,partiton_table,ColumnName,mini,maxi,openconnection):
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()    
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur.execute('INSERT INTO {} SELECT * from {} where {}>{} AND {}<={} ORDER BY {}'.format(partiton_table,maintable,ColumnName,mini,ColumnName,maxi,ColumnName))

    cur.close()


def ThreadWorking2 (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
)    cur = openconnection.cursor()
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur.execute('CREATE TABLE IF NOT EXISTS {} AS SELECT * {} INNER JOIN {} ON ({}.{} = {}.{})'.format(OutputTable,InputTable1,InputTable2,InputTable1,Table1JoinColumn,InputTable2,Table2JoinColumn))

    cur.close()




def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    #First, partiton both the tables
    #Then, set the threads, write at the same time and wait till they get over
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()
    for i in range(5):
        #to create partition tables. How will I know the schema?
        cur.execute('CREATE TABLE IF NOT EXISTS {} AS TABLE {}'.format(InputTablePart1 + str(i),InputTable1))
        cur.execute('DELETE FROM {}'.format(InputTablePart1 + str(i)))
    for i in range(5):
        #to create partition tables. How will I know the schema?
        cur.execute('CREATE TABLE IF NOT EXISTS {} AS TABLE {}'.format(InputTablePart2 + str(i),InputTable2))
        cur.execute('DELETE FROM {}'.format(InputTablePart2 + str(i)))
    cur.execute('SELECT max({}) FROM {}'.format(SortingColumnName,InputTable1))
    max_value1  = cur.fetchone()[0]
    cur.execute('SELECT min({}) FROM {}'.format(SortingColumnName,InputTable1))
    min_value1  = cur.fetchone()[0]
    part_size1 = (max_value1 - min_value1)/5 #You have the size of each partition
    #Creating list of boundaries
    value = 0
    parti1 = []
    parti1.append(min_value1)
    for i in range(1,5):
        value = min_value1 + part_size1 + value
        parti1.append(value)
    parti1.append(max_value1)
    cur.execute('SELECT max({}) FROM {}'.format(SortingColumnName,InputTable2))
    max_value2  = cur.fetchone()[0]
    cur.execute('SELECT min({}) FROM {}'.format(SortingColumnName,InputTable2))
    min_value2  = cur.fetchone()[0]
    part_size2 = (max_value2 - min_value2)/5 #You have the size of each partition
    #Creating list of boundaries
    value = 0
    parti2 = []
    parti2.append(min_value2)
    for i in range(1,5):
        value = min_value2 + part_size2 + value
        parti2.append(value)
    parti2.append(max_value2)
    #for i in range(5):
    #   cur.execute('CREATE TABLE {} AS SELECT * {} INNER JOIN {} ON ({}.{} = {}.{}'.format(OutputTable+str(i),InputTable1,InputTable2,InputTable1,Table1JoinColumn,InputTable2,Table2JoinColumn))
    #  cur.execute('DELETE FROM {}'.format(OutputTable+str(i)))
    for i in range(5):
        cur.execute('INSERT INTO {} SELECT * from {} where {}>{} AND {}<={}'.format(InputTablePart1+str(i),InputTablePart1,Table1JoinColumn,parti1[i],Table1JoinColumn,parti1[i+1]))
        cur.execute('INSERT INTO {} SELECT * from {} where {}>{} AND {}<={}'.format(InputTablePart2+str(i),InputTablePart2,Table2JoinColumn,parti2[i],Table2JoinColumn,parti2[i+1]))
    for i in range(5):
        t = threading.Thread(target = ThreadsWorking2,args = (InputTable1+str(i),InputTablePart2+str(i),Table1JoinColumn, Table2JoinColumn, OutputTable+str(i), openconnection,))
        threads.append(t)
        t.start()
    for i in range(5):
        cur.execute('INSERT INTO {} SELECT * from {}'.format(OutputTable,OutputTable+str(i)))

    cur.close()



################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment3
	print "Creating Database named as ddsassignment3"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment3 database"
	con = getOpenConnection();

    loadMovies('movies', 'movies.dat', con);

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	# deleteTables('parallelSortOutputTable', con);
 #       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
