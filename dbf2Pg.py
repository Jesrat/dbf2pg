#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import dbf
import logging
import datetime
import psycopg2

# credits to
# https://github.com/foxcarlos/dbf2pg/blob/master/dbf2pg.py

def progress(count, total, status=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))
    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)
    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()

def crearTablaPg(pPyTabla):
    logging.info('lets extract fields from dbf table')
    '''Metodo que permite tomar la estructura de una tabla .DBF y
    crear una tabla en PostgreSQL con la misma estructura '''
    fields = ''
    for campo in pPyTabla.field_names:
        #Prepara los campos para hacer el Create Table - Prepare the fields to make the "Create Table"
        tipo, long, long2, tipo2 = pPyTabla.field_info(campo)
        tipo = chr(tipo)
        if campo == 'is':
            campo = 'ist'
        if tipo == 'C':
            c = "{0} character varying({1}),\n".format(campo, long)
        elif tipo == 'D':
            c = "{0} date,\n".format(campo)
        elif tipo == 'M':
            c = "{0} text,\n".format(campo)
        elif tipo == 'L':
            c = "{0} boolean,\n".format(campo)
        elif tipo == 'T':
            c = "{0} timestamp without time zone,\n".format(campo)
        elif tipo == 'I':
            c = "{0} integer,\n".format(campo)
        elif tipo == 'N':
            c = "{0} numeric({1},{2}),\n".format(campo, long, long2)

        fields += c
    crearTabla = 'CREATE TABLE table_name ({0})'.format(fields[:-2].strip())
    return crearTabla

def insertarReg(pPyTabla,pTableName):
    '''Metodo que permite tomar los registros de una tabla .DBF
    e insertarlos en una tabla en postgreSQL'''
    campo = ''
    valorValue = ''
    vInsertStatement = str()
    vTotalRecords = len(pPyTabla)
    vFailedRecords = 0
    for recordid, r in enumerate(pPyTabla):
        progress(recordid,vTotalRecords,status='[{0}]-Failed, [{1}]-Ok'.format(vFailedRecords,(recordid-vFailedRecords)))
        cur = conn.cursor()
        try:
            x = [f for f in  r]
            valorValue = ''
            for l in x:
                #type(1L) es Long type(1) es Entero y type(1.0) es Float
                if type(l) in [type(1L), type(1), type(1.0)]:
                    campo = "{0}".format(l)
                if isinstance(l, str) or isinstance(l, unicode):
                    #Ignoro cualquier caracter extraño
                    l = l.encode('ASCII', errors = 'ignore')
                    #Si el campo contiene un signo de $ lo remplazo por ''
                    l = l.replace("'", "''") if "'" in l else l
                    campo = "'{0}'".format(str(l).strip())
                elif isinstance(l, bool):
                    campo = "{0}".format(l)
                elif isinstance(l, datetime.date):
                    campo = "'{0}'".format(l)
                else:
                    campo = "{0}".format("null")
                valorValue += "{0},\n".format(campo)
            vInsertStatement = 'insert into {0}  values ({1})'.format(pTableName, valorValue[:-2])
            cur.execute(vInsertStatement)
            conn.commit()
        except Exception as e:
            vFailedRecords += 1
            logging.exception("got error in record {0} \nthe field contains: \n{1} \ni got the values: \n{2} \ninsert is: ( {3} ) \nand the error is {4}".format(recordid,r,campo,vInsertStatement,e))
        finally:
            cur.close()
    print("\n")
    

def main():
    vStrTable = raw_input('drag and drop table')
    vStrTable = vStrTable.replace("'",'').replace(" ",'')
    print('lets use the table {0}'.format(vStrTable))
    vNow = datetime.datetime.now()
    vTableName = '{0}_{1}'.format(os.path.basename(vStrTable).replace('.dbf',''),vNow.strftime('%d%m%Y%H%M%S'))
    vLogFile = os.path.join(os.path.dirname(vStrTable),'dbf2Pg{0}.log'.format(vTableName))
    logging.basicConfig(filename=vLogFile, filemode='w', format='%(asctime)s %(module)s %(message)s', level=logging.INFO)
    vPyDbfTable = dbf.Table(vStrTable)
    vPyDbfTable.open()
    logging.info('we will create {0} table'.format(vTableName))
    vCreateTable = crearTablaPg(vPyDbfTable)
    logging.info("got table definition: \n{0}".format(vCreateTable))
    cur = conn.cursor()
    cur.execute(vCreateTable.replace('table_name',vTableName))
    cur.close()
    conn.commit()
    logging.info('table created succesfully')
    logging.info('now we got to insert {0} records on it'.format(len(vPyDbfTable)))
    insertarReg(vPyDbfTable,vTableName)
    logging.info('we are done')


if __name__=='__main__':
    conn = None
    try:
        conn = psycopg2.connect(host="", user="", password="", dbname="")
        main()
    except Exception as e:
        logging.exception(e)
    finally:
        if conn is not None:
            conn.close()

