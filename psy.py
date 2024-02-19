#!/usr/bin/python3
import psycopg2
import json
import sys
from types import *

def runPsy(conn, curs, jsonFile):
    with open(jsonFile) as f:
        for line in f:
            try:
                data = json.loads(line)
                if "newcustomer" in data:
                    if check_customer_exists(curs, data["newcustomer"]["customerid"]) == False and check_frequentflieron(curs, data["newcustomer"]["frequentflieron"]) == True:
                        sql_insert = """ INSERT INTO customers(customerid, name, birthdate, frequentflieron )
                                      VALUES (%s, %s, %s, (select airlineid from airlines where name = %s));"""
                        curs.execute(sql_insert, (data["newcustomer"]["customerid"], data["newcustomer"]["name"], data["newcustomer"]["birthdate"], data["newcustomer"]["frequentflieron"],))
                elif "flewon" in data:
                    for cust in data["flewon"]["customers"]:
                        if check_customer_exists(curs, cust["customerid"]) == False: 
                            if check_frequentflieron(curs, cust["frequentflieron"]) == True:
                                sql_insert = """ INSERT INTO customers(customerid, name, birthdate, frequentflieron )
                                      VALUES (%s, %s, %s, %s);"""
                                curs.execute(sql_insert, (cust["customerid"], cust["name"], cust["birthdate"], cust["frequentflieron"],)) 

                        sql_flight_insert = """INSERT INTO flewon(flightid, customerid, flightdate, id) 
                                              VALUES (%s, %s, %s, %s);"""
                        curs.execute(sql_flight_insert, (data["flewon"]["flightid"], cust["customerid"], data["flewon"]["flightdate"], get_lastid(curs)+1,))
            except:
                print("Error424")
                exit()
            conn.commit()
def check_customer_exists(curs, customerid):
    sql_check_customer = """ select customerid from customers where customerid = %s;"""
    curs.execute(sql_check_customer, (customerid,))
    cust_exists = curs.fetchone()
    if cust_exists is not None:
        return True
    else:
        return False

def check_frequentflieron(curs, frequentflieron):
    sql_check = """ select name from airlines where airlineid = %s or name = %s;"""
    curs.execute(sql_check, (frequentflieron, frequentflieron,))
    flightname_exists = curs.fetchone()
    if flightname_exists is not None:
        return True
    else:
        return False

def get_lastid(curs):
    sql_get_id = """ select id from flewon order by id desc LIMIT 1;"""
    curs.execute(sql_get_id)
    last_id = curs.fetchone()
    return last_id[0]