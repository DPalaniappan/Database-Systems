from peewee import *
from datetime import date
import json

database = PostgresqlDatabase('flightsskewed', **{'host': 'localhost', 'user': 'vagrant', 'password': 'vagrant'})

class UnknownField(object):
    def __init__(self, *_, **__): pass

class BaseModel(Model):
    class Meta:
        database = database

class NumberOfFlightsTaken(BaseModel):
    customerid = CharField(null=True)
    customername = CharField(null=True)
    numflights = BigIntegerField(null=True)
    class Meta:
        table_name = 'NumberOfFlightsTaken'
        primary_key = False

class Airports(BaseModel):
    airportid = CharField(primary_key=True)
    city = CharField(null=True)
    name = CharField(null=True)
    total2011 = IntegerField(null=True)
    total2012 = IntegerField(null=True)
    class Meta:
        table_name = 'airports'

class Airlines(BaseModel):
    airlineid = CharField(primary_key=True)
    hub = ForeignKeyField(column_name='hub', field='airportid', model=Airports, null=True)
    name = CharField(null=True)
    class Meta:
        table_name = 'airlines'

class Customers(BaseModel):
    birthdate = DateField(null=True)
    customerid = CharField(primary_key=True)
    frequentflieron = ForeignKeyField(column_name='frequentflieron', field='airlineid', model=Airlines, null=True)
    name = CharField(null=True)
    class Meta:
        table_name = 'customers'

class Flights(BaseModel):
    airlineid = ForeignKeyField(column_name='airlineid', field='airlineid', model=Airlines, null=True)
    dest = ForeignKeyField(column_name='dest', field='airportid', model=Airports, null=True)
    flightid = CharField(primary_key=True)
    local_arrival_time = TimeField(null=True)
    local_departing_time = TimeField(null=True)
    source = ForeignKeyField(backref='airports_source_set', column_name='source', field='airportid', model=Airports, null=True)
    class Meta:
        table_name = 'flights'

class Flewon(BaseModel):
    customerid = ForeignKeyField(column_name='customerid', field='customerid', model=Customers, null=True)
    flightdate = DateField(null=True)
    flightid = ForeignKeyField(column_name='flightid', field='flightid', model=Flights, null=True)
    id = IntegerField(primary_key=True)
    class Meta:
        table_name = 'flewon'

class Numberofflightstaken(BaseModel):
    customerid = CharField(null=True)
    customername = CharField(null=True)
    numflights = BigIntegerField(null=True)
    class Meta:
        table_name = 'numberofflightstaken'
        primary_key = False

def runORM(jsonFile):
    with open(jsonFile) as f:
        for line in f:
            
                data = json.loads(line)
                if "newcustomer" in data:
                    if not Customers.select().where(Customers.customerid == data["newcustomer"]["customerid"]).exists():
                        if Airlines.select().where(Airlines.name == data["newcustomer"]["frequentflieron"]).exists():
                            newcustomer = Customers(name= data["newcustomer"]["name"], customerid= data["newcustomer"]["customerid"], birthdate = data["newcustomer"]["birthdate"], frequentflieron= Airlines.select(Airlines.airlineid).where(Airlines.name == data["newcustomer"]["frequentflieron"]))
                            newcustomer.save(force_insert=True)

                elif "flewon" in data: 
                    for cust in data["flewon"]["customers"]:
                        if not Customers.select().where(Customers.customerid == cust["customerid"]).exists():
                            if Airlines.select().where(Airlines.airlineid == cust["frequentflieron"]).exists():
                                newcustomer = Customers(name = cust["name"], customerid = cust["customerid"], birthdate = cust["birthdate"], frequentflieron = cust["frequentflieron"])
                                newcustomer.save(force_insert=True)

                        max_id = Flewon.select(Flewon.id).order_by(Flewon.id.desc()).limit(1).scalar()
                        newflewon = Flewon(flightid = data["flewon"]["flightid"], customerid= cust["customerid"], flightdate = data["flewon"]["flightdate"], id= max_id + 1)
                        newflewon.save(force_insert=True)

    Numberofflightstaken.delete().execute()
    for cust in Customers.select().order_by(Customers.name):
        flightcount = Flewon.select().where(Flewon.customerid == cust.customerid).count()
        newnumflighttaken = Numberofflightstaken(customerid = cust.customerid, customername = cust.name, numflights = flightcount)
        newnumflighttaken.save(force_insert = True)          
 
    
                    