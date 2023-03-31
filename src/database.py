import os
from dotenv import load_dotenv

from sqlalchemy import create_engine, MetaData, Table, String, Integer, Column, Date, ForeignKey, select, insert, text
from  sqlalchemy.sql.expression import func

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base

from datetime import datetime


class Methods:

    def __init__(self):
        load_dotenv()

        DATABASE_URL = os.getenv('DATABASE_URL')

        # engine = create_engine("postgresql+psycopg2://user:@localhost:5432/")
        self.engine = create_engine(DATABASE_URL)
        self.connection = self.engine.connect()

        self.Session = sessionmaker(self.engine)

        self.metadata = MetaData()

        self.stores_table = Table(
            'stores',
            self.metadata, 
            Column('store_id', Integer(), primary_key=True),
            Column('address', String(200), nullable=False),
            Column('region', Integer(),  nullable=False, unique=False, index=False),
        )

        self.customers_table = Table(
            'customers',
            self.metadata, 
            Column('birth_date', Date(), primary_key=False),
            Column('customer_id', Integer(), nullable=False),
            Column('name', String(200), nullable=False),
            Column('surname', String(200), nullable=False)
        )

        self.prices_table = Table(
            'prices',
            self.metadata, 
            Column('end_date', Date(), primary_key=False),
            Column('price', Integer(), nullable=False),
            Column('price_id', Integer, nullable=False),
            Column('product_id', Integer, nullable=False),
            Column('start_date', Date(), nullable=False)
        )

        stores = Table("stores", self.metadata, autoload_with=self.engine)
        Base = automap_base()
        Base.prepare(autoload_with=self.engine)
        self.Store = Base.classes.stores


    def getStoreInfo(self, store_id):
        self.metadata.create_all(self.engine)

        select_statement = select(self.stores_table).where(self.stores_table.c.store_id == store_id)
        results = self.connection.execute(select_statement).fetchall()

        dict_result = {}
        for i in range(len(results)):
            d1 = []
            for j in range(len(results[i])):
                d1.append(results[i][j])
            dict_result[i] = d1
            
        if dict_result == {}:
            return "store not found"
        else:
            return dict_result


    def getStores(self):
        self.metadata.create_all(self.engine)

        select_statement = select(self.stores_table)
        results = self.connection.execute(select_statement).fetchall()

        dict_result = {}

        for i in range(len(results)):
            d1 = []
            for j in range(len(results[i])):
                d1.append(results[i][j])
            dict_result[i] = d1
            
        return dict_result


    def getCustomers(self):
        self.metadata.create_all(self.engine)

        select_statement = select(self.customers_table).limit(10).order_by(func.random())
        results = self.connection.execute(select_statement).fetchall()

        dict_result = {}
        for i in range(len(results)):
            d1 = []
            for j in range(len(results[i])):
                if j == 0:
                    d1.append(str(results[i][j]))
                else:
                    d1.append(results[i][j])
            dict_result[i] = d1
        
        return dict_result


    def getPricesMax(self):
        """ 
        SELECT product_id, MAX(price) FROM prices GROUP BY product_id;
        """
        self.metadata.create_all(self.engine)

        # select_statement = select(self.prices_table.c.product_id, func.max(self.prices_table.c.price))
        select_statement = text("""SELECT product_id, MAX(price) FROM prices GROUP BY product_id""")
        results = self.connection.execute(select_statement).fetchall()

        dict_result = {}

        for i in range(len(results)):
            d1 = []
            for j in range(len(results[i])):
                d1.append(results[i][j])
            dict_result[i] = d1
            
        return dict_result


    def addNewStore(self, address, region):
        with self.Session() as session:
            store = self.Store(region=f"{region}", address=f"{address}")
            session.add(store)
        session.commit()

        return True

