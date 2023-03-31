import os
from dotenv import load_dotenv

import datetime
import random
import itertools

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, select, func, text
from sqlalchemy.ext.automap import automap_base


DATABASE_URL = os.getenv('DATABASE_URL')
engine = create_engine(DATABASE_URL)

engine = create_engine(connection_string)
Session = sessionmaker(engine)

creation_query = """CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name varchar(255) NOT NULL,
    category varchar(255) NOT NULL,
    brand varchar(255) NOT NULL
);
CREATE TABLE IF NOT EXISTS stores (
    store_id SERIAL PRIMARY KEY,
    address VARCHAR(255) NOT NULL,
    region INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    surname VARCHAR(255),
    birth_date DATE NOT NULL
);
CREATE TABLE IF NOT EXISTS prices (
    price_id SERIAL UNIQUE,
    product_id INTEGER NOT NULL,
    price INTEGER NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    CONSTRAINT product_start_end PRIMARY KEY (product_id, start_date, end_date)
);
CREATE TABLE IF NOT EXISTS sales (
    sale_id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    store_id INTEGER NOT NULL,
    customer_id INTEGER NULL,
    sale_date DATE NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);
"""

with Session() as session:
    session.execute(text(creation_query))
    session.commit()
    
meta = MetaData()
meta.reflect(engine)

customers = Table("customers", meta, autoload_with=engine)
products = Table("products", meta, autoload_with=engine)
stores = Table("stores", meta, autoload_with=engine)
prices = Table("prices", meta, autoload_with=engine)
sales = Table("sales", meta, autoload_with=engine)

Base = automap_base()
Base.prepare(autoload_with=engine)

Customer = Base.classes.customers
Product = Base.classes.products
Store = Base.classes.stores
Price = Base.classes.prices
Sale = Base.classes.sales

PERIOD_START = datetime.date(2019, 12, 1)
PERIOD_END = datetime.date(2021, 1, 31)


def create_customers():
    names = "Григорий Макс Клава Никита Бугага Бугага Евпатий Большой Анатолий Светлана Валерий Елизавета".split(" ")
    surnames = "Лепс Корж Кока Джигурда Бугага Коловрат Вассерман Лобода Меладзе Монеточка".split() + [None]
    birth_dates = [
        datetime.date(1990, 1, 1),
        datetime.date(1980, 2, 2),
        datetime.date(1970, 3, 3),
        datetime.date(1960, 4, 4),
        datetime.date(2000, 5, 5)
    ]
    
    with Session() as session:
        session.execute(text("TRUNCATE TABLE customers CASCADE"))
        for name, surname in itertools.product(names, surnames):
            customer = Customer(name=name, surname=surname, birth_date=random.choice(birth_dates))
            session.add(customer)
        session.commit()
        

def create_stores():
    streets = "Ленина Маркса Энгельса Андропова Пушкина Лермонтова Паустовского Чайковского".split()
    cities = "Заозерск Приозерск Подозерск Междуреченск Залесск Загорск Подгорск Пригорск Медногорск".split()
    houses = "1 2 3 4 5 6 7 8 Колотушкина".split()
    
    with Session() as session:
        session.execute(text("TRUNCATE TABLE stores CASCADE"))
        for city, street, house in itertools.product(cities, streets, houses):
            store = Store(region=cities.index(city), address=f"г. {city}, ул. {street}, д. {house}")
            session.add(store)
        session.commit()
        
        
def create_products():
    categories = "сосиски колбаса ветчина сыр картоха молоко пиво соль сахар нулевочка подгузник макароны спагетти салат огурцы томаты яблоки торт вафли конфеты рис".split()
    brands = "Вкусняха Объедайка БольшойЖивот Обжорка Кулинар Жора КраснаяЦена ДешевоИТочка Вкус Вкусовщина".split()
    types = "вес. уп.".split()
    
    with Session() as session:
        session.execute(text("TRUNCATE TABLE products CASCADE"))
        for category, brand, typ in itertools.product(categories, brands, types):
            product = Product(name=f"{category} {brand} {typ}", category=category, brand=brand)
            session.add(product)
        session.commit()
        

def _create_periods():
    start_date = PERIOD_START
    period = 6 if random.random() > 0.9 else 13
    end_date = start_date + datetime.timedelta(days=period)
    yield start_date, end_date
    while end_date <= PERIOD_END:
        start_date = end_date + datetime.timedelta(days=1)
        period = 6 if random.random() > 0.9 else 13
        end_date = start_date + datetime.timedelta(days=period)
        yield start_date, end_date

        
def create_prices():
    with Session() as session:
        session.execute(text("TRUNCATE TABLE prices CASCADE"))
        for product_id in map(lambda row: row[0], session.execute(select(Product.product_id).distinct())):
            random.seed(product_id)
            for start_date, end_date in _create_periods():
                price_value = random.randint(2500, 30000)
                price = Price(product_id=product_id, price=price_value, start_date=start_date, end_date=end_date)
                session.add(price)
            session.commit()
            
            
def _iter_dates():
    dt = PERIOD_START
    while dt <= PERIOD_END:
        yield dt
        dt += datetime.timedelta(days=1)
        
        
def _get_price(product_id, date, session=None):
    with session or Session() as session:
        query = (
            select(Price.price)
            .filter(Price.product_id == product_id)
            .filter(Price.start_date <= date)
            .filter(Price.end_date >= date)
        )
        return session.execute(query).first()[0]


def create_sales():
    random.seed(42)
    with Session() as session:
        session.execute(text("TRUNCATE TABLE sales CASCADE"))
        product_ids = [pid[0] for pid in session.execute(select(Product.product_id)).all()]
        store_ids = [sid[0] for sid in session.execute(select(Store.store_id)).all()]
        customer_ids = [cid[0] for cid in session.execute(select(Customer.customer_id)).all()]
        customer_ids += [None] * 2 * len(customer_ids)
        for sale_date in _iter_dates():
            current_products = random.sample(product_ids, random.randint(10, 15))
            current_stores = random.sample(store_ids, random.randint(10, 15))
            current_customers = random.sample(customer_ids, random.randint(10, 15))
            print(sale_date)
            for pid in current_products:
                for sid in current_stores:
                    for cid in current_customers:
                        sale = Sale(product_id=pid, store_id=sid, customer_id=cid, sale_date=sale_date)
                        session.add(sale)
            session.commit()
            

create_customers()
create_stores()
create_products()
create_prices()
create_sales()