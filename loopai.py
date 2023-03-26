import csv
from datetime import datetime
from pytz import timezone
from flask import Flask, jsonify
from sqlalchemy import create_engine, Column, Integer, String, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# create a timezone object for UTC
utc = timezone('UTC')

# create the Flask app
app = Flask(__name__)

# create the database engine and session
engine = create_engine('sqlite:///store_status.db')
Session = sessionmaker(bind=engine)
session = Session()

# create the database tables
Base = declarative_base()
class Store(Base):
    __tablename__ = 'stores'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    timezone = Column(String)

class StoreStatus(Base):
    __tablename__ = 'store_statuses'
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer)
    timestamp_utc = Column(DateTime)
    status = Column(String)
    
class BusinessHour(Base):
    __tablename__ = 'business_hours'
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer)
    day_of_week = Column(Integer)
    start_time_local = Column(DateTime)
    end_time_local = Column(DateTime)

Base.metadata.create_all(engine)

# store data from data_source1.csv into the database
with open('D:\project\store status.csv', 'r') as f:
    reader = csv.DictReader(f)

    # iterate over each row in the CSV file
    for row in reader:
        # convert the timestamp to a datetime object in UTC timezone
        timestamp_utc = datetime.strptime(row['timestamp_utc'][:-4], '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=utc)


        # create a new store object or retrieve an existing one from the database
        store = session.query(Store).filter_by(id=row['store_id']).first()
        if not store:
            store = Store(id=row['store_id'], name=f"Store {row['store_id']}", timezone='America/Chicago')
            session.add(store)

        # create a new status object and associate it with the store
        status = StoreStatus(store_id=row['store_id'], timestamp_utc=timestamp_utc, status=row['status'])
        session.add(status)

    # commit the changes to the database
    session.commit()

# store data from data_source2.csv into the database
with open('D:\project\Menu hours.csv', 'r') as f:
    reader = csv.DictReader(f)

    # iterate over each row in the CSV file
    for row in reader:
        # retrieve the store from the database
        store = session.query(Store).filter_by(id=row['store_id']).first()
        if store:
            # create or update the store's business hours
            day_of_week = int(row['dayOfWeek'])
            start_time_local = datetime.strptime(row['start_time_local'], '%H:%M:%S').time()
            end_time_local = datetime.strptime(row['end_time_local'], '%H:%M:%S').time()

            business_hour = session.query(BusinessHour).filter_by(store_id=row['store_id'], day_of_week=day_of_week).first()
            if not business_hour:
                business_hour = BusinessHour(store_id=row['store_id'], day_of_week=day_of_week, start_time_local=datetime.combine(datetime.min, start_time_local), end_time_local=datetime.combine(datetime.min, end_time_local))
                session.add(business_hour)
            else:
                business_hour.start_time_local = datetime.combine(datetime.min, start_time_local)
                business_hour.end_time_local = datetime.combine(datetime.min, end_time_local)

    # commit the changes to the database
    session.commit()


# store data from data_source3.csv into the database
with open('D:\project\bq.csv', 'r') as f:
    reader = csv.DictReader(f)

    for row in reader:
        # create a new store object or retrieve an existing one from the database
        store = session.query(Store).filter_by(id=row['store_id']).first()
        if not store:
            store = Store(id=row['store_id'], name=f"Store {row['store_id']}", timezone='America/Chicago')
            session.add(store)

        # update the store's timezone
        store.timezone = row['timezone_str']

    # commit the changes to the database
    session.commit()

from datetime import timedelta

# create a route that outputs the report to the user
@app.route('/report')
def report():
    # get the max timestamp among all the observations in the first CSV
    max_timestamp = session.query(func.max(StoreStatus.timestamp_utc)).scalar()

    # calculate the start and end times for the time intervals
    last_hour_end = max_timestamp
    last_hour_start = last_hour_end - timedelta(hours=1)
    last_day_end = last_hour_end
    last_day_start = last_day_end - timedelta(days=1)
    last_week_end = last_day_end
    last_week_start = last_week_end - timedelta(weeks=1)

    # get the list of stores and their statuses within the time intervals
    stores = session.query(Store).all()
    results = []
    for store in stores:
        # get the store's business hours
        business_hours = session.query(BusinessHour).filter_by(store_id=store.id).all()

        # calculate the uptime and downtime within each time interval
        uptime_last_hour = downtime_last_hour = uptime_last_day = downtime_last_day = uptime_last_week = downtime_last_week = 0
        for business_hour in business_hours:
            # calculate the uptime and downtime within each business hour
            start_time_utc = business_hour.start_time_local.replace(tzinfo=timezone(store.timezone)).astimezone(utc)
            end_time_utc = business_hour.end_time_local.replace(tzinfo=timezone(store.timezone)).astimezone(utc)
            status_events = session.query(StoreStatus).filter_by(store_id=store.id).filter(StoreStatus.timestamp_utc >= start_time_utc).filter(StoreStatus.timestamp_utc <= end_time_utc).order_by(StoreStatus.timestamp_utc.asc()).all()

            if len(status_events) > 0:
                last_status = status_events[0]
                last_status_time = last_status.timestamp_utc
                for status_event in status_events[1:]:
                    # calculate the downtime since the last status change
                    time_since_last_status = status_event.timestamp_utc - last_status_time
                    if last_status.status == 'OPEN':
                        uptime_last_hour += max((time_since_last_status - timedelta(minutes=1)).total_seconds() / 60, 0)
                        uptime_last_day += max((time_since_last_status - timedelta(minutes=1)).total_seconds() / 3600, 0)
                        uptime_last_week += max((time_since_last_status - timedelta(minutes=1)).total_seconds() / 3600 / 24, 0)
                    else:
                        downtime_last_hour += max((time_since_last_status - timedelta(minutes=1)).total_seconds() / 60, 0)
                        downtime_last_day += max((time_since_last_status - timedelta(minutes=1)).total_seconds() / 3600, 0)
                        downtime_last_week += max((time_since_last_status - timedelta(minutes=1)).total_seconds() / 3600 / 24, 0)
                    last_status = status_event
                    last_status_time = last_status.timestamp_utc

                # calculate the downtime since the last status change to the end of the business hour
                time_since_last_status = end_time_utc - last_status_time
                if last_status.status == 'OPEN':
                    uptime_last_hour += max((time_since_last_status - timedelta(minutes=1)).total_seconds() / 60, 0)
                    uptime_last_day += max((time_since_last_status - timedelta(minutes=1)).total_seconds() / 3600, 0)
                    downtime_last_hour += max((business_hour_end_utc - end_time_utc).total_seconds() / 60, 0)
                    downtime_last_day += max((business_hour_end_utc - end_time_utc).total_seconds() / 3600, 0)
                else:
                    downtime_last_hour += max((time_since_last_status - timedelta(minutes=1)).total_seconds() / 60, 0)
                    downtime_last_day += max((time_since_last_status - timedelta(minutes=1)).total_seconds() / 3600, 0)
                    uptime_last_hour += max((business_hour_end_utc - end_time_utc).total_seconds() / 60, 0)
                    uptime_last_day += max((business_hour_end_utc - end_time_utc).total_seconds() / 3600, 0)
