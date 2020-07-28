from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import scoped_session, sessionmaker
from flask_jwt_extended import JWTManager
from nba_api.stats.static import teams
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from flask_executor import Executor
from dotenv import load_dotenv
from datetime import datetime, timedelta
from flask_cors import CORS
from string import capwords
from flask import Flask, g
from flask_socketio import SocketIO, emit
from flask_apscheduler import APScheduler
from os import getenv
from warnings import filterwarnings

import pandas as pd
import string
import time
import csv
import math

app = Flask(__name__)
CORS(app)

app.config['SQLALCHEMY_DATABASE_URI'] = getenv('CLEARDB_DATABASE_URL')
app.config['SQLALCHEMY_POOL_SIZE'] = 20
app.config['CORS_HEADERS'] = 'Content-Type'
app.config['JWT_TOKEN_LOCATION'] = ['cookies', 'headers']
app.config['JWT_SECRET_KEY'] = getenv('API_SECRET')
app.config['JWT_BLACKLIST_ENABLED'] = True
app.config['JWT_BLACKLIST_TOKEN_CHECKS'] = ['access', 'refresh']
app.config['JWT_COOKIE_CSRF_PROTECT'] = True
app.config['JWT_SESSION_COOKIE'] = False
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False


db = SQLAlchemy(app)
jwt = JWTManager(app)
io = SocketIO(app, cors_allowed_origins='http://localhost:3000')

executor = Executor(app)

def get_db():
    if 'db' not in g:
        g.db = SQLAlchemy(app)
    return g.db

@app.teardown_appcontext
def teardown_db(exc):
    db = g.pop('db', None)
    if db is not None:
        db.session.remove()

from fanbasemarket.routes.auth import auth
from fanbasemarket.routes.users import users
from fanbasemarket.routes.teams import teams

app.register_blueprint(auth, url_prefix='/api/auth/')
app.register_blueprint(users, url_prefix='/api/users/')
app.register_blueprint(teams, url_prefix='/api/teams/')

scheduler = APScheduler()

from fanbasemarket.pricing.live import bigboy_pulls_only

@scheduler.task('interval', seconds=10)
def pull_and_emit():
    with app.app_context():
        db = get_db()
        results = bigboy_pulls_only(db)
        emit('prices', results, broadcast=True, namespace='/')

def create_app():
    scheduler.init_app(app)
    scheduler.start()
    return app
