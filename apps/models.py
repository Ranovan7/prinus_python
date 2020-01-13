import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import UserMixin

from apps import login
from apps import db
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy import desc


class Raw(db.Model):
    __tablename__ = 'raw'

    id = db.Column(db.Integer, primary_key=True)
    content = db.Column(JSONB, unique=True)
    received = db.Column(db.DateTime, default=datetime.datetime.utcnow)


class Users(UserMixin, db.Model):
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(12), index=True, unique=True, nullable=False)
    password = db.Column(db.String(128))

    def set_password(self, password):
        self.password = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password, password)

    def __repr__(self):
        return '<User %r>' % self.username


class Tenant(db.Model):
    __tablename__ = 'tenant'

    id = db.Column(db.Integer, primary_key=True)
    nama = db.Column(db.String(35), index=True, unique=True, nullable=False)
    slug = db.Column(db.String(12), index=True, unique=True, nullable=False)

    locations = relationship('Location', backref='location_tenant')
    loggers = relationship('Logger', backref='logger_tenant')
    periodiks = relationship('Periodik', backref='periodik_tenant')

    created_at = db.Column(db.DateTime)
    modified_at = db.Column(db.DateTime)


@login.user_loader
def load_user(id):
    return User.query.get(int(id))


class Logger(db.Model):
    __tablename__ = 'logger'

    id = db.Column(db.Integer, primary_key=True)
    sn = db.Column(db.String(10), index=True, unique=True, nullable=False)
    tipe = db.Column(db.String(12), default="arr")
    location_id = db.Column(db.Integer, db.ForeignKey('location.id'), nullable=True)
    tenant_id = db.Column(db.Integer, db.ForeignKey('tenant.id'), nullable=True)
    logger_periodik = db.relationship('Periodik', back_populates='logger', lazy='dynamic')
    temp_cor = db.Column(db.Float)
    humi_cor = db.Column(db.Float)
    batt_cor = db.Column(db.Float)
    tipp_fac = db.Column(db.Float)
    ting_son = db.Column(db.Float)  # dalam centi, tinggi sonar thd dasar sungai

    location = relationship('Location', backref='logger')
    tenant = relationship('Tenant', back_populates='loggers')

    created_at = db.Column(db.DateTime)
    modified_at = db.Column(db.DateTime)
    # latest_sampling = db.Column(db.DateTime)
    # latest_up = db.Column(db.DateTime)
    # latest_id = db.Column(db.Integer)

    def __repr__(self):
        return '<Device {}>'.format(self.sn)


class Location(db.Model):
    __tablename__ = 'location'

    id = db.Column(db.Integer, primary_key=True)
    nama = db.Column(db.String(50), index=True, unique=True)
    ll = db.Column(db.String(35))
    tipe = db.Column(db.String(1))  # 1 CH, 2 TMA, 3 Bendungan, 4 Klim
    tenant_id = db.Column(db.Integer, db.ForeignKey('tenant.id'), nullable=True)

    # logger_tenant = relationship('Tenant', back_populates='locations')
    # periodik = relationship('Periodik', back_populates='lokasi',
    #                         order_by="desc(Periodik.sampling)")

    created_at = db.Column(db.DateTime)
    modified_at = db.Column(db.DateTime)
    # latest_sampling = db.Column(db.DateTime)
    # latest_up = db.Column(db.DateTime)
    # latest_id = db.Column(db.Integer)


class Periodik(db.Model):
    __tablename__ = 'periodik'

    id = db.Column(db.Integer, primary_key=True)
    sampling = db.Column(db.DateTime, index=True)
    logger_sn = db.Column(db.String(8), db.ForeignKey('logger.sn'))
    location_id = db.Column(db.Integer, db.ForeignKey('location.id'), nullable=True)
    tenant_id = db.Column(db.Integer, db.ForeignKey('tenant.id'), nullable=True)
    mdpl = db.Column(db.Float)
    apre = db.Column(db.Float)
    sq = db.Column(db.Integer)
    temp = db.Column(db.Float)
    humi = db.Column(db.Float)
    batt = db.Column(db.Float)
    rain = db.Column(db.Float)  # hujan dalam mm
    wlev = db.Column(db.Float)  # TMA dalam centi
    up_s = db.Column(db.DateTime)  # Up Since
    ts_a = db.Column(db.DateTime)  # Time Set at
    received = db.Column(db.DateTime, default=datetime.datetime.utcnow)

    logger = relationship("Logger", back_populates="logger_periodik")
    # lokasi = relationship("Location", back_populates="location_periodik")
    # periodik_tenant = relationship("Tenant", back_populates="periodiks")
    # __table_args__ = (db.UniqueConstraint('device_sn', 'sampling',
    #                                       name='_device_sampling'),)
#
#     def __repr__(self):
#         return '<Periodik {} Device {}>'.format(self.sampling, self.device_sn)
#     @classmethod
#     def temukan_hujan(self, sejak=None):
#         '''return periodik yang rain > 0'''
#         dari = 30 # hari lalu
#         if not sejak:
#             sejak = datetime.datetime.now() - datetime.timedelta(days=dari)
#             sejak = sejak.replace(minute=0, hour=7)
#         data = [d for d in self.query.filter(self.sampling >=
#                                              sejak).order_by(self.sampling)]
#         lokasi_hari_hujan = [d.lokasi_id for d in data if (d.rain or 0) > 0]
#         print(lokasi_hujan)
