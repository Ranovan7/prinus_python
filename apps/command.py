import click
import logging
import requests
import datetime
import os
import json
import daemonocle
import paho.mqtt.subscribe as subscribe

from sqlalchemy import func, or_, desc
from sqlalchemy.exc import IntegrityError
from pytz import timezone

from telegram import Bot

from apps import app, db
from apps.models import Logger, Location, Raw, Tenant, Periodik

bws_sul2 = ("bwssul2", "limboto1029")

URL = "https://prinus.net/api/sensor"
MQTT_HOST = "mqtt.bbws-bsolo.net"
MQTT_PORT = 14983
MQTT_TOPICS = "sensors"
MQTT_CLIENT = None
HUJAN_LEBAT = 10
HUJAN_SANGAT_LEBAT = 20
POS_NAME = {
    '1': "Hujan",
    '2': "Duga Air",
    '4': "Klimatologi"
}

logging.basicConfig(
        filename='/tmp/pbasemqttsub.log',
        level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')


def utc2local(time, tz="Asia/Jakarta"):
    ''' return with year-month-day hours:minutes:seconds in local(tz) timezone '''
    time = time.astimezone(timezone(tz))
    return time


def local2utc(time):
    ''' return with year-month-day hours:minutes:seconds in local(tz) timezone '''
    time = time.astimezone(timezone('UTC'))
    return time


def getstarttime(time):
    ''' get starting time of the data '''
    res = time.hour < 7 and (time - datetime.timedelta(days=1)) or time
    return res.replace(hour=7, minute=0, second=0)


def prettydate(d):
    diff = datetime.datetime.utcnow() - d
    s = diff.seconds
    if diff.days > 30:
        return f"Lebih dari Sebulan Lalu"
    elif diff.days > 7 and diff.days < 30:
        return f"Lebih dari Seminggu Lalu"
    elif diff.days >= 1 and diff.days < 7:
        return f"{diff.days} Hari Lalu"
    elif s < 3600:
        return f"{round(s/60)} Menit Lalu"
    else:
        return f"{round(s/3600)} Jam Lalu"


@app.cli.command()
@click.argument('command')
def telegram(command):
    time = datetime.datetime.now()
    # time = datetime.datetime.strptime("2020-01-09 11:00:00", "%Y-%m-%d %H:%M:%S")
    if command == 'test':
        print(send_telegram())
    elif command == 'periodik':
        print("Sending Periodik Info")
        periodik_report(time)
    elif command == 'count':
        print("Sending Today's Periodik Count")
        periodik_count_report(time)
    elif command == 'warning':
        print("Sending Alert Message")
        rain_alert(time)


def send_telegram(bot, id, message, debug_text):
    debug_text = f"Sending Telegram to {id}"
    try:
        bot.sendMessage(id, text=message)
        logging.debug(f"{debug_text}")
    except Exception as e:
        logging.debug(f"{debug_text} Error : {e}")


def periodik_report(time):
    ''' Message Tenants about last 2 hour rain and water level '''
    bot = Bot(token=app.config['PRINUSBOT_TOKEN'])

    ch_report(time, bot)
    tma_report(time, bot)

    # bot.sendMessage(app.config['TELEGRAM_TEST_ID'], text="Sending 2-Hourly Reports to All Tenants")


def ch_report(time, bot):
    tenants = Tenant.query.order_by(Tenant.id).all()

    for ten in tenants:
        tz = ten.timezone or "Asia/Jakarta"
        localtime = utc2local(time, tz=tz)
        end = datetime.datetime.strptime(f"{localtime.strftime('%Y-%m-%d')} {localtime.hour}:00:00", "%Y-%m-%d %H:%M:%S")
        start = getstarttime(end)

        final = f"*Curah Hujan {end.strftime('%d %b %Y')}*\n"
        final += f"{start.strftime('%H:%M')} - {end.strftime('%H:%M')}\n"
        message = ""

        locations = Location.query.filter(
                                    or_(Location.tipe == '1', Location.tipe == '4'),
                                    Location.tenant_id == ten.id).all()

        i = 0
        for pos in locations:
            result = get_periodik_sum(pos, start, end)
            latest = get_latest_telemetri(pos)

            i += 1
            rain = f"{round(result['rain'], 2)} mm selama {result['duration']} menit" if result['rain'] > 0 else '-'
            message += f"\n{i}. {pos.nama} : {rain}"
            message += f"\n     {result['percent']}%, data terakhir {latest['latest']}\n"

        if message:
            final += message
        else:
            final += "\nBelum Ada Lokasi yg tercatat"

        send_telegram(bot, ten.telegram_info_id, final, f"TeleRep-send {ten.nama}")

        print(f"{ten.nama}")
        print(final)
        print()


def tma_report(time, bot):
    tenants = Tenant.query.order_by(Tenant.id).all()

    for ten in tenants:
        locations = Location.query.filter(
                                    Location.tipe == '2',
                                    Location.tenant_id == ten.id).all()

        final = f"*TMA*\n"
        message = ""
        i = 0
        for pos in locations:
            latest = get_latest_telemetri(pos)

            i += 1
            if latest['periodik']:
                info = f"{latest['periodik'].wlev or '-'}, {latest['latest']}"
                tgl = f"\n     ({latest['periodik'].sampling.strftime('%d %b %Y, %H:%M')})\n"
            else:
                info = "Belum Ada Data"
                tgl = "\n"
            message += f"\n{i}. {pos.nama} : {info}"
            message += tgl

        if message:
            final += message
        else:
            final += "\nBelum Ada Lokasi yg tercatat"

        send_telegram(bot, ten.telegram_info_id, final, f"TeleRep-send {ten.nama}")

        print(final)
        print()


def get_periodik_sum(pos, start, end):
    periodics = Periodik.query.filter(
                                Periodik.sampling.between(local2utc(start), local2utc(end)),
                                Periodik.location_id == pos.id).all()
    result = {
        'pos': pos,
        'rain': 0,
        'duration': 0,
        'percent': 0
    }
    for period in periodics:
        result['rain'] += period.rain
        result['duration'] += 5
        result['percent'] += 1

    diff = end - start
    percent = (result['percent']/(diff.seconds/300)) * 100
    result['percent'] = round(percent, 2)

    return result


def get_latest_telemetri(pos):
    latest = Periodik.query.filter(Periodik.location_id == pos.id).order_by(desc(Periodik.sampling)).first()

    result = {
        'periodik': latest,
        'lastest': ""
    }
    if latest:
        result['latest'] = prettydate(latest.sampling)
    else:
        result['latest'] = "Belum Ada Data"
    return result


def periodik_count_report(time):
    ''' Message Tenants about last day periodic counts '''
    bot = Bot(token=app.config['PRINUSBOT_TOKEN'])

    tenants = Tenant.query.order_by(Tenant.id).all()

    for ten in tenants:
        # param tz should be entered if tenant have timezone
        # log.tenant.timezone
        tz = ten.timezone or "Asia/Jakarta"
        localtime = utc2local(time, tz=tz)
        end = datetime.datetime.strptime(f"{localtime.year}-{localtime.month}-{time.day - 1} 23:56:00", "%Y-%m-%d %H:%M:%S")
        start = datetime.datetime.strptime(f"{localtime.year}-{localtime.month}-{time.day - 1} 00:00:00", "%Y-%m-%d %H:%M:%S")

        final = '''*%(ten)s*\n*Kehadiran Data*\n%(tgl)s (0:0 - 23:55)
        ''' % {'ten': ten.nama, 'tgl': start.strftime('%d %b %Y')}
        message = ""

        locations = Location.query.filter(
                                    # Location.tipe == '1',
                                    Location.tenant_id == ten.id).all()

        i = 0
        for pos in locations:
            i += 1
            result = get_periodic_arrival(pos, start, end)
            message += f"\n{i} {pos.nama} ({result['tipe']}) : {result['percent']}%"

        if message:
            final += message
        else:
            final += "\nBelum Ada Lokasi yg tercatat"

        send_telegram(bot, ten.telegram_info_id, final, f"TeleCount-send {ten.nama}")
        print(final)
        print()
    bot.sendMessage(app.config['TELEGRAM_TEST_ID'], text="Sending Daily Count Reports to All Tenants")


def get_periodic_arrival(pos, start, end):
    periodics = Periodik.query.filter(
                                Periodik.sampling.between(local2utc(start), local2utc(end)),
                                Periodik.location_id == pos.id).all()
    tipe = POS_NAME[pos.tipe] if pos.tipe else "Lain-lain"
    result = {
        'pos': pos,
        'tipe': tipe,
        'percent': 0
    }
    for period in periodics:
        result['percent'] += 1

    diff = end - start
    percent = (result['percent']/(diff.seconds/300)) * 100
    result['percent'] = round(percent, 2)

    return result


def rain_alert(time):
    ''' Message Tenants about heavy rain per location '''
    start = datetime.datetime.strptime(f"{time.strftime('%Y-%m-%d')} {time.hour - 1}:00:00", "%Y-%m-%d %H:%M:%S")
    end = datetime.datetime.strptime(f"{time.strftime('%Y-%m-%d')} {time.hour - 1}:59:00", "%Y-%m-%d %H:%M:%S")

    bot = Bot(token=app.config['PRINUSBOT_TOKEN'])

    periodik_result = {}
    periodics = Periodik.query.filter(Periodik.sampling.between(start, end))
    for period in periodics:
        if period.logger.tipe in ['awlr']:
            continue
        location_name = period.location.nama if period.location else f"Lokasi {period.logger_sn}"
        if period.logger.tenant.nama not in periodik_result:
            periodik_result[period.logger.tenant.nama] = {
                'logger': {},
                'telegram_group': period.periodik_tenant.telegram_alert_group,
                'telegram_id': period.periodik_tenant.telegram_alert_id
            }
        if location_name not in periodik_result[period.logger.tenant.nama]['logger']:
            periodik_result[period.periodik_tenant.nama]['logger'][location_name] = 0
        periodik_result[period.periodik_tenant.nama]['logger'][location_name] += round(period.rain or 0, 2)

    for ten, info in periodik_result.items():
        print(f"{ten}")
        header = f"Peringatan Hujan, {time.strftime('%d %b %Y')}, pukul {time.hour - 1} sampai {time.hour}\n"
        message = ""
        for loc, rain in info['logger'].items():
            if rain > HUJAN_SANGAT_LEBAT:
                message += f"Terjadi Hujan Sangat Lebat di {loc} dengan intensitas {rain} mm\n"
            elif rain > HUJAN_LEBAT:
                message += f"Terjadi Hujan Lebat di {loc} dengan intensitas {rain} mm\n"
        if info['telegram_id'] and message:
            try:
                logging.debug(f"TeleWarn-send to {ten}")
                final = header + message
                bot.sendMessage(info['telegram_id'], text=final)
            except Exception as e:
                logging.debug(f"TeleWarn-send Error ({ten}) : {e}")
        print(header)
    # bot.sendMessage(app.config['TELEGRAM_TEST_ID'], text="Sending Alert to All Tenants")


@app.cli.command()
@click.argument('command')
def listen(command):
    daemon = daemonocle.Daemon(worker=subscribe_topic,
                              pidfile='listener.pid')
    daemon.do_action(command)


def on_mqtt_message(client, userdata, msg):
    data = json.loads(msg.payload.decode('utf-8'))
    # logging.debug(data.get('device'))
    # logging.debug('Message Received')
    # logging.debug(f"Topic : {msg.topic}")
    result = recordperiodic(data)
    logging.debug(result)


def subscribe_topic():
    logging.debug('Start listen...')
    # MQTT_TOPICS = [ten.slug for ten in Tenant.query.all()]
    logging.debug(f"Topics : {MQTT_TOPICS}")
    subscribe.callback(on_mqtt_message, MQTT_TOPICS,
                       hostname=MQTT_HOST, port=MQTT_PORT)
    logging.debug('Subscribed')


@app.cli.command()
def fetch_logger():
    res = requests.get(URL, auth=bws_sul2)

    if res.status_code == 200:
        logger = json.loads(res.text)
        local_logger = [d.sn for d in Device.query.all()]
        if len(local_logger) != len(logger):
            for l in logger:
                if l.get('sn') not in local_logger:
                    new_logger = Device(sn=l.get('sn'))
                    db.session.add(new_logger)
                    db.session.commit()
                    print('Tambah:', new_logger.sn)
    else:
        print(res.status_code)


@app.cli.command()
@click.option('-s', '--sampling', default='', help='Awal waktu sampling')
def fix_rain(sampling):
    if not sampling:
        today = datetime.datetime.today()
        tdy_str = today.strftime("%Y-%m-%d")
        start = datetime.datetime.strptime(f"{tdy_str} 00:00:00", "%Y-%m-%d %H:%M:%S")
    else:
        start = datetime.datetime.strptime(f"{sampling} 00:00:00", "%Y-%m-%d %H:%M:%S")
    end = start + datetime.timedelta(days=1)
    all_raw = Raw.query.filter(Raw.received.between(start, end)).order_by(Raw.received).all()
    # print(f"{ins_per.content.get('sampling')} - {ins_per.content.get('device')} : {ins_per.content.get('tick')}")
    count = 1
    for ins_per in all_raw:
        try:
            sampling = datetime.datetime.fromtimestamp(ins_per.content.get('sampling'))
            sn = ins_per.content.get('device').split('/')[1]
            logger = Logger.query.filter_by(sn=sn).first()
            period = Periodik.query.filter_by(
                            sampling=sampling,
                            logger_sn=logger.sn).first()
            if period:
                new_rain = round(ins_per.content.get('tick') * (logger.tipp_fac or 0.2), 2) if ins_per.content.get('tick') else None
                period.rain = new_rain
                db.session.commit()
                print(f"(UPDATE {count} {sampling}) {logger.sn} : {new_rain}")
            else:
                recordperiodic(ins_per.content)
                print(f"(INSERT {count} {sampling}) {logger.sn} : {ins_per.content.get('tick')}")
            count += 1
        except Exception as e:
            print(f"Error : {e}")


@app.cli.command()
@click.argument('sn')
@click.option('-s', '--sampling', default='', help='Awal waktu sampling')
def fetch_periodic(sn, sampling):
    sampling_param = ''
    if sampling:
        sampling_param = '&sampling=' + sampling
    res = requests.get(URL + '/' + sn + '?robot=1' + sampling_param, auth=bws_sul2)
    data = json.loads(res.text)
    for d in data:
        content = Raw(content=d)
        db.session.add(content)
        try:
            db.session.commit()
            recordperiodic(d)
        except Exception as e:
            db.session.rollback()
            print("ERROR:", e)
        print(d.get('sampling'), d.get('temperature'))


@app.cli.command()
@click.option('-s', '--sampling', default='', help='Awal waktu sampling')
def fetch_periodic_today(sampling):
    devices = Logger.query.all()
    today = datetime.datetime.today()
    if not sampling:
        sampling = today.strftime("%Y/%m/%d")
    for d in devices:
        try:
            print(f"Fetch Periodic for {d.sn}")
            logging.debug(f"Fetch Periodic for {d.sn}")
            os.system(f"flask fetch-periodic {d.sn} -s {sampling}")
        except Exception as e:
            print(f"!!Fetch Periodic ({d.sn}) ERROR : {e}")
            logging.debug(f"!!Fetch Periodic ({d.sn}) ERROR : {e}")


def recordperiodic(raw):
    sn = str(raw.get('device').split('/')[1])
    try:
        db.session.rollback()
        db.session.flush()
        logger = Logger.query.filter_by(sn=sn).first()
        if logger:
            if logger.tenant_id:
                # check if sampling exist
                sampling = datetime.datetime.fromtimestamp(raw.get('sampling'))
                up_since = datetime.datetime.fromtimestamp(raw.get('up_since'))
                check_periodik = Periodik.query.filter_by(sampling=sampling, logger_sn=logger.sn).first()
                if check_periodik:
                    return f"Logger {logger.sn}, Exception : Periodik with sampling {sampling} already exist"

                # insert data
                try:
                    new_periodik = Periodik(
                        logger_sn=sn,
                        location_id=logger.location_id or None,
                        tenant_id=logger.tenant_id,
                        mdpl=raw.get('altitude') or None,
                        apre=raw.get('pressure') or None,
                        sq=raw.get('signal_quality') or None,
                        temp=(raw.get('temperature') + logger.temp_cor) if raw.get('temperature') and logger.temp_cor else raw.get('temperature'),
                        humi=(raw.get('humidity') + logger.humi_cor) if raw.get('humidity') and logger.humi_cor else raw.get('humidity'),
                        batt=(raw.get('battery') + logger.batt_cor) if raw.get('battery') and logger.batt_cor else raw.get('battery'),
                        rain=(raw.get('tick') * (logger.tipp_fac or 0.2)) if raw.get('tick') else None,
                        wlev=((logger.ting_son or 100) - (raw.get('distance') * 0.1)) if raw.get('distance') else None,
                        sampling=datetime.datetime.fromtimestamp(raw.get('sampling')),
                        up_s=datetime.datetime.fromtimestamp(raw.get('up_since')),
                        ts_a=datetime.datetime.fromtimestamp(raw.get('time_set_at')),
                    )
                    content = Raw(content=raw)

                    db.session.add(content)
                    db.session.add(new_periodik)
                    # db.session.flush()
                    db.session.commit()
                    return f"Logger {logger.sn} data recorded, up since {up_since}"  # on {logger.location.nama}
                except Exception as e:
                    db.session.rollback()
                    db.session.flush()
                    return f"Logger {logger.sn}, Exception (while trying to record data) : {e}"
            else:
                return f"({sn}), Exception : Logger 'tenant_id' not set."
        else:
            return f"({sn}), Exception : Logger data not found in database."
    except Exception as e:
        return f"({sn}) Errors : {e}"


def raw2periodic(raw):
    '''Menyalin data dari Raw ke Periodik'''
    sn = raw.get('device').split('/')[1]
    session = db.session
    session.rollback()
    device = session.query(Device).filter_by(sn=sn).first()
    obj = {'device_sn': device.sn, 'lokasi_id': device.lokasi.id if
           device.lokasi else None}
    if raw.get('tick'):
        rain = (device.tipp_fac or 0.2) * raw.get('tick')
        obj.update({'rain': rain})
    if raw.get('distance'):
        # dianggap distance dalam milimeter
        # 'distance' MB7366(mm) di centimeterkan
        wlev = (device.ting_son or 100) - raw.get('distance') * 0.1
        obj.update({'wlev': wlev})
    time_to = {'sampling': 'sampling',
               'up_since': 'up_s',
               'time_set_at': 'ts_a'}
    direct_to = {'altitude': 'mdpl',
                 'signal_quality': 'sq',
                 'pressure': 'apre'}
    apply_to = {'humidity': 'humi',
                'temperature': 'temp',
                'battery': 'batt'}
    for k, v in time_to.items():
        obj.update({v: datetime.datetime.fromtimestamp(raw.get(k))})
    for k, v in direct_to.items():
        obj.update({v: raw.get(k)})
    for k, v in apply_to.items():
        if k in raw:
            corr = getattr(device, v + '_cor', 0) or 0
            obj.update({v: raw.get(k) + corr})

    try:
        d = Periodik(**obj)
        db.session.add(d)
        device.update_latest()
        if device.lokasi:
            device.lokasi.update_latest()
        db.session.commit()
    except IntegrityError:
        print(obj.get('device_sn'), obj.get('lokasi_id'), obj.get('sampling'))
        db.session.rollback()


if __name__ == '__main__':
    import datetime
    tgl = datetime.date(2018,12,20)
    print(persentase_hadir_data(tgl))
