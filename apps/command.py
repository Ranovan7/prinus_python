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
from apps.models import Logger, Raw, Tenant, Periodik

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
    '2': "TMA",
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
    if time.hour < 7:
        res = datetime.datetime.strptime(f"{time.year}-{time.month}-{time.day - 1} 07:00:00", "%Y-%m-%d %H:%M:%S")
    else:
        res = datetime.datetime.strptime(f"{time.year}-{time.month}-{time.day} 07:00:00", "%Y-%m-%d %H:%M:%S")
    return res


@app.cli.command()
@click.argument('command')
def telegram(command):
    time = datetime.datetime.now()
    # time = datetime.datetime.strptime("2020-01-09 11:00:00", "%Y-%m-%d %H:%M:%S")
    if command == 'test':
        print(persentase_hadir_data(tgl))
    elif command == 'info_ch':
        info = info_ch()
        bot = Bot(token=app.config.get('BWSSUL2BOT_TOKEN'))
        bot.sendMessage(app.config.get('BWS_SUL2_TELEMETRY_GROUP'),
                        text=info,
                        parse_mode='Markdown')
    elif command == 'info_tma':
        info = info_tma()
        bot = Bot(token=app.config.get('BWSSUL2BOT_TOKEN'))
        bot.sendMessage(app.config.get('BWS_SUL2_TELEMETRY_GROUP'),
                        text=(info),
                        parse_mode='Markdown')
    elif command == 'send':
        bot = Bot(token=app.config.get('BWSSUL2BOT_TOKEN'))
        bot.sendMessage(app.config.get('BWS_SUL2_TELEMETRY_GROUP'),
                        text=(persentase_hadir_data(tgl)),
                        parse_mode='Markdown')
    elif command == 'periodik':
        print("Sending Periodik Info")
        periodik_report(time)
    elif command == 'count':
        print("Sending Today's Periodik Count")
        periodik_count_report(time)
    elif command == 'warning':
        print("Sending Alert Message")
        rain_alert(time)


def periodik_report(time):
    ''' Message Tenants about last 2 hour rain and water level '''
    bot = Bot(token=app.config['PRINUSBOT_TOKEN'])

    ch_report(time, bot)
    tma_report(time, bot)

    # bot.sendMessage(app.config['TELEGRAM_TEST_ID'], text="Sending 2-Hourly Reports to All Tenants")


def ch_report(time, bot):
    periodik_result = {}
    tenants = Tenant.query.order_by(Tenant.id).all()

    for ten in tenants:
        tz = ten.timezone or "Asia/Jakarta"
        localtime = utc2local(time, tz=tz)
        end = datetime.datetime.strptime(f"{localtime.strftime('%Y-%m-%d')} {localtime.hour}:00:00", "%Y-%m-%d %H:%M:%S")
        start = getstarttime(end)

        periodik_result[ten.nama] = {
            'logger': {},
            'start': start,
            'end': end,
            'telegram_group': ten.telegram_info_group,
            'telegram_id': ten.telegram_info_id
        }

        loggers = Logger.query.filter(
                                    Logger.tipe == 'arr',
                                    Logger.tenant_id == ten.id).all()
        for log in loggers:
            location_name = log.location.nama if log.location else f"Lokasi {log.sn}"
            periodik_result[ten.nama]['logger'][location_name] = 0

        periodics = Periodik.query.filter(
                                    Periodik.sampling.between(local2utc(start), local2utc(end)),
                                    Periodik.rain > 0,
                                    Periodik.tenant_id == ten.id).all()

        for period in periodics:
            periodik_result[period.periodik_tenant.nama]['logger'][location_name] += period.rain

    for ten, info in periodik_result.items():
        final = f"*Curah Hujan {info['start'].strftime('%d %b %Y')}*\n"
        final += f"({info['start'].strftime('%H:%M')}) - ({info['end'].strftime('%H:%M')})\n"
        message = ""
        i = 0
        for name, count in info['logger'].items():
            i += 1
            message += f"\n{i}. {name} : {round(count, 2)} mm"
        if message:
            final += message
        try:
            bot.sendMessage(info['telegram_id'], text=final)
            logging.debug(f"TeleRep-send to {ten}")
        except Exception as e:
            logging.debug(f"TeleRep-send Error ({ten}) : {e}")
        print(final)
        print()


def tma_report(time, bot):
    periodik_result = {}
    loggers = Logger.query.filter(Logger.tipe == 'awlr').order_by(Logger.id).all()

    for log in loggers:
        location_name = log.location.nama if log.location else f"Lokasi {log.sn}"
        if log.tenant and log.tenant.nama not in periodik_result:
            periodik_result[log.tenant.nama] = {
                'logger': {},
                'telegram_group': log.tenant.telegram_info_group,
                'telegram_id': log.tenant.telegram_info_id
            }

        latest = Periodik.query.filter(Periodik.logger_sn == log.sn).order_by(desc(Periodik.sampling)).first()
        if log.tenant and location_name not in periodik_result[log.tenant.nama]['logger']:
            if latest:
                sample = latest.sampling.strftime('%H:%M %d %b %Y')
                periodik_result[log.tenant.nama]['logger'][location_name] = f"{latest.wlev or '-'}m, pada {sample}"
            else:
                periodik_result[log.tenant.nama]['logger'][location_name] = "Belum Ada Data"

    for ten, info in periodik_result.items():
        final = f"*TMA*\n"
        message = ""
        i = 0
        for name, count in info['logger'].items():
            i += 1
            message += f"\n{i}. {name} : {count}"
        if message:
            final += message
        try:
            bot.sendMessage(info['telegram_id'], text=final)
            logging.debug(f"TeleRep-send to {ten}")
        except Exception as e:
            logging.debug(f"TeleRep-send Error ({ten}) : {e}")
        print(final)
        print()


def periodik_count_report(time):
    ''' Message Tenants about last day periodic counts '''
    bot = Bot(token=app.config['PRINUSBOT_TOKEN'])

    periodik_result = {}
    tenants = Tenant.query.order_by(Tenant.id).all()

    for ten in tenants:
        # param tz should be entered if tenant have timezone
        # log.tenant.timezone
        tz = ten.timezone or "Asia/Jakarta"
        localtime = utc2local(time, tz=tz)
        end = datetime.datetime.strptime(f"{localtime.year}-{localtime.month}-{time.day - 1} 23:56:00", "%Y-%m-%d %H:%M:%S")
        start = datetime.datetime.strptime(f"{localtime.year}-{localtime.month}-{time.day - 1} 00:00:00", "%Y-%m-%d %H:%M:%S")

        periodik_result[ten.nama] = {
            'logger': {
                'Klimatologi': {},
                'Hujan': {},
                'TMA': {},
                'Lain': {}
            },
            'telegram_group': ten.telegram_info_group,
            'telegram_id': ten.telegram_info_id
        }

        loggers = Logger.query.filter(Logger.tenant_id == ten.id).all()
        for log in loggers:
            location_name = log.location.nama if log.location else f"Lokasi {log.sn}"
            pos_tipe = POS_NAME[log.location.tipe] if log.location and log.location.tipe else "Lain"

            periodik_result[ten.nama]['logger'][pos_tipe][location_name] = 0

        periodics = Periodik.query.filter(
                                    Periodik.sampling.between(local2utc(start), local2utc(end)),
                                    Periodik.tenant_id == ten.id).all()

        for period in periodics:
            location_name = period.logger.location.nama if period.logger.location else f"Lokasi {period.logger.sn}"
            pos_tipe = POS_NAME[period.logger.location.tipe] if period.logger.location and period.logger.location.tipe else "Lain"

            periodik_result[ten.nama]['logger'][pos_tipe][location_name] = 1

    for ten, info in periodik_result.items():
        final = '''*%(ten)s*\n*Kehadiran Data*\n%(tgl)s (0:0 - 23:55)
        ''' % {'ten': ten, 'tgl': start.strftime('%d %b %Y')}
        for tipe, pos in info['logger'].items():
            message = ""
            i = 0
            all = 0
            for name, count in pos.items():
                percent = round((count/288) * 100, 2)
                message += f"\n- {name} : {percent}%\n"
                i += 1
                all += percent
            avg = round(all/i, 2) if i else 0
            if message:
                final += f"\n# Pos {tipe} ({avg}%) \n"
                final += message
        try:
            logging.debug(f"TeleCount-send to {ten}")
            bot.sendMessage(info['telegram_id'], text=final)
        except Exception as e:
            logging.debug(f"TeleCount-send Error ({ten}) : {e}")
        print(final)
    bot.sendMessage(app.config['TELEGRAM_TEST_ID'], text="Sending Daily Count Reports to All Tenants")


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


def info_ch():
    ret = "*BWS Sulawesi 2*\n\n"
    ch = build_ch()
    ret += ch
    return ret


def info_tma():
    ret = "*BWS Sulawesi 2*\n\n"
    tma = build_tma()
    ret += tma
    return ret


def build_ch():
    now = datetime.datetime.now()
    dari = now.replace(hour=7, minute=0, second=0, microsecond=0)
    if now.hour < 7:
        dari -= datetime.timedelta(days=1)
    ret = "*Curah Hujan %s*\n" % (dari.strftime('%d %b %Y'))
    dari_fmt = dari.date() != now.date() and '%d %b %Y %H:%M' or '%H:%M'
    ret += "Akumulasi: %s sd %s (%.1f jam)\n\n" % (dari.strftime(dari_fmt),
                                                 now.strftime('%H:%M'),
                                                 (now - dari).seconds / 3600)
    i = 1
    for pos in Lokasi.query.filter(or_(Lokasi.jenis == '1', Lokasi.jenis ==
                                      '4')):
        ret += "%s. %s" % (i, pos.nama)
        j = 1
        durasi = 0
        ch = 0
        for p in Periodik.query.filter(Periodik.lokasi_id == pos.id,
                                       Periodik.rain > 0,
                                       Periodik.sampling > dari):
            durasi += 5
            ch += p.rain
        if ch > 0:
            ret += " *%.1f mm (%s menit)*" % (ch, durasi)
        else:
            ret += " _tidak hujan_"
        ret += "\n"
        i += 1
    return ret


def build_tma():
    ret = '\n*Tinggi Muka Air*\n\n'
    i = 1
    now = datetime.datetime.now()
    for pos in Lokasi.query.filter(Lokasi.jenis == '2'):
        ret += "%s. %s" % (i, pos.nama)
        periodik = Periodik.query.filter(Periodik.lokasi_id ==
                              pos.id, Periodik.sampling <= now).order_by(desc(Periodik.sampling)).first()
        ret +=  " *TMA: %.2f Meter* jam %s\n" % (periodik.wlev * 0.01,
                                  periodik.sampling.strftime('%H:%M %d %b %Y'))
        i += 1
    return ret


def persentase_hadir_data(tgl):
    out = '''*BWS Sulawesi 2*
*Kehadiran Data*
%(tgl)s (0:0 - 23:55)
''' % {'tgl': tgl.strftime('%d %b %Y')}
    pos_list = Lokasi.query.filter(Lokasi.jenis == '1')
    if pos_list.count():
        str_pos = ''
        j_data = 0
        i = 1
        for l in pos_list:
            banyak_data = Periodik.query.filter(Periodik.lokasi_id == l.id,
                                                func.DATE(Periodik.sampling) == tgl).count()
            persen_data = (banyak_data/288) * 100
            j_data += persen_data
            str_pos += '%s. %s ' % (i, l.nama + ': *%.1f%%*\n' % (persen_data))
            i += 1
        str_pos = '\n*Pos Hujan: %.1f%%*\n\n' % (j_data/(i-1)) + str_pos
        out += str_pos
    # end pos_hujan

    pos_list = Lokasi.query.filter(Lokasi.jenis == '2')
    if pos_list.count():
        str_pos = ''
        i = 1
        j_data = 0
        persen_data = 0
        for l in pos_list:
            banyak_data = Periodik.query.filter(Periodik.lokasi_id == l.id,
                                                func.DATE(Periodik.sampling) == tgl).count()
            persen_data = (banyak_data/288) * 100
            j_data += persen_data
            str_pos += '%s. %s ' % (i, l.nama + ': *%.1f%%*\n' % (persen_data))
            i += 1
        str_pos = '\n*Pos TMA: %.1f%%*\n\n' % (j_data/(i-1)) + str_pos
        out += str_pos
    # end pos_tma_list

    pos_list = Lokasi.query.filter(Lokasi.jenis == '4')
    if pos_list.count():
        str_pos = ''
        i = 1
        j_data = 0
        persen_data = 0
        for l in pos_list:
            banyak_data = Periodik.query.filter(Periodik.lokasi_id == l.id,
                                                func.DATE(Periodik.sampling) == tgl).count()
            persen_data = (banyak_data/288) * 100
            j_data += persen_data
            str_pos += '%s. %s ' % (i, l.nama + ': *%.1f%%*\n' % (persen_data))
            i += 1
            str_pos = '\n*Pos Klimatologi: %.1f%%*\n\n' % (j_data/(i-1)) + str_pos
        out += str_pos
    return out


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
