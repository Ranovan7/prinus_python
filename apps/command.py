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

from telegram import Bot

from apps import app, db
from apps.models import Logger, Raw, Tenant, Periodik

bws_sul2 = ("bwssul2", "limboto1029")

URL = "https://prinus.net/api/sensor"
MQTT_HOST = "mqtt.bbws-bsolo.net"
MQTT_PORT = 14983
MQTT_TOPICS = "sensors"
MQTT_CLIENT = None

logging.basicConfig(
        filename='/tmp/pbasemqttsub.log',
        level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')


@app.cli.command()
@click.argument('command')
def telegram_warning(command):
    # tgl = datetime.date.today() - datetime.timedelta(days=1)
    if command == 'test':
        print("Sending Test Message")
        message = "Testing Telegram Bot"
        reports = tenants_report()
        message += f"\n{reports}"
        bot = Bot(token=app.config['PRINUSBOT_TOKEN'])
        bot.sendMessage(app.config['TELEGRAM_TEST_ID'],
                        text=message)
    elif command == 'info':
        print("Testing gathering data")
        bot = Bot(token=app.config['PRINUSBOT_TOKEN'])
        res = bot.sendMessage(app.config['TELEGRAM_TEST_ID'], text="Testing")
        print(res)
    # elif command == 'info_ch':
    #     # info = info_ch()
    #     bot = Bot(token=app.config.get('PRINUSBOT_TOKEN'))
    #     bot.sendMessage(app.config.get('TELEGRAM_TEST_CHANNEL'),
    #                     text="Testing",
    #                     parse_mode='Markdown')


def tenants_report():
    # time = datetime.datetime.now()
    time = datetime.datetime.strptime("2020-01-12 11:00:00", "%Y-%m-%d %H:%M:%S")
    start = datetime.datetime.strptime(f"{time.year}-{time.month}-{time.day} {time.hour - 1}:00:00", "%Y-%m-%d %H:%M:%S")
    end = datetime.datetime.strptime(f"{time.year}-{time.month}-{time.day} {time.hour - 1}:55:00", "%Y-%m-%d %H:%M:%S")
    result = f"Range ({start}) to ({end})"

    periodik_result = {}
    periodics = Periodik.query.filter(Periodik.sampling.between(start, end))
    for period in periodics:
        if period.logger.tenant.nama not in periodik_result:
            periodik_result[period.logger.tenant.nama] = {}
        if period.logger_sn not in periodik_result[period.logger.tenant.nama]:
            periodik_result[period.periodik_tenant.nama][period.logger_sn] = 0
        periodik_result[period.periodik_tenant.nama][period.logger_sn] += round(period.rain or 0, 2)

    for ten, loc_rain in periodik_result.items():
        result += f"\n{ten}"
        for loc, rain in loc_rain.items():
            if rain > 0:
                result += f"\n----{loc} : {round(rain, 2)} mm"
    return result


@app.cli.command()
@click.argument('command')
def telegram(command):
    tgl = datetime.date.today() - datetime.timedelta(days=1)
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
                        temp=(raw.get('temperature') + logger.temp_cor) if raw.get('temperature') and logger.temp_cor else 0,
                        humi=(raw.get('humidity') + logger.humi_cor) if raw.get('humidity') and logger.humi_cor else 0,
                        batt=(raw.get('battery') + logger.batt_cor) if raw.get('battery') and logger.batt_cor else 0,
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
