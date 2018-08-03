#!/usr/bin/python
# -*- coding: utf-8 -*-

from flask import Flask, request, json, jsonify, abort
import json
from datetime import date, datetime
import decimal
from functools import wraps
import logic
import traceback
from config import CONFIGURATION as config
from sql import SQL
import db
import sender
import functools


app = Flask(__name__)


class MyJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            # Convert decimal instances to strings.
            return str(obj)
        return super(MyJSONEncoder, self).default(obj)


app.json_encoder = MyJSONEncoder


def auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Token', None)
        # print(request)
        if (not token) or (token != TOKEN):
            abort(401)
        return f(*args, **kwargs)
    return decorated_function


@app.route('/phone/<phone>', methods=['GET'])
@auth
def get_phone(phone):
    result = db.fetch(SQL['lands_by_phone'], {'phone': phone})
    return jsonify(result)


@app.route('/phone/<phone>/periods', methods=['GET'])
@auth
def get_periods_by_phone(phone):
    result = db.fetch(SQL['periods_by_phone'], {'phone': phone})
    return jsonify(result)


@app.route('/phone/<phone>/subscribed', methods=['GET'])
@auth
def get_subscribed_by_phone(phone):
    srv = sender.Sender(config['sender'])
    contact = sender.Contact(srv, phone)
    return jsonify(contact.active)


@app.route('/phone/<phone>/subscribe', methods=['GET'])
@auth
def get_subscribe_by_phone(phone):
    srv = sender.Sender(config['sender'])
    contact = sender.Contact(srv, phone)
    contact.subscribe()
    return jsonify(contact.active)


@app.route('/phone/<phone>/unsubscribe', methods=['GET'])
@auth
def get_unsubscribe_by_phone(phone):
    srv = sender.Sender(config['sender'])
    contact = sender.Contact(srv, phone)
    contact.unsubscribe()
    return jsonify(contact.active)


@app.route('/phone/<phone>/period/<period>', methods=['GET'])
@auth
def get_land_phone_period(phone, period):
    result = db.fetch(SQL['period_by_phone'],
                      {'phone': phone, 'period': period}
                      )
    return jsonify(result)


@app.route('/phone/<phone>/factperiod/<period>', methods=['GET'])
@auth
def get_land_phone_factperiod(phone, period):
    result = db.fetch(SQL['period_by_phone'],
                      {'phone': phone, 'period': period}
                      )
    return jsonify(result)


@app.route('/land/<land>/period/<period>', methods=['GET'])
@auth
def get_land_period(period, land):
    result = db.fetch(SQL['periods_by_land'],
                      {'land': land, 'period': period}
                      )
    return jsonify(result)


@app.route('/land/<land>/period/<period>/charge', methods=['GET'])
@auth
def get_land_period_charge(period, land):
    result = db.fetch(SQL['charge_by_land'],
                      {'land': land, 'period': period}
                      )
    return jsonify(result)


@app.route('/land/<land>/period/<period>/payment', methods=['GET'])
@auth
def get_land_period_payment(period, land):
    result = db.fetch(SQL['payment_by_land'],
                      {'land': land, 'period': period}
                      )
    return jsonify(result)


@app.route('/charge/<period>/<land>', methods=['GET'])
@auth
def get_land_charge(period, land):
    result = db.fetch(SQL['charge_by_land'],
                      {'land': land, 'period': period}
                      )
    return jsonify(result)


@app.route('/charge/<period>', methods=['GET'])
@auth
def get_charge(period):
    result = db.fetch(SQL['charge_in_period'],
                      {'period': period}
                      )
    return jsonify(result)


@app.route('/phone/<phone>/profile', methods=['GET'])
@auth
def get_user_info_by_phone(phone):
    result = logic.compactProfile(
        db.fetch(SQL['user_info_by_phone'],
                 {'phone': phone}
                 )
    )
    return jsonify(result)


@app.route('/smsusers', methods=['GET'])
@auth
def users_for_sms():
    result = db.fetch(SQL['users_for_sms']
                      )
    return jsonify(result)


@app.route('/sendersync', methods=['GET'])
@auth
def sync_users_for_sms():
    s = sender.Sender()
    # print(res)
    res = db.fetch(SQL['users_for_sms'])
    contacts = map(lambda x:
                   sender.Contact(s,
                                  email=x.get('email'),
                                  phone='+' + x.get('phone'),
                                  name=' '.join(
                                      [x.get('last_name', ''), x.get('io_name', '')])
                                  ),
                   res)
    result = (s.saveContacts(contacts))
    return jsonify(result)


@app.route('/maintanance', methods=['POST'])
@auth
def maintance():
    print('maintanance')
    return jsonify(logic.maintenanceAccurals())


@app.route('/', methods=['GET'])
def get_tasks():
    result = db.test_db()
    return jsonify(result)


@app.route('/payments', methods=['POST'])
@auth
def post_payments():
    result = None
    data = request.get_json(force=True)
    result = logic.upsert_payment(data)
    # except Exception as e:
    #     print(e)
    #     traceback.print_stack()
    #     # result = {'ERROR'}
    return jsonify(result)


TOKEN = config.get('TOKEN', None)
if not (TOKEN):
    print('WARNING: TOKEN is not found in config.py')


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
