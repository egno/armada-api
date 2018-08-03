import requests
import db
import json
from config import CONFIGURATION as config


def params_to_uri(params):
    return '&'.join(list(param + '=' + params[param] for param in params if not params[param] is None))


def list_to_params(paramName, x, result={}):
    if type(x) is list:
        for i, item in enumerate(x):
            list_to_params('%s[%s]' % (paramName, i), item, result)
    else:
        result[paramName] = x
    return result


def partList(myList, count):
    return myList[:count], myList[count:]


class Sender:

    def __init__(self, config=config.get('sender')):
        self.url = config['url']
        self.key = config['key']
        self.maxRowsToSend = 10
        self.lists = self.get_result(
            'getLists', {'format': 'json', 'api_key': self.key})

    def resut_to_dict(self, res):
        field_names = res['field_names']
        data = res['data']
        return [{x: row[i] for i, x in enumerate(field_names)} for row in data]

    def call_method(self, method, params):
        string_params = params_to_uri(params)
        url = '%s%s?%s' % (self.url, method, string_params)
        print(url)
        req = requests.get(url)
        result = req.json()
        return(result)

    def get_result(self, method, params):
        result = self.call_method(method, params)['result']
        return(result)

    def getContact(self, phone):
        res = self.get_result(
            'exportContacts', {'format': 'json', 'api_key': self.key, 'phone': phone})
        return(self.resut_to_dict(res))

    def subscribe(self, phone):
        list_ids = ','.join([str(x['id']) for x in self.lists])
        res = self.call_method('subscribe', {
                               'format': 'json', 'api_key': self.key, 'list_ids': list_ids, 'fields[phone]': phone})
        print(res)
        return(res)

    def unsubscribe(self, phone):
        res = self.call_method('exclude', {
                               'format': 'json', 'api_key': self.key, 'contact_type': 'phone', 'contact': phone})
        print(res)
        return(res)

    def saveContacts(self, contacts):

        def savePart(data):
            params = {'format': 'json', 'api_key': self.key}
            list_to_params('field_names', field_names, params)
            list_to_params('data', data, params)
            res = self.call_method('importContacts', params)
            return(res)

        listString = ','.join([str(l.get('id')) for l in self.lists])
        data = [[contact.name, contact.phone, contact.email,
                 listString, listString] for contact in contacts]
        field_names = ['Name', 'phone', 'email',
                       'phone_list_ids', 'email_list_ids']

        result = []
        while data:
            currentList, data = partList(data, self.maxRowsToSend)
            result.append(savePart(currentList))

        return(result)


class Contact:

    def __init__(self, sender, **kwargs):
        self.sender = sender
        self.phone = kwargs.get('phone')
        self.email = kwargs.get('email')
        self.data = None
        self.emails = None
        self.active = None
        self.name = kwargs.get('name', ' '.join(
            [kwargs.get('last_name', ''), kwargs.get('io_name', '')]))
        if kwargs.get('load'):
            self.update_status()

    def update_status(self):
        self.data = self.sender.getContact(self.phone)
        self.emails = [{x['email']: {'status': x['email_status']}}
                       for x in self.data if x['email'] > '']
        if self.emails:
            self.email = self.emails[0]
        self.active = self.get_phone_status()

    def get_phone_status(self):

        stat = {
            'new': True,
            'active': True,
            'inactive': False,
            'unsubscribed': False,
            'blocked': False
        }

        statuses = [x['phone_status'] for x in self.data if x['phone'] > '']
        result = max([False] + [stat[x] for x in statuses])
        return(result)

    def subscribe(self):
        self.sender.subscribe(self.phone)
        self.update_status()

    def unsubscribe(self):
        self.sender.unsubscribe(self.phone)
        self.update_status()
