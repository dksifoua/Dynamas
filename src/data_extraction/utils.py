import base64
import pandas as pd

def get_batch(n, nb_col): 
    return n * nb_col

def to_datetime(timestamp):
    return pd.to_datetime(timestamp, unit='ms').tz_localize('UTC').tz_convert('US/Eastern')

def to_timestamp(datetime):
    timestamp = lambda x: pd.Timestamp(x, tz='US/Eastern')
    if isinstance(datetime, str):
        datetime = timestamp(datetime)
    return int(datetime.timestamp() * 1000)

def decode(x):
    return base64.b64decode(x).decode('utf-8')

def encode(x):
    return base64.b64encode(x).decode('utf-8')

def extract_json(json_data):
    data = {}
    
    for _, v in json_data.items():
        for dico in v:
            for cell in dico['Cell']:
                try:
                    data[cell['column']].append((cell['$'], cell['timestamp'], dico['key']))
                except:
                    data[cell['column']] = [(cell['$'], cell['timestamp'], dico['key'])]
    
    data = pd.DataFrame.from_dict(data)
    data.columns = map(decode, data.columns)
    data.columns = map(lambda x: x.split(':')[1], data.columns)
    
    # J'extrais le timestamp & key des cellules de chaque ligne pour les mettre dans de nouvelles colonnes
    ts = data.apply(lambda x: list(map(lambda x_: x_[1], x.values)), axis=1).apply(set).apply(list).apply(lambda x: x[0]).values
    ks = data.apply(lambda x: list(map(lambda x_: x_[2], x.values)), axis=1).apply(set).apply(list).apply(lambda x: x[0]).apply(decode).values
    
    data = data.applymap(lambda x: x[0]).applymap(decode)
    
    data['key'] = ks
    data['timestamp'] = ts
    data = data.sort_values(by=['timestamp'])
    
    return data

def build_filter(filter_type, operator, comparator_type, comparator_value, family=None, qualifier=None):
    f = ''
    f += '{'
    f += '	"type": "{}",'.format(filter_type)
    f += '  "op": "{}",'.format(operator)
    if family != None and qualifier != None:
        f += '"family": "{}",'.format(family)
        f += '"qualifier": "{}",'.format(qualifier)
    f += '	"comparator": {'
    f += '		"type": "{}",'.format(comparator_type)
    f += '		"value": "{}"'.format(comparator_value)
    f += '  }'
    f += '}'
    return f

def build_xml(batch, filters=[], start_time=None, end_time=None):
    nb_filters = len(filters)
    xml = ''
    xml += '<?xml version="1.0" encoding="utf-8" ?>'
    if start_time == None or end_time == None:
        xml += '<Scanner batch="{}">'.format(batch)
    else:
        xml += '<Scanner batch="{}" startTime="{}" endTime="{}">'.format(batch, start_time, end_time)
    if nb_filters > 0:
        xml += '<filter>'
        xml += '	{'
        xml += '		"type": "FilterList",'
        xml += '		"op": "MUST_PASS_ALL",'
        xml += '		"filters": ['
        for i in range(nb_filters):
            xml += filters[i]
            if i != nb_filters - 1:
                xml += ','
        xml += '		]'
        xml += '	}'
        xml += '</filter>'
    xml += '</Scanner>'
    
    return xml