def func(**args):
    if args:
        range_min = 0
        range_max = 100
        param = args.popitem()[1] # args = {'some-identity':{'some-name':'some-value'}, ...}
        if type(param) == dict:
            val = float(param.popitem()[1])
            if val >= range_min and val <= range_max:
                return {'enable':'true'}
