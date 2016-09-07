def func(**args):
    if args:
        key = args.keys()[0]
        args = {key:args[key]}
    return args
