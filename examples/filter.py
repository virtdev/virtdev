def func(args):
    if args and type(args) == dict:
        k = args.keys()[0]
        return {k:args[k]}
