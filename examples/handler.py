def func(args):
    r = (0, 100)
    real = args.popitem()[1]
    val = float(real.popitem()[1])
    if val >= r[0] and val <= r[1]:
        return {"Enable":True}
