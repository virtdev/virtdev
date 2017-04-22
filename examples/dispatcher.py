def func(**args):
	if args and len(args) >= 3:
		cnt = 0
		res = [[] for _ in range(3)]
		for i in args:
			res[cnt % 3].append({i:args[i]})
			cnt = cnt + 1
		return res
