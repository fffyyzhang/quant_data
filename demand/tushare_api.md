# 获取全市场当日不复权数据
df = pro.daily(trade_date='20250826')
返回数据：
	ts_code	trade_date	open	high	low	close	pre_close	change	pct_chg	vol	amount
0	000001.SZ	20250826	12.45	12.50	12.30	12.36	12.45	-0.09	-0.7229	1383598.61	1709560.923
1	000002.SZ	20250826	6.98	7.03	6.90	6.99	7.16	-0.17	-2.3743	4379464.06	3051239.669

# 获取当日全市场后复权因子
df = pro.adj_factor( trade_date='20250826')
返回数据：
	ts_code	trade_date	adj_factor
0	000001.SZ	20250826	131.7878
1	000002.SZ	20250826	181.7040


