
from airflow.hooks.mysql_hook import MySqlHook

mysql_hook = MySqlHook(mysql_conn_id="my_mysql_conn")
df = mysql_hook.get_pandas_df(sql="SELECT * FROM users")

print(df)