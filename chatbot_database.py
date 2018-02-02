import sqlite3
import json
from datetime import datetime

# ###STEP1: construct database with sql n stuf #######
#definition for the dataset to be used 
timeframe = '2012-01'

sql_transaction = []

connection = sqlite3.connect('{}.db'.format(timeframe))

c = connection.cursor()

def create_table():
	c.execute("""CREATE TABLE IF NOT EXISTS parent_reply(
				parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE,
				parent TEXT, comment TEXT, subreddit TEXT, 
				unix INT, score INT)""")
				
def format_data(data):
	data = data.replace("\n", " newlinechar ")
	data = data.replace("\r", " newlinechar ")
	data = data.replace('"', "'")
	return data
	
def find_parent(pid):
	
	try:
		query = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
		c.execute(query)
		result = c.fetchone()
		#print("parent found, or soemthing")
		if result != None:
			return result[0];
		else: return False 
	except Exception as e:
		print("find_parent", e)
		return False 
		
def find_existing_score(pid):
	try:
		query = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
		c.execute(query)
		result = c.fetchone()
		if result != None:
			return result[0];
		else: return False 
	except Exception as e:
		print("find_existing_score", e)
		return False

def acceptable(comment):
	if len(comment.split(' ')) > 50 or len(comment) < 1 or len(comment) > 1000:
		return False 
	elif comment == '[deleted]' or comment == '[removed]':
		return False
	else: return True

def sql_insert_replace_comment(comment_id, parent_id, parent_data, comment, subreddit, score, created_utc):
	try:
		sql = """UPDATE parent_reply SET comment_id = ?, parent_id = ?, parent_data = ?,
			comment = ?, subreddit = ?, score = ?, created_utc = ? WHERE parent_id = ?""".format(comment_id, parent_id, parentdata, comment, subreddit, score, int(created_utc), parent_id)
		transaction_bldr(sql)
	except Exception as e:
		print("sql_insert_replace_comment", str(e))

def sql_insert_has_parent(comment_id, parent_id, parent_data, comment, subreddit, score, created_utc):
	try:
		sql = """INSERT INTO parent_reply SET comment_id = ?, parent_id = ?, parent_data = ?,
			comment = ?, subreddit = ?, score = ?, created_utc = ?""".format(comment_id, parent_id, parentdata, comment, subreddit, score, int(created_utc))
		transaction_bldr(sql)
	except Exception as e:
		print("sql_insert_has_parent", str(e))

def sql_insert_no_parent(comment_id, parent_id, comment, subreddit, score, created_utc):
	try:
		sql = """INSERT INTO parent_reply SET comment_id = ?, parent_id = ?, comment = ?,
			subreddit = ?, score = ?, created_utc = ?""".format(comment_id, parent_id, comment, subreddit, score, int(created_utc))
		transaction_bldr(sql)
	except Exception as e:
		print("sql_insert_no_parent", str(e))
		
def transaction_bldr(sql):
	global sql_transaction
	sql_transaction.append(sql)
	if len(sql_transaction) > 1000:
		c.execute('BEGIN TRANSACTION')
		for s in sql_transaction:
			try:
				c.execute(s)
			except:
				pass
			connection.commit()
			sql_transaction = []
			
if __name__ == "__main__":
	create_table()
	row_counter = 0
	paired_rows = 0 # comments with a reply
	inserts = 0
		
	with open("D:/chatbot/data/RC_2012-01", buffering=1000) as f:
		for row in f:
			row_counter += 1 
			row = json.loads(row)
			#for k in row:
			#	if(k == 'author'):
			#		print(k, " ",row[k])
			
			#extract json-stuff
			parent_id = row['parent_id']
			comment_id = row['name']
			comment = format_data(row['body'])
			created_utc = row['created_utc']
			subreddit = row['subreddit']
			score = row['score']
			parent_data = find_parent(parent_id)
			
			#check if data is ok
			if score >= 2:
				existing_comment_score = find_existing_score(parent_id)
				if existing_comment_score:
					if score > existing_score and acceptable(comment):
						###update database entry 
						sql_insert_replace_comment(comment_id, parent_id, parent_data, comment, subreddit, score, created_utc)
						inserts += 1
				else:
					if acceptable(comment):
						if parent_data : 
							sql_insert_has_parent(comment_id, parent_id, parent_data, comment, subreddit, score, created_utc)
							inserts += 1
							paired_rows += 1
						else: sql_insert_no_parent(comment_id, parent_id, comment, subreddit, score, created_utc)
			
			if row_counter % 100000 == 0:
                #print('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(row_counter, paired_rows, str(datetime.now())))
				print("read 100K rows, nmbr pairs: ", paired_rows, "nmbr inserts: ", inserts)
			