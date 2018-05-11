import os
import MySQLdb
import word2vec
import lmdb
from ConfigParser import SafeConfigParser

def _update_pno_sort_database(mysql_conn, lmdb_file):
	cursor = mysql_conn.cursor()
	cursor.execute('select ')



def embedding_product_no(dimension=64, conf_file='recommend_system.conf'):
	"""
	
	:param dimension: 
	:param conf_file: 
	:return: 
	"""
	config = SafeConfigParser()
	path = os.path.join(os.path.dirname(os.path.abspath(__file__)), conf_file)
	config.read(path)

	recommend_system_actionlog ='/app/luzhicong/recommend_system/data/actionlog.`txt'
	recommend_system_product_no_emb_bin ='/app/luzhicong/recommend_system/data/product_no_emb.bin'

    # Train item2vec
	word2vec.word2vec(recommend_system_actionlog, recommend_system_product_no_emb_bin, size=dimension, verbose=True)

	model = word2vec.WordVectors.from_binary(recommend_system_product_no_emb_bin, encoding="ISO-8859-1")

	print len(model.vocab)
	print model.vocab
	m, d = model.vectors.shape

	fout = open('/app/luzhicong/recommend_system/data/product_no_embedding_result.txt', 'w')

	print >>fout, m, d

	env_product_no_idx = lmdb.open('/app/luzhicong/recommend_system/data/product_no_idx.lmdb', map_size=1e12)
	print env_product_no_idx.stat()
	txn = env_product_no_idx.begin(write=True)
	txn.drop(db=env_product_no_idx.open_db())

	for i in range(m):
		print >>fout, model.vocab[i].encode('ISO-8859-1'), '\t',
		for j in range(d):
			print >>fout, model.vectors[i][j],
			#txn.put(model.vocab[i].encode('ISO-8859-1'), str(i))
		txn.put(model.vocab[i].encode('ISO-8859-1'), str(i))
		print >>fout

	print txn.stat()
	txn.commit()
	env_product_no_idx.close()

	fin_user_behavior = open('/app/luzhicong/recommend_system/data/user_behavior.txt', 'r')
	env_user_behavior = lmdb.open('/app/luzhicong/recommend_system/data/user_behavior.lmdb', map_size=1e12)
	txn = env_user_behavior.begin(write=True)
	txn.drop(db=env_user_behavior.open_db())
	line = fin_user_behavior.readline()
	while line:
		seq = line.strip().split(',')
		if len(seq)>1:
			txn.put(seq[0], ','.join(seq[1:]))
		line = fin_user_behavior.readline()
	txn.commit()
	print env_user_behavior.stat()
	env_user_behavior.close()


if __name__ == '__main__':
	embedding_product_no()
