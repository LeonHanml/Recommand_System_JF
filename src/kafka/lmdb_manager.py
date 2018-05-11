import lmdb


def initialize():
    env = lmdb.open("/app/luzhicong/recommend_system/data/user_behavior.lmdb", map_size=1e14)
    return env

def insert(env, key, value):
    txn = env.begin(write = True)
    txn.put(str(key), value)
    txn.commit()

def update(env, key, value):
    txn = env.begin(write = True)
    txn.put(str(key), value)
    txn.commit()

def search(env, key):
    txn = env.begin()
    name = txn.get(str(key))
    return name

def delete(env, sid):
    txn = env.begin(write = True)
    txn.delete(str(sid))
    txn.commit()

def display(env):
    txn = env.begin()
    cur = txn.cursor()
    for key, value in cur:
        print (key, value)

env = initialize()
