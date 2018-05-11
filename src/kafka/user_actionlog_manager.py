from lmdb_manager import *

MAX_LENGTH = 20

def insert_user_action(key, product_no):
    origin_actions = search(env, key)
    if origin_actions is None:
        insert(env, key, product_no)
    else:
        action_list = origin_actions.split(',')
        length = len(action_list)
        if length >= 20:
            action_list = action_list[-19:]
        action_list.append(product_no)
        new_actions = ','.join(action_list)
        update(env, key, new_actions)



