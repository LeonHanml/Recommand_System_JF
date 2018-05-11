#!/bin/python
import os
from flask import Flask, request
import requests
import json
import sys

from RecommendSystem import RecommendSystem


reload(sys)
sys.setdefaultencoding('utf-8')

app = Flask(__name__)


@app.route('/recommend')
def recommend():
	user_id = request.args.get("user_id")
	once_return_count = request.args.get("size")

	if user_id and not user_id.isdigit():
		return_obj = {
			"status": 1001,
			"msg": "invalid user_id",
			"data": None
		}
	elif once_return_count and not once_return_count.isdigit():
		return_obj = {
			"status": 1001,
			"msg": "invalid user_id",
			"data": None
		}		
	else: 
		if user_id is None:
			user_id = -1
		if once_return_count is None:
			once_return_count = 100
		pno_list = RecommendSystem.recommend_pno_for_user(user_id, once_return_count)
		return_obj = {
			"status": 1000,
			"msg": "success",
			"data":{
				"length": len(pno_list),
				"product_no_list": pno_list
			}
		}
	return json.dumps(return_obj)

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=4396)
