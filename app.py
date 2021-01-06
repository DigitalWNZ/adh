import sys
import os
import function_test
# sys.executable is used when using uWSGI to start flask server
sys.executable = 'python'

# add py4j.zip to sys.path
# if 'PY4J' in os.environ and 'SPARK_HOME' in os.environ:
#     sys.path.insert(0, os.environ['PY4J'])
#     sys.path.insert(0, os.environ['SPARK_HOME'] + "/python")
# else:
#     pass


from flask import Flask,jsonify
from flask_restful import request,Api,Resource
from function_test import function_test

import string,random


def endpoint(api):
    endpoint_prefix='/adh'
    api.add_resource(function_test,endpoint_prefix + '/test')

def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))

app=Flask(__name__)
app.secret_key=randomString(10)
api=Api(app)
endpoint(api)