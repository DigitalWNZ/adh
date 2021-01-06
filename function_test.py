from flask_restful import request,Resource
from adh_full_process import main_function
class function_test(Resource):
    def post(self):
        main_function(request)