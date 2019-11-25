# -*- coding: utf-8 -*-
"""
Created on Sat Nov 23 14:54:54 2019

@author: John Jegard
"""

from flask import Flask, request, jsonify, make_response, abort
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
import os
from datetime import datetime
import iso8601



#Initialize app
app = Flask(__name__);
basedir = os.path.abspath(os.path.dirname(__file__));

#Config Database
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'db.sqlite');
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False;

#Initialize Database
db = SQLAlchemy(app);
#Initialize Marshmallow
ma = Marshmallow(app);



#Create class Log
class Log(db.Model):
    id = db.Column(db.Integer, primary_key=True);
    userId = db.Column(db.String(20), nullable=False);
    sessionId = db.Column(db.String(20), nullable=False);
    time = db.Column(db.DateTime, nullable=False);
    log_type = db.Column(db.String(10), nullable=False);
    locationX = db.Column(db.Integer, nullable=True);
    locationY = db.Column(db.Integer, nullable=True);
    viewedId = db.Column(db.String(20), nullable=True);
    pageFrom = db.Column(db.String(20), nullable=True);
    pageTo = db.Column(db.String(20), nullable=True);
    
    def __init__(self, userId, sessionId, time, log_type, locationX, locationY, viewedId, pageFrom, pageTo):
        self.userId = userId;
        self.sessionId = sessionId;
        self.time = time;
        self.log_type = log_type;
        self.locationX = locationX;
        self.locationY = locationY;
        self.viewedId = viewedId;
        self.pageFrom = pageFrom;
        self.pageTo = pageTo;

#Create class LogSchema
class LogSchema(ma.Schema):
    class Meta:
        fields = ('id', 'userId', 'sessionId', 'time', 'log_type', 'locationX', 'locationY', 'viewedId',\
                  'pageFrom', 'pageTo');

#Initialize Schema
log_schema = LogSchema(); #Used for a single log
logs_schema = LogSchema(many=True); #Used for many logs


#############THIS IS NOT USED###############################
#Boolean Function to determine if datetime is within the range of start time and end time
def time_within_range(test, start, end):
    if test >= start and test <= end:
        return True;
    return False;

#Routes POST methods
    


#Create a Log
@app.route('/logs', methods=['POST'])
def add_logs():
    try:
        userId = request.json['userId'];
        sessionId = request.json['sessionId'];
        
        for action in request.json['actions']:
            #iso8601 parses a string into datetime using the isotime format
            time = iso8601.parse_date(action['time']);
            log_type = action['type'];
            
            #case when log-type = "CLICK"
            if log_type == 'CLICK':
                locationX = action['properties']['locationX'];
                locationY = action['properties']['locationY'];
                viewedId = None;
                pageFrom = None;
                pageTo = None;
            
            #case when log_type = "VIEW"
            elif log_type == 'VIEW':
                locationX = None;
                locationY = None;
                viewedId = action['properties']['viewedId'];
                pageFrom = None;
                pageTo = None;
            
            #case when log_type = "NAVIGATE"
            else:
                locationX = None;
                locationY = None;
                viewedId = None;
                pageFrom = action['properties']['pageFrom'];
                pageTo = action['properties']['pageTo'];
        
            new_log = Log(userId, sessionId, time, log_type, locationX, locationY, viewedId, pageFrom, pageTo);
            db.session.add(new_log);
    except:
        abort(400);
        
    db.session.commit();
    
    return make_response('logs added', 200);


#Routes GET methods
#Get All Logs
@app.route('/logs', methods=['GET'])
def get_all_logs():
    all_logs = Log.query.all();
    result = logs_schema.dump(all_logs);
    return jsonify(result);


#Get Logs by userId
@app.route('/user/<string:userId>', methods=['GET'])
def get_logs_by_userId(userId):
    logs_by_userId = Log.query.filter(Log.userId == userId);
    result = logs_schema.dump(logs_by_userId);
    return jsonify(result);


#Get Logs by log_type
@app.route('/log_type/<string:log_type>', methods=['GET'])
def get_logs_by_type(log_type):
    logs_by_type = Log.query.filter(Log.log_type == log_type);
    result = logs_schema.dump(logs_by_type);
    return jsonify(result);


# Get Logs by time range
@app.route('/time_range/<string:start_time>/<string:end_time>', methods=['GET'])
def get_logs_by_time_range(start_time, end_time):
    start = iso8601.parse_date(start_time);
    end = iso8601.parse_date(end_time);
    
    logs_by_time_range = Log.query.filter(Log.time >= start).filter(Log.time <= end);
    result = logs_schema.dump(logs_by_time_range);
    return jsonify(result);


#Get Logs by userId and Log_type
@app.route('/user/<string:userId>/log_type/<string:log_type>', methods=['GET'])
def get_logs_by_userId_and_type(userId, log_type):
    logs_by_userId_and_type = Log.query.filter(Log.userId == userId).filter(Log.log_type == log_type);
    result = logs_schema.dump(logs_by_userId_and_type);
    return jsonify(result);


#Get Logs by userId and time range
@app.route('/user/<string:userId>/time_range/<string:start_time>/<string:end_time>', methods=['GET'])
def get_logs_by_userId_and_time_range(userId, start_time, end_time):
    start = iso8601.parse_date(start_time);
    end = iso8601.parse_date(end_time);
    
    logs_by_userId_and_time_range = Log.query.filter(Log.userId == userId).filter(Log.time >= start)\
    .filter(Log.time <= end);
    
    result = logs_schema.dump(logs_by_userId_and_time_range);
    return jsonify(result);


#Get Logs by log_type and time range
@app.route('/log_type/<string:log_type>/time_range/<string:start_time>/<string:end_time>', methods=['GET'])
def get_logs_by_type_and_time_range(log_type, start_time, end_time):
    start = iso8601.parse_date(start_time);
    end = iso8601.parse_date(end_time);
    
    logs_by_type_and_time_range = Log.query.filter(Log.log_type == log_type).filter(Log.time >= start)\
    .filter(Log.time <= end);
    
    result = logs_schema.dump(logs_by_type_and_time_range);
    return jsonify(result);


#Get Logs by userId and log_type and time range
@app.route('/user/<string:userId>/log_type/<string:log_type>/time_range/<string:start_time>/<string:end_time>', methods=['GET'])
def get_logs_by_userId_and_type_and_time_range(userId, log_type, start_time, end_time):
    start = iso8601.parse_date(start_time);
    end = iso8601.parse_date(end_time);
    
    logs_by_userId_and_type_and_time_range = Log.query.filter(Log.userId == userId)\
    .filter(Log.log_type == log_type).filter(Log.time >= start).filter(Log.time <= end);
    
    result = logs_schema.dump(logs_by_userId_and_type_and_time_range);
    return jsonify(result);


#Routes DELETE methods
#Delete Logs by id
@app.route('/logs/<int:id>', methods=['DELETE'])
def delete_single_log(id):
    single_log = Log.query.get(id);
    db.session.delete(single_log);
    db.session.commit();
    
    return log_schema.jsonify(single_log);


#Delete Logs by userId
@app.route('/user/<string:userId>', methods=['DELETE'])
def delete_logs_userId(userId):
    Log.query.filter(Log.userId == userId).delete();
    db.session.commit();
    
    return make_response('logs deleted', 200);


#Delete Logs by log_type
@app.route('/log_type/<string:log_type>', methods=['DELETE'])
def delete_logs_log_type(log_type):
    Log.query.filter(Log.log_type == log_type).delete();
    db.session.commit();
    
    return make_response('logs deleted', 200);


#Delete Logs by time range
@app.route('/time_range/<string:start_time>/<string:end_time>', methods=['DELETE'])
def delete_logs_time_range(start_time, end_time):
    start = iso8601.parse_date(start_time);
    end = iso8601.parse_date(end_time);
    
    Log.query.filter(Log.time >= start).filter(Log.time <= end).delete();
    db.session.commit();
    
    return make_response('logs deleted', 200);


#Error handling
    
def error_handler(e):
    return f"Error: {e}, route: {request.url}";
 
#400 Error bad request
@app.errorhandler(400)
def bad_request_400(e):
    err_msg = error_handler(e);
    err_msg += "\nData needs to be in json format. Check that all keys and values are spelled correctly";
    return make_response(err_msg, 400);

#404 Error not found
@app.errorhandler(404)
def not_found_404(e):
    return make_response(error_handler(e), 404);

#405 Error method not allowed
@app.errorhandler(405)
def method_not_allowed_405(e):
    return make_response(error_handler(e), 405);

#500 Error internal server error
@app.errorhandler(500)
def internal_server_error_500(e):
    return make_response(error_handler(e), 500);

    

#Run Server
if __name__ == '__main__':
    app.run(debug=True);
    