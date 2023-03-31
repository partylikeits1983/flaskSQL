import os
from dotenv import load_dotenv
import json
from flask import Flask, request, render_template, jsonify
from datetime import datetime, date

# database code
from database import Methods

app = Flask(__name__)

@app.route("/")
def get_entry_point():
    return "<p>RESPONSE: 200</p>"

@app.route("/status")
def get_status():
    return "<p>Status: nominal</p>"

@app.route('/stores', methods=['GET'])
def get_all_stores():
    db = Methods()
    result = db.getStores()
    return json.dumps(result, ensure_ascii=False)

@app.route('/stores/<store_id>', methods=['GET'])
def get_store_info(store_id):
    db = Methods()
    result = db.getStoreInfo(store_id)
    return json.dumps(result, ensure_ascii=False)

@app.route('/customers/show', methods=['GET'])
def get_customers():
    db = Methods()
    result = db.getCustomers()
    return json.dumps(result, ensure_ascii=False)

@app.route('/prices/max', methods=['GET'])
def get_prices_max():
    db = Methods()
    result = db.getPricesMax()
    return json.dumps(result, ensure_ascii=False)

@app.route('/stores/add', methods=['GET', 'POST'])
def add_store():
    db = Methods()

    address = request.json['address'] 
    region = request.json['region']

    if (db.addNewStore(address, region)):
        return "success"
    else:
        return "failed"


if __name__ == '__main__':
    app.run("0.0.0.0", port=8080)

