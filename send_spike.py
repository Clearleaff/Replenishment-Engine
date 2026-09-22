#!/usr/bin/env python3
"""
send_spike.py
Publishes synthetic inventory depletion events to RabbitMQ for hybrid-orchestrator.
Usage:
  python3 send_spike.py critical   # Triggers REQUIRES_HUMAN_APPROVAL (Scenario B)
  python3 send_spike.py low        # Triggers AUTO_APPROVED (Scenario A)
"""

import sys
import json
import pika

AMQP_HOST = '127.0.0.1'
AMQP_PORT = 11098
AMQP_USER = 'guest'
AMQP_PASS = 'THjwWm1V8Ud7uJnQt34D1U'
QUEUE_NAME = 'eshop.inventory.order_stock_confirmed'

import time
import random

scenario = sys.argv[1].lower() if len(sys.argv) > 1 else 'critical'
target_sku = 2
target_loc = sys.argv[3].upper() if len(sys.argv) > 3 else 'BLR'
order_id = int(sys.argv[4]) if len(sys.argv) > 4 else random.randint(10100, 99999)

credentials = pika.PlainCredentials(AMQP_USER, AMQP_PASS)
parameters = pika.ConnectionParameters(AMQP_HOST, AMQP_PORT, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue=QUEUE_NAME, durable=True)

if scenario == 'low':
    payload = {
        "orderId": order_id,
        "skuId": target_sku,
        "locationCode": target_loc,
        "quantityDepleted": 5,
        "occurredAt": "2026-09-22T10:00:00Z",
        "balance": {
            "onHand": 50,
            "reserved": 0,
            "safetyStock": 20,
            "reorderPoint": 40,
            "maxStock": 200,
            "version": int(time.time()),
            "authoritative": True
        }
    }
    print(f"🚀 Publishing Low-Risk Scenario for SKU {target_sku} ({target_loc}, Order #{order_id})...")
else:
    # Critical Deficit (Stock drops to 0, safety stock is 50, target is 200)
    payload = {
        "orderId": order_id,
        "skuId": target_sku,
        "locationCode": target_loc,
        "quantityDepleted": 30,
        "occurredAt": "2026-09-22T10:00:00Z",
        "balance": {
            "onHand": 0,
            "reserved": 0,
            "safetyStock": 50,
            "reorderPoint": 100,
            "maxStock": 500,
            "version": int(time.time()),
            "authoritative": True
        }
    }
    print(f"🚨 Publishing Critical Deficit Scenario for SKU {target_sku} ({target_loc}, Order #{order_id})...")

channel.basic_publish(
    exchange='',
    routing_key=QUEUE_NAME,
    body=json.dumps(payload),
    properties=pika.BasicProperties(
        delivery_mode=2,  # make message persistent
        content_type='application/json'
    )
)

connection.close()
print("✅ Event published successfully to RabbitMQ!")
print("👉 Now check your Hybrid Orchestrator terminal and run:")
print("   curl -s http://127.0.0.1:5005/api/v1/proposals | jq")

