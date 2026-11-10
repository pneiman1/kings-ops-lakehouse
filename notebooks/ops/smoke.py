# Databricks notebook source
import json
try:
    r = {"status": "ok", "user": spark.sql("SELECT current_user() AS u").collect()[0]["u"]}
except Exception as e:
    r = {"status": "error", "error": str(e)}
dbutils.notebook.exit(json.dumps(r))
