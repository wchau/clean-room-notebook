# Databricks notebook source
# MAGIC %pip install git+https://github.com/wchau/clean-room-notebook
# MAGIC from databricks_clean_room_orchestrator.client import CleanRoomClient

# COMMAND ----------

CleanRoomClient().teardownAllStations()
