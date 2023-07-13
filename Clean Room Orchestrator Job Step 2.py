# Databricks notebook source
# MAGIC %pip install git+https://github.com/wchau/clean-room-notebook -q
# MAGIC from databricks_clean_room_orchestrator.client import CleanRoomClient

# COMMAND ----------

CleanRoomClient().teardownStation()

# COMMAND ----------


