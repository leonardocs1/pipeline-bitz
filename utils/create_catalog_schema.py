# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS bitz
# MAGIC COMMENT "Catálogo principal do projeto, com camadas de dados governadas via Unity Catalog";
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bitz.raw
# MAGIC COMMENT "Camada RAW - Dados brutos da API";
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS bitz.raw.saldo
# MAGIC COMMENT "Volume para dados de saldo";
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bitz.bronze
# MAGIC COMMENT "Camada Bronze - dados padronizados, com metadados e controle de ingestão";
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bitz.silver
# MAGIC COMMENT "Camada Silver - dados tratados, com regras de negócio aplicadas";
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bitz.gold
# MAGIC COMMENT "Camada Gold - dados analíticos e métricas finais para BI e IA";
# MAGIC
