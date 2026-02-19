CREATE EXTERNAL TABLE IF NOT EXISTS proven-arcade-487516-t0.bronze_dataset.departments 
OPTIONS (
  format = 'JSON',
  uris = ['gs://akash-33/landing/hospital/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS proven-arcade-487516-t0.bronze_dataset.encounters 
OPTIONS (
  format = 'JSON',
  uris = ['gs://akash-33/landing/hospital/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS proven-arcade-487516-t0.bronze_dataset.patients 
OPTIONS (
  format = 'JSON',
  uris = ['gs://akash-33/landing/hospital/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS proven-arcade-487516-t0.bronze_dataset.providers 
OPTIONS (
  format = 'JSON',
  uris = ['gs://akash-33/landing/hospital/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS proven-arcade-487516-t0.bronze_dataset.transactions 
OPTIONS (
  format = 'JSON',
  uris = ['gs://akash-33/landing/hospital/transactions/*.json']
);