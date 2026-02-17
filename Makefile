SHELL := /bin/bash

# Load .env if it exists
ifneq (,$(wildcard .env))
include .env
export
endif

.PHONY: aws_whoami ingest upload verify s3_ls

aws_whoami:
	aws sts get-caller-identity

ingest:
	python -m src.ingest.fetch_data

upload:
	python -m src.ingest.upload_to_s3

verify:
	python -m src.ingest.verify_s3_upload

s3_ls:
	aws s3 ls s3://$(AIRBNB_S3_BUCKET)/$(AIRBNB_S3_PREFIX)/ --recursive
