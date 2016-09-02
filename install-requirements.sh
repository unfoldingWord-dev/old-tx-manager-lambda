#!/usr/bin/env bash

#functions=("request" "messageLog", "jobStarted", "jobCompleted", "jobFailed")
functions=("request")

for function in "${functions[@]}"
do
  pip install -r functions/$function/requirements.txt -t functions/$function
done
