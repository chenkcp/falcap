#Project Instruction

## Coding standard
- this is the python code deployed as AWS lambda function.
- the lambda function is call as web api or run on schedule by AWS eventbridge.

## project objective
- The objective of the project is to get the work orders, each work order has predefined constraint test from database; the constraint test must have minitmum parts with parametric data to run the constraint test
- there is work order data model which record the workorder status and test data model which record the each constraint test details 
- The return of api call is the work order object which has the workorder id status and each constraint and its Pass/Fail/Skip status

## Issue Identification 
- flag any hardcoded API keys or secrets immediately
- identify redundancy of information in the data models
- make sure the data model has records in any situation of skipped or bypassed or blocked work order and cosntraint test
- catch any data format inconsistency
  
   
