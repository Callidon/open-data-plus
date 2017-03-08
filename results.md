# Open Data Analyzer results

# Experiments with setup in ALMA room

10 machines, 7GB of memory allocated per executor

Stage | Execution time | # Mappers | # Stages
------------ | ------------- | ------------- | -------------
stage1 | ~ 45mn | ?? | 3970
stage2 | ~ 50mn | ?? | 3970

# Experiments with Curiosiphi

1 machine, 100GB of memory allocated per executor

Stage | Execution time | # Mappers | # Stages
------------ | ------------- | ------------- | -------------
stage1 | 7890,99s/2,19h | 40 | 3695
stage2 | 7694,13s/2,13h | 40 | 3695
agg | 270,11s/4,5mn | 40 | 86
agg + tree | 236,86s/3,9mn | 40 | 86

Average number of memory used: 35GB
