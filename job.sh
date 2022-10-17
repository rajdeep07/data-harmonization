#!/bin/sh

conda activate data-harmonization
python /home/navazdeen/data-harmonization/data_harmonization/main/code/tiger/Ingester.py
conda activate data-harmonization-benchmark
python '/home/navazdeen/data-harmonization/data_harmonization/main/code/tiger/benchmark/deduplication.py -t'
python '/home/navazdeen/data-harmonization/data_harmonization/main/code/tiger/benchmark/deduplication.py -p'
conda activate data-harmonization
python '/home/navazdeen/data-harmonization/data_harmonization/main/code/tiger/clustering/Blocking.py -t'
python '/home/navazdeen/data-harmonization/data_harmonization/main/code/tiger/clustering/Blocking.py -p'
python /home/navazdeen/data-harmonization/data_harmonization/main/code/tiger/Classifier.py
python /home/navazdeen/data-harmonization/data_harmonization/main/code/tiger/Synthesis.py
python /home/navazdeen/data-harmonization/data_harmonization/main/code/tiger/Mapping.py
python /home/navazdeen/data-harmonization/data_harmonization/main/code/tiger/Merger.py
