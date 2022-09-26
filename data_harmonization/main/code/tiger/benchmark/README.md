# benchmarking using dedupe.io
This is the repo to run data harmonization on disparate data sources using entity deduplication supported by dedupe.io

Commands to generate results from python package:

For Inferencing:
```
cd benchmark
conda env create --name data-harmonization-benchmark --file environment-b.yml
conda activate data-harmonization-benchmark
python deduplication.py
```

For Training:
```
cd benchmark
rm dedupe_dataframe_training.json
rm dedupe_dataframe_learned_settings
conda env create --name data-harmonization-benchmark --file environment-b.yml
conda activate data-harmonization-benchmark
python deduplication.py
```