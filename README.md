# music-statistics

## How to run

0. Please place file `userid-timestamp-artid-artname-traid-traname.tsv` from the provided dataset to `data/input/`

1. First you need to build docker images
```bash
make build
```
2. To run the pipeline
```bash
make run
```
3. The results of pipeline execution you can find in `./data/output/`. Each run will generate a new file

## Why spark?

Spark seems to a be a good choice for this kind of transformation job because:
 - we need to work with a big input file
 - do some cleaning
 - do transformation 
 - spark helps to execute everything in efficient way and still have a real application that could be tested and easily extended

## To improve

1. Add configuration file and get all the input parameters(file paths, number of top songs, etc) from the config
2. Make  `run_pipeline.py` to accept command line arguments in order ot make it a bit more flexible
3. Add validation step(check file exists, the right format, schema etc)
4. Add data cleaning step and drop all the rows that are not satisfied validation criteria
5. Improve job efficiency: play around with different techniques in order to improve efficiency. I was not focusing on making the job run in the most efficient way, but simply make the assignment done.
6. Add unit tests
7. Add more monitoring for each step(how man rows we process etc)
8. Some steps could be simplified and merged, but I wanted to make the logic is clear as possible
   
