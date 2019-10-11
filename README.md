# dbgap-extract

Usage of dbgap_extract.py:

Either run this script using a space-separated list of study accessions:

`python dbgap_extract.py --study_accession_list accession_1 accession_2 .... [--output_filename file_out.tsv]`

or with a path to a file containing a newline-separated list of study accessions:

`python dbgap_extract.py --study_accession_list_filename file.txt [--output_filename file_out.tsv]`

This repo also contains a validation script which can be used to verify the correctness of the output of dbgap_extract.py. The output of the validation script is meant to be reviewed by a human, and requires some manual effort.

`python validate_extract.py --input_file <phs_list.txt> --output_file <dbgap_extract_file.tsv>`