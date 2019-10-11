import argparse
import sys


def get_unique_accessions_from_input_PHS_list(filename):
    f = open(filename)
    r = f.readlines()
    f.close()

    deduped_accessions = {}

    for accession in r:
        deduped_accessions[accession.strip()] = 1

    sorted_unique_accessions = deduped_accessions.keys()
    sorted_unique_accessions.sort()
    return sorted_unique_accessions


def get_unique_accessions_from_output_extract(filename):
    f = open(filename)
    r = f.readlines()
    f.close()

    deduped_accessions = {}

    for accession in r:
        study = accession.split("\t")
        accession = study[-1].strip().split(".")[0]
        if "phs" not in accession:
            continue
        deduped_accessions[accession] = 1

    sorted_unique_accessions = deduped_accessions.keys()
    sorted_unique_accessions.sort()
    return sorted_unique_accessions


def main():
    parser = argparse.ArgumentParser(description="Validate dbgap extract file.")
    parser.add_argument(
        "--input_file",
        help="a file containing a newline-separated list of study accessions",
    )
    parser.add_argument("--output_file", help="a generated extract file")

    args = parser.parse_args(sys.argv[1:])

    if args.input_file is None and args.output_file is None:
        print("-------")
        print("Usage:")
        print(
            "> python validate_extract.py --input_file <phs_list.txt> --output_file <dbgap_extract_file.tsv>"
        )
        print("-------")
        exit(0)

    input_file = args.input_file
    output_file = args.output_file
    accessions_from_input = get_unique_accessions_from_input_PHS_list(input_file)

    accessions_from_output = get_unique_accessions_from_output_extract(output_file)

    print("Looking at input {} vs output {}".format(input_file, output_file))
    print("Input: ", accessions_from_input)
    print("Output: ", accessions_from_output)

    if len(accessions_from_input) != len(accessions_from_output):
        print(
            "Mismatch: inputted {} PHS, outputted {} PHS".format(
                len(accessions_from_input), len(accessions_from_output)
            )
        )

    for record in accessions_from_input:
        if record not in accessions_from_output:
            print(
                "Output is missing {}. \n\t > Check if records are missing here https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/GetSampleStatus.cgi?study_id={}&rettype=html".format(
                    record, record
                )
            )


if __name__ == "__main__":
    main()
