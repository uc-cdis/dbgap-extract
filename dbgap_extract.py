import requests
import argparse
import xml.etree.ElementTree as ET
import csv
import sys
from datetime import datetime
import logging
import os

FILENAME = "extract-" + datetime.now().strftime("%m-%d-%Y-%H-%M-%S")
REQUEST_URL = "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/GetSampleStatus.cgi?study_id={}&rettype=xml"
LOG_FILE = FILENAME + ".log"
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def scrape(studies_to_scrape, output_filename):
    study_data = {}
    field_names = [
        "submitted_sample_id",
        "biosample_id",
        "dbgap_sample_id",
        "sra_sample_id",
        "submitted_subject_id",
        "dbgap_subject_id",
        "consent_code",
        "consent_short_name",
        "sex",
        "body_site",
        "analyte_type",
        "sample_use",
        "repository",
        "dbgap_status",
        "sra_data_details",
        "study_accession",
        "study_accession_with_consent",
        "study_with_consent",
    ]

    write_list_of_rows_to_tsv([field_names], output_filename)

    for study_accession in studies_to_scrape:
        request_url = REQUEST_URL.format(study_accession)
        try:
            r = requests.get(request_url)
            root = ET.fromstring(r.text)
        except Exception as e:
            logging.error("Failed to parse data from NIH endpoint. {}".format(e))
            exit()

        study_accession = root.findall("Study")[0].attrib["accession"]
        sample_list_element = root.findall("Study")[0].findall("SampleList")[0]
        sample_elements = sample_list_element.findall("Sample")
        sample_rows_to_write_for_this_study = []
        for sample in sample_elements:
            try:
                row = []
                sample_dict = sample.attrib
                sample_dict["study_accession"] = study_accession
                if "consent_code" in sample_dict:
                    sample_dict["study_accession_with_consent"] = (
                        study_accession + ".c" + sample_dict.get("consent_code", "")
                    )
                    sample_dict["study_with_consent"] = (
                        ".".join(study_accession.split(".")[:-2])
                        + ".c"
                        + sample_dict.get("consent_code", "")
                    )
                else:
                    logging.debug(
                        "Sample "
                        + sample.attrib.get("submitted_sample_id", "")
                        + " lacks a consent code. Leaving "
                        + "study_accession_with_consent and study_with_consent columns empty."
                    )

                for field in field_names:
                    row.append(sample.attrib.get(field, ""))
                sample_rows_to_write_for_this_study.append(row)
            except Exception as e:
                logging.error(
                    "Error processing sample "
                    + sample.attrib.get("submitted_sample_id", "")
                )
                logging.error(e)
        write_list_of_rows_to_tsv(sample_rows_to_write_for_this_study, output_filename)


def write_list_of_rows_to_tsv(rows, output_filename):
    with open(output_filename, "a+") as out_file:
        tsv_writer = csv.writer(out_file, delimiter="\t")
        for row in rows:
            tsv_writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(description="Generate dbgap extract file.")
    parser.add_argument(
        "--study_accession_list_filename",
        help="a file containing a newline-separated list of study accessions",
    )
    parser.add_argument(
        "--study_accession_list",
        nargs="*",
        help="a space-separated array of study accessions",
    )
    parser.add_argument(
        "--output_filename", help="optionally specify a name for the output file"
    )

    args = parser.parse_args(sys.argv[1:])

    if args.study_accession_list_filename is None and args.study_accession_list is None:
        logging.debug("-------")
        logging.debug(
            "Usage error. Run this script using one of the two below command forms:"
        )
        logging.debug(
            "> python dbgapExtract.py --study_accession_list accession_1 accession_2 .... [--output_filename file_out.tsv]"
        )
        logging.debug(
            "> python dbgapExtract.py --study_accession_list_filename file.txt [--output_filename file_out.tsv]"
        )
        logging.debug("-------")
        exit(0)

    output_filename = FILENAME + ".tsv"
    if args.output_filename is not None:
        output_filename = args.output_filename
        # Log to a file matching the filename-prefix supplied by the user.
        LOG_FILE = output_filename.split(".")[0] + ".log"
        fileh = logging.FileHandler(LOG_FILE, "a")
        log = logging.getLogger()
        for hdlr in log.handlers[:]:
            if "baseFilename" in dir(hdlr) and FILENAME in hdlr.baseFilename:
                os.remove(hdlr.baseFilename)
                log.removeHandler(hdlr)
        log.addHandler(fileh)

    logging.basicConfig(filename=FILENAME + ".log", level=logging.DEBUG)

    studies_to_scrape = []
    if args.study_accession_list is not None:
        studies_to_scrape = args.study_accession_list

    if args.study_accession_list_filename is not None:
        f = open(args.study_accession_list_filename)
        studies_to_scrape = f.readlines()
        studies_to_scrape = list(map(lambda x: x.strip(), studies_to_scrape))
        f.close()

    logging.debug(
        "Extracting the below studies to {} \n".format(output_filename)
        + " ".join(studies_to_scrape)
    )

    scrape(studies_to_scrape, output_filename)

    logging.debug(
        "All done. Extracted elements to {}. Logged info to {}".format(
            output_filename, LOG_FILE
        )
    )


if __name__ == "__main__":
    main()
