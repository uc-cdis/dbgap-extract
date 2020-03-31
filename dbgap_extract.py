import requests
import argparse
import xml.etree.ElementTree as ET
import csv
import sys
from datetime import datetime
import logging
import os
import json
import queue

FILENAME = "extract-" + datetime.now().strftime("%m-%d-%Y-%H-%M-%S")
REQUEST_URL = "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/GetSampleStatus.cgi?study_id={}&rettype=xml"
LOG_FILE = FILENAME + ".log"
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG)

FIELD_NAMES = [
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
    "study_subject_id",
]


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
    parser.add_argument(
        "--expand_sra_details",
        help="whether or to expand SRA details into a dict. defaults to flattening",
        action="store_true",
    )

    args = parser.parse_args(sys.argv[1:])

    if args.study_accession_list_filename is None and args.study_accession_list is None:
        logging.debug("-------")
        logging.debug(
            "Usage error. Run this script using one of the two below command forms:"
        )
        logging.debug(
            "> python dbgap_extract.py --study_accession_list accession_1 accession_2 .... [--output_filename file_out.tsv]"
        )
        logging.debug(
            "> python dbgap_extract.py --study_accession_list_filename file.txt [--output_filename file_out.tsv]"
        )
        logging.debug("-------")
        exit(1)

    output_filename = FILENAME + ".tsv"
    if args.output_filename is not None:
        output_filename = args.output_filename
        # Log to a file matching the filename-prefix supplied by the user.
        new_log_file = output_filename.split(".")[0] + ".log"
        if os.path.exists(LOG_FILE):
            os.remove(LOG_FILE)
        setup_logging(new_log_file)
    else:
        # Log to a time-stamped log file of the form extract-%m-%d-%Y-%H-%M-%S.log
        setup_logging(LOG_FILE)

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

    scrape(studies_to_scrape, output_filename, args)

    logging.debug(
        "All done. Extracted elements to {}. Logged info to {}".format(
            output_filename, LOG_FILE
        )
    )


def setup_logging(log_filename):
    """
    Prep the output file for the log and setup the log handling

    Args:
        log_filename (str): file name for output log file
    """
    global LOG_FILE
    LOG_FILE = log_filename
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    directory = os.path.dirname(log_filename)
    if directory is not None and directory != "" and not os.path.exists(directory):
        os.makedirs(directory)
    fileh = logging.FileHandler(log_filename, "a")
    log = logging.getLogger()
    for hdlr in log.handlers[:]:
        log.removeHandler(hdlr)
    log.setLevel(logging.DEBUG)
    log.addHandler(fileh)
    log.addHandler(logging.StreamHandler(sys.stdout))
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def _get_previous_version_of_study_accession(study_accession):
    """
    Get the previous version of a given study accession by attemping to walk backwards

    Args:
        study_accession (str): current study accession number / phsid

    Returns:
        str: previous version of study accession number
    """
    study_accession_split = study_accession.split(".")
    version_number = int(study_accession_split[1][1:])
    if version_number == 1:
        # Can't go back any further
        return None
    previous_version_of_study_accession = ".".join(
        [
            study_accession_split[0],
            "v" + str(version_number - 1),
            study_accession_split[2],
        ]
    )
    return previous_version_of_study_accession


def _get_flattened_sra_data_details_from_xml_sample(sample):
    """
    Get SRA details as a string similar to how it's represented on
    the dbGaP site

    Args:
        sample (xml.etree.ElementTree.Element): sample element from XML tree

    Returns:
        str: SRA details as a string
    """
    sra_datas = sample.findall("SRAData")[0].findall("Stats")
    sra_data_details = ""
    if len(sra_datas) > 0:
        for stat in sra_datas:
            stat_dict = stat.attrib
            stats_as_string = ""
            for key in stat_dict:
                stats_as_string += key + ":" + stat_dict[key] + "|"
            if stats_as_string[-1] == "|":
                stats_as_string = stats_as_string[:-1]
            sra_data_details += "(" + stats_as_string + ") "
    return sra_data_details


def _get_sra_data_details_from_xml_sample(sample):
    """
    Get SRA details as a dictionary

    Args:
        sample (xml.etree.ElementTree.Element): sample element from XML tree

    Returns:
        dict: SRA details as a dict
    """
    sra_datas = sample.findall("SRAData")[0].findall("Stats")
    sra_data_details = {}
    if len(sra_datas) > 0:
        for stat in sra_datas:
            stat_dict = stat.attrib
            sra_data_details.update(stat_dict)
    sra_details = json.dumps(sra_data_details).replace('""', '"')
    return sra_details


def get_sample_dict_from_xml_sample(study_accession, sample, args):
    """
    Get dictionary of sample from the XML sample

    Args:
        study_accession (str): the study phsid/accession number
        sample (xml.etree.ElementTree.Element): sample element from XML tree
        args (argparse.Namespace): arguments sent to command line

    Returns:
        dict: SRA details as a dict
    """
    sample_dict = sample.attrib

    sample_dict["sample_use"] = json.dumps(
        [use.text for use in sample.findall("Uses")[0].findall("Use")]
    )

    if args.expand_sra_details:
        sample_dict["sra_data_details"] = _get_sra_data_details_from_xml_sample(sample)
    else:
        sample_dict[
            "sra_data_details"
        ] = _get_flattened_sra_data_details_from_xml_sample(sample)

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
    study_accession_w_version = ".".join(sample_dict["study_accession"].split(".")[:-1])
    sample_dict["study_subject_id"] = (
        study_accession_w_version + "_" + sample_dict["submitted_subject_id"]
    )
    return sample_dict


def write_sample_rows_for_study(
    study_accession, sample_elements, output_filename, args
):
    """
    Write rows to the output file for the samples provided.

    Args:
        study_accession (str): the study phsid/accession number
        sample (xml.etree.ElementTree.Element): sample element from XML tree
        output_filename (str): output file name to write to
        args (argparse.Namespace): arguments sent to command line
    """
    sample_rows_to_write_for_this_study = []
    for sample in sample_elements:
        try:
            row = []
            sample_dict = get_sample_dict_from_xml_sample(study_accession, sample, args)

            for field in FIELD_NAMES:
                row.append(sample_dict.get(field, ""))

            sample_rows_to_write_for_this_study.append(row)
        except Exception as e:
            logging.error(
                "Error processing sample "
                + sample.attrib.get("submitted_sample_id", "")
            )
            logging.error(e)
    write_list_of_rows_to_tsv(sample_rows_to_write_for_this_study, output_filename)


def scrape(studies_to_scrape, output_filename, args):
    """
    Scrape dbGaP for the given studies and write TSV output to the filename
    provided.

    Args:
        studies_to_scrape (List[str]): the list of study phsid/accession numbers
        output_filename (str): output file name to write to
        args (argparse.Namespace): arguments sent to command line
    """
    study_data = {}

    if os.path.exists(output_filename):
        os.remove(output_filename)

    write_list_of_rows_to_tsv([FIELD_NAMES], output_filename)

    # Use a queue to make it appropriate to modify the list during iteration
    q = queue.Queue()
    [q.put(s) for s in studies_to_scrape]
    already_seen = []

    while not q.empty():
        study_accession = q.get_nowait()
        request_url = REQUEST_URL.format(study_accession)
        try:
            r = requests.get(request_url)
            root = ET.fromstring(r.text)
        except Exception as e:
            logging.error("Failed to parse data from NIH endpoint. {}".format(e))
            exit(1)

        study_accession = root.findall("Study")[0].attrib["accession"]
        sample_list_element = root.findall("Study")[0].findall("SampleList")[0]
        sample_elements = sample_list_element.findall("Sample")

        if len(sample_elements) == 0:
            previous_version_of_study_accession = _get_previous_version_of_study_accession(
                study_accession
            )
            if previous_version_of_study_accession:
                q.put_nowait(previous_version_of_study_accession)
                logging.error(
                    "\nERROR: Study accession {} lacks samples. Going back a version to {}.".format(
                        study_accession, previous_version_of_study_accession
                    )
                )
            else:
                logging.debug(
                    "\nCould not find samples for any version of study accession {}.".format(
                        study_accession
                    )
                )
        elif study_accession not in already_seen:
            write_sample_rows_for_study(
                study_accession, sample_elements, output_filename, args
            )
            already_seen.append(study_accession)


def write_list_of_rows_to_tsv(rows, output_filename):
    """
    Write the given rows to the output file.

    Args:
        rows (List[str]): list of rows to write to file
        output_filename (str): output file name to write to
    """
    with open(output_filename, "a+") as out_file:
        tsv_writer = csv.writer(out_file, delimiter="\t")
        for row in rows:
            tsv_writer.writerow(row)


if __name__ == "__main__":
    main()
