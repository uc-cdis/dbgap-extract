# Run this file with:
# python3 -m pytest tests.py

import sys
import pytest
import filecmp
import dbgap_extract
import argparse
import xml.etree.ElementTree as ET


def test_get_previous_version_of_study_accession():
    study_accession = "phs001143.v2.p1"
    previous_version = dbgap_extract._get_previous_version_of_study_accession(
        study_accession
    )
    assert previous_version == "phs001143.v1.p1"

    # Can't go back a version
    study_accession = "phs001143.v1.p1"
    previous_version = dbgap_extract._get_previous_version_of_study_accession(
        study_accession
    )
    assert previous_version == None

    study_accession = "phs000179.v33.p2"
    previous_version = dbgap_extract._get_previous_version_of_study_accession(
        study_accession
    )
    assert previous_version == "phs000179.v32.p2"


def get_test_sample_elements():
    with open("test_data/test_xml.xml") as fh:
        xml_text = fh.read()
        root = ET.fromstring(xml_text)
        study_accession = root.findall("Study")[0].attrib["accession"]
        sample_list_element = root.findall("Study")[0].findall("SampleList")[0]
        sample_elements = sample_list_element.findall("Sample")
        return sample_elements


def test_get_sra_data_details_from_xml_sample():
    sample_elements = get_test_sample_elements()

    sra_data_details = dbgap_extract._get_flattened_sra_data_details_from_xml_sample(
        sample_elements[0]
    )
    assert (
        sra_data_details
        == "(status:public|experiments:1|runs:3|bases:406977793500|size_Gb:74|experiment_type:WGS|platform:ILLUMINA|center:ABC Fast Track Services) "
    )

    sra_data_details = dbgap_extract._get_flattened_sra_data_details_from_xml_sample(
        sample_elements[1]
    )
    assert (
        sra_data_details
        == "(status:public|experiments:1|runs:2|bases:250660703000|size_Gb:49|experiment_type:WGS|platform:ILLUMINA|center:CDE Fast Track Services) "
    )


def assert_dict_equality(dict_a, dict_b):
    assert len(list(dict_a.keys())) == len(list(dict_b.keys()))
    for key in dict_a:
        assert dict_a[key] == dict_b[key]


def test_get_sample_dict_from_xml_sample():
    sample_elements = get_test_sample_elements()
    args = argparse.Namespace(expand_sra_details=False)

    expected_sample_dict = {
        "repository": "CDE",
        "submitted_sample_id": "NWD1",
        "biosample_id": "SAMN1",
        "submitted_subject_id": "ABC",
        "dbgap_sample_id": "1",
        "dbgap_subject_id": "1",
        "sra_sample_id": "SRS1",
        "consent_code": "1",
        "consent_short_name": "GRU-IRB",
        "sex": "male",
        "body_site": "Whole blood",
        "analyte_type": "DNA",
        "dbgap_status": "Loaded",
        "sample_use": ["Seq_DNA_SNP_CNV", "WGS"],
        "sra_data_details": "(status:public|experiments:1|runs:3|bases:406977793500|size_Gb:74|experiment_type:WGS|platform:ILLUMINA|center:ABC Fast Track Services) ",
        "study_accession": "phs001234.v3.p1",
        "study_accession_with_consent": "phs001234.v3.p1.c1",
        "study_with_consent": "phs001234.c1",
        "study_subject_id": "phs001234.v3_ABC",
    }
    sample_dict = dbgap_extract.get_sample_dict_from_xml_sample(
        "phs001234.v3.p1", sample_elements[0], args
    )
    assert_dict_equality(sample_dict, expected_sample_dict)

    expected_sample_dict = {
        "repository": "FGH",
        "submitted_sample_id": "NWD1",
        "biosample_id": "SAMN2",
        "submitted_subject_id": "CDE",
        "dbgap_sample_id": "1",
        "dbgap_subject_id": "2",
        "sra_sample_id": "SRS2",
        "consent_code": "1",
        "consent_short_name": "GRU-IRB",
        "sex": "female",
        "body_site": "Whole blood",
        "analyte_type": "DNA",
        "dbgap_status": "Loaded",
        "sample_use": ["Seq_DNA_SNP_CWB", "GWS"],
        "sra_data_details": "(status:public|experiments:1|runs:2|bases:250660703000|size_Gb:49|experiment_type:WGS|platform:ILLUMINA|center:CDE Fast Track Services) ",
        "study_accession": "phs001234.v3.p1",
        "study_accession_with_consent": "phs001234.v3.p1.c1",
        "study_with_consent": "phs001234.c1",
        "study_subject_id": "phs001234.v3_CDE",
    }
    sample_dict = dbgap_extract.get_sample_dict_from_xml_sample(
        "phs001234.v3.p1", sample_elements[1], args
    )
    assert_dict_equality(sample_dict, expected_sample_dict)


def test_get_sample_dict_sra_expand_from_xml_sample():
    sample_elements = get_test_sample_elements()
    args = argparse.Namespace(expand_sra_details=True)

    expected_sample_dict = {
        "repository": "CDE",
        "submitted_sample_id": "NWD1",
        "biosample_id": "SAMN1",
        "submitted_subject_id": "ABC",
        "dbgap_sample_id": "1",
        "dbgap_subject_id": "1",
        "sra_sample_id": "SRS1",
        "consent_code": "1",
        "consent_short_name": "GRU-IRB",
        "sex": "male",
        "body_site": "Whole blood",
        "analyte_type": "DNA",
        "dbgap_status": "Loaded",
        "sample_use": ["Seq_DNA_SNP_CNV", "WGS"],
        "sra_data_details": {
            "status": "public",
            "experiments": "1",
            "runs": "3",
            "bases": "406977793500",
            "size_Gb": "74",
            "experiment_type": "WGS",
            "platform": "ILLUMINA",
            "center": "ABC Fast Track Services",
        },
        "study_accession": "phs001234.v3.p1",
        "study_accession_with_consent": "phs001234.v3.p1.c1",
        "study_with_consent": "phs001234.c1",
        "study_subject_id": "phs001234.v3_ABC",
    }
    sample_dict = dbgap_extract.get_sample_dict_from_xml_sample(
        "phs001234.v3.p1", sample_elements[0], args
    )
    assert_dict_equality(sample_dict, expected_sample_dict)

    expected_sample_dict = {
        "repository": "FGH",
        "submitted_sample_id": "NWD1",
        "biosample_id": "SAMN2",
        "submitted_subject_id": "CDE",
        "dbgap_sample_id": "1",
        "dbgap_subject_id": "2",
        "sra_sample_id": "SRS2",
        "consent_code": "1",
        "consent_short_name": "GRU-IRB",
        "sex": "female",
        "body_site": "Whole blood",
        "analyte_type": "DNA",
        "dbgap_status": "Loaded",
        "sample_use": ["Seq_DNA_SNP_CWB", "GWS"],
        "sra_data_details": {
            "status": "public",
            "experiments": "1",
            "runs": "2",
            "bases": "250660703000",
            "size_Gb": "49",
            "experiment_type": "WGS",
            "platform": "ILLUMINA",
            "center": "CDE Fast Track Services",
        },
        "study_accession": "phs001234.v3.p1",
        "study_accession_with_consent": "phs001234.v3.p1.c1",
        "study_with_consent": "phs001234.c1",
        "study_subject_id": "phs001234.v3_CDE",
    }
    sample_dict = dbgap_extract.get_sample_dict_from_xml_sample(
        "phs001234.v3.p1", sample_elements[1], args
    )
    assert_dict_equality(sample_dict, expected_sample_dict)
