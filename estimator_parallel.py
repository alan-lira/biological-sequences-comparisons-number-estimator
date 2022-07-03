from configparser import ConfigParser
from csv import DictWriter
from functools import partial
from math import ceil
from multiprocessing import Pool
from pathlib import Path
from shutil import rmtree
from sys import argv
from time import time
from typing import List


def check_if_has_valid_number_of_arguments(argv_list: list) -> None:
    number_of_arguments_expected = 1
    arguments_expected_list = ["estimator_config_file"]
    number_of_arguments_provided = len(argv_list) - 1
    if number_of_arguments_provided != number_of_arguments_expected:
        number_of_arguments_expected_message = \
            "".join([str(number_of_arguments_expected),
                     " arguments were" if number_of_arguments_expected > 1 else " argument was"])
        number_of_arguments_provided_message = \
            "".join([str(number_of_arguments_provided),
                     " arguments were" if number_of_arguments_provided > 1 else " argument was"])
        invalid_number_of_arguments_message = \
            "Invalid number of arguments provided!\n" \
            "{0} expected: {1}\n" \
            "{2} provided: {3}".format(number_of_arguments_expected_message,
                                       ", ".join(arguments_expected_list),
                                       number_of_arguments_provided_message,
                                       ", ".join(argv_list[1:]))
        raise ValueError(invalid_number_of_arguments_message)


def check_if_file_exists(file_path: Path) -> None:
    if not file_path.is_file():
        file_not_found_message = "'{0}' not found. The application will halt!".format(str(file_path))
        raise FileNotFoundError(file_not_found_message)


def parse_estimator_config(estimator_config_file: Path) -> List:
    config_parser = ConfigParser()
    config_parser.optionxform = str
    config_parser.read(estimator_config_file,
                       encoding="utf-8")
    output_directory_path = config_parser.get("Output Settings",
                                              "output_directory")
    number_of_processes = int(config_parser.get("General Settings",
                                                "number_of_processes"))
    n_lower_bound = int(config_parser.get("General Settings",
                                          "n_lower_bound"))
    n_upper_bound = int(config_parser.get("General Settings",
                                          "n_upper_bound"))
    n_range_per_output_file = int(config_parser.get("General Settings",
                                                    "n_range_per_output_file"))
    max_s_lower_bound = int(config_parser.get("General Settings",
                                              "max_s_lower_bound"))
    estimator_config = [output_directory_path,
                        number_of_processes,
                        n_lower_bound,
                        n_upper_bound,
                        n_range_per_output_file,
                        max_s_lower_bound]
    return estimator_config


def write_csv_file_header(output_csv_file: Path) -> None:
    with open(file=output_csv_file, mode="w") as csv_file:
        header_field_names = ["n",
                              "max_S",
                              "max_S Bounds",
                              "Actual D_a",
                              "Estimated D_a",
                              "Absolute Error",
                              "Relative Error",
                              "Percent Error (%)"]
        csv_writer = DictWriter(f=csv_file,
                                fieldnames=header_field_names)
        csv_writer.writeheader()


def generate_sequences_indices_list(n: int,
                                    max_s: int) -> list:
    sequences_indices_list = []
    first_data_structure_sequences_indices_list = []
    second_data_structure_sequences_indices_list = []
    first_data_structure_first_sequence_index = 0
    first_data_structure_last_sequence_index = n - 1
    first_data_structure_sequences_index_range = range(first_data_structure_first_sequence_index,
                                                       first_data_structure_last_sequence_index)
    for first_data_structure_sequence_index in first_data_structure_sequences_index_range:
        second_data_structure_first_sequence_index = first_data_structure_sequence_index + 1
        second_data_structure_last_sequence_index = n
        second_data_structure_last_sequence_added = 0
        while second_data_structure_last_sequence_added != second_data_structure_last_sequence_index - 1:
            first_data_structure_sequences_indices_list.append(first_data_structure_sequence_index)
            sequences_on_second_data_structure_count = 0
            second_data_structure_sequence_index = 0
            for second_data_structure_sequence_index in range(second_data_structure_first_sequence_index,
                                                              second_data_structure_last_sequence_index):
                second_data_structure_sequences_indices_list.extend([second_data_structure_sequence_index])
                sequences_on_second_data_structure_count = sequences_on_second_data_structure_count + 1
                if sequences_on_second_data_structure_count == max_s:
                    break
            if len(first_data_structure_sequences_indices_list) > 0 \
                    and len(second_data_structure_sequences_indices_list) > 0:
                sequences_indices_list.append([first_data_structure_sequences_indices_list,
                                               second_data_structure_sequences_indices_list])
                second_data_structure_last_sequence_added = second_data_structure_sequence_index
                second_data_structure_first_sequence_index = second_data_structure_last_sequence_added + 1
            first_data_structure_sequences_indices_list = []
            second_data_structure_sequences_indices_list = []
    return sequences_indices_list


def estimate_total_number_of_diffs(n: int,
                                   max_s: int) -> int:
    estimated_total_number_of_diffs = 0
    if 1 <= max_s < (n / 2):
        estimated_total_number_of_diffs = ceil((n / max_s) * ((n - 1) - ((n - max_s) / 2)))
    elif (n / 2) <= max_s < n:
        estimated_total_number_of_diffs = 2 * ((n - 1) - (max_s / 2))
    return int(estimated_total_number_of_diffs)


def calculate_absolute_error_of_total_number_of_diffs_estimation(estimated_total_number_of_diffs: int,
                                                                 actual_total_number_of_diffs: int) -> int:
    return estimated_total_number_of_diffs - actual_total_number_of_diffs


def calculate_relative_error_of_total_number_of_diffs_estimation(estimated_total_number_of_diffs: int,
                                                                 actual_total_number_of_diffs: int) -> float:
    return (estimated_total_number_of_diffs - actual_total_number_of_diffs) / actual_total_number_of_diffs


def calculate_percent_error_of_total_number_of_diffs_estimation(estimated_total_number_of_diffs: int,
                                                                actual_total_number_of_diffs: int) -> float:
    return ((estimated_total_number_of_diffs - actual_total_number_of_diffs) / actual_total_number_of_diffs) * 100


def estimate_and_append_to_csv_file(n: int,
                                    max_s: int,
                                    output_csv_file: Path) -> None:
    # Generate Sequences Indices List
    sequences_indices_list = generate_sequences_indices_list(n,
                                                             max_s)
    # Get Case of max_s
    case_max_s = None
    if 1 <= max_s < (n / 2):
        case_max_s = "First Case [1 <= max_S < (n / 2)]"
    elif (n / 2) <= max_s < n:
        case_max_s = "Second Case [(n / 2) <= max_S < n]"
    # Get Actual Total Number of Diffs (Dₐ)
    actual_d_a = len(sequences_indices_list)
    # Estimate Total Number of Diffs (Dₐ Estimation)
    estimated_d_a = estimate_total_number_of_diffs(n,
                                                   max_s)
    # Calculate Absolute Error of Dₐ Estimation (Modulus)
    d_a_estimation_absolute_error = calculate_absolute_error_of_total_number_of_diffs_estimation(estimated_d_a,
                                                                                                 actual_d_a)
    # Get Absolute Value (Modulus) of the Absolute Error
    abs(d_a_estimation_absolute_error)
    # Calculate Relative Error of Dₐ Estimation (Modulus)
    d_a_estimation_relative_error = calculate_relative_error_of_total_number_of_diffs_estimation(estimated_d_a,
                                                                                                 actual_d_a)
    # Get Absolute Value (Modulus) of the Relative Error
    abs(d_a_estimation_relative_error)
    # Calculate Percent Error of Dₐ Estimation (Modulus)
    d_a_estimation_percent_error = calculate_percent_error_of_total_number_of_diffs_estimation(estimated_d_a,
                                                                                               actual_d_a)
    # Get Absolute Value (Modulus) of the Percent Error
    abs(d_a_estimation_percent_error)
    # Append Result to Output CSV File
    with open(file=output_csv_file, mode="a") as csv_file:
        result_line = "{0},{1},{2},{3},{4},{5},{6},{7}\n".format(n,
                                                                 max_s,
                                                                 case_max_s,
                                                                 actual_d_a,
                                                                 estimated_d_a,
                                                                 d_a_estimation_absolute_error,
                                                                 d_a_estimation_relative_error,
                                                                 d_a_estimation_percent_error)
        csv_file.write(result_line)
    # Delete Generated Sequences Indices List
    del sequences_indices_list


def parallel_task(*args):
    n_upper_bound = args[0]
    max_s_lower_bound = args[1]
    n_range_per_output_file = args[2]
    output_csv_file_list = args[3]
    n = args[4]
    output_file_index = ceil(n / n_range_per_output_file) - 1
    output_csv_file = output_csv_file_list[output_file_index]
    for max_s in range(max_s_lower_bound, n_upper_bound):
        if max_s < n:  # Maximum Value for max_s = n - 1
            estimate_and_append_to_csv_file(n,
                                            max_s,
                                            output_csv_file)
        else:
            break


def execute_estimation(estimator_config: List) -> None:
    # Get Output Directory Path
    output_directory_path = Path(estimator_config[0])
    # Get Number of Processes
    number_of_processes = estimator_config[1]
    print("Number of Processes: {0}".format(number_of_processes))
    # Get n Lower Bound
    n_lower_bound = estimator_config[2]
    # Get n Upper Bound
    n_upper_bound = estimator_config[3]
    # Get n Range per Output File
    n_range_per_output_file = estimator_config[4]
    # Get max_S Lower Bound
    max_s_lower_bound = estimator_config[5]
    # Set n Range List
    n_range_list = list(range(n_lower_bound, n_upper_bound + 1))
    # print(n_range_list)
    # Set Number of Output Files
    number_of_output_files = ceil(len(n_range_list) / n_range_per_output_file)
    print("Number of Output Files: {0}".format(number_of_output_files))
    # Remove Output CSV Base Directory Path (If Already Exists)
    rmtree(output_directory_path, ignore_errors=True)
    # Create Output CSV Base Directory
    output_directory_path.mkdir()
    # Set Output CSV Files Path List
    output_csv_file_list = []
    for output_file_index in range(1, number_of_output_files + 1):
        # Set Output CSV File Path
        output_csv_file_path = output_directory_path.joinpath("part_" + str(output_file_index) + ".csv")
        # Write Output CSV File Header
        write_csv_file_header(output_csv_file_path)
        # Append Output CSV File to List
        output_csv_file_list.append(output_csv_file_path)
    # Set Pool
    pool = Pool(processes=number_of_processes)
    # Set Partial Function
    partial_function = partial(parallel_task,
                               n_upper_bound,
                               max_s_lower_bound,
                               n_range_per_output_file,
                               output_csv_file_list)
    # Start Map Pool
    pool.map(partial_function,
             n_range_list)


def estimate_parallel(argv_list: list) -> None:
    # Begin
    begin_time = time()
    # Print Application Start Notice
    print("D_a Estimator Application Started!")
    # Check if Has Valid Number of Arguments
    check_if_has_valid_number_of_arguments(argv_list)
    # Read Estimator Config File
    estimator_config_file = Path(argv_list[1])
    # Check If Estimator Config File Exists
    check_if_file_exists(estimator_config_file)
    # Parse Estimator Config
    estimator_config = parse_estimator_config(estimator_config_file)
    # Execute Estimation
    execute_estimation(estimator_config)
    # Print Application End Notice
    print("D_a Estimator Application Finished Successfully!")
    end_time = time()
    print("Duration Time: {0} seconds.".format(end_time - begin_time))
    # End
    exit(0)


if __name__ == "__main__":
    estimate_parallel(argv)
