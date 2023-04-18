# Created by amnon at 14/03/2023
Feature: Spreadsheet from a project with multiple submissions
  # Enter feature description here

  Scenario: upload spreadsheet then add new file in a new submission using api
    Given I create a submission
    And upload spreadsheet submission1.xlsx
    And set submission state to Exported
    And I create a new submission on the same project
    And add a new file called new_file.txt with type sequence_file
    When I export a spreadsheet
    Then spreadsheet contains a file called new_file.txt with type sequence_file

  Scenario: upload a spreadsheet then add and delete a file in first submission
    Given I create a submission
    And upload spreadsheet submission1.xlsx
    And set submission state to Exported
    And I create a new submission on the same project
    And add a new file called new_file.txt with type sequence_file
    And delete a file called SRR3562314_1.fastq.gz in first submission
    When I export a spreadsheet

    Then spreadsheet contains a file called new_file.txt with type sequence_file
    And spreadsheet contains a file called SRR3562314_2.fastq.gz with type sequence_file
    And spreadsheet does not contain a file called SRR3562314_1.fastq.gz with type sequence_file

  Scenario: export a production dataset
    Given submission with uuid 2230dc04-fa0c-4a6b-b043-aa4fae94c0ce
    When I export a spreadsheet
    Then spreadsheet is found in the output directory

  Scenario: export a test dataset
    Given submission with uuid af62a427-c009-4bdf-88be-7902fb58e671
    When I export a spreadsheet
    Then spreadsheet is found in the output directory

  Scenario: export a large test dataset
    Given submission with uuid 1e806c65-d578-40d1-868b-c8965ac29b8b
    When I export a spreadsheet
    Then spreadsheet is found in the output directory
